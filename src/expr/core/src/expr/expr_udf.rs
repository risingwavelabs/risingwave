// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::{Arc, LazyLock, Mutex, Weak};

use arrow_schema::{Field, Fields, Schema};
use await_tree::InstrumentAwait;
use cfg_or_panic::cfg_or_panic;
use risingwave_common::array::{ArrayRef, DataChunk};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum};
use risingwave_pb::expr::ExprNode;
use risingwave_udf::ArrowFlightUdfClient;

use super::{BoxedExpression, Build};
use crate::expr::Expression;
use crate::{bail, Result};

#[derive(Debug)]
pub struct UdfExpression {
    children: Vec<BoxedExpression>,
    arg_types: Vec<DataType>,
    return_type: DataType,
    arg_schema: Arc<Schema>,
    client: Arc<ArrowFlightUdfClient>,
    identifier: String,
    span: await_tree::Span,
}

#[async_trait::async_trait]
impl Expression for UdfExpression {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    #[cfg_or_panic(not(madsim))]
    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let vis = input.visibility();
        let mut columns = Vec::with_capacity(self.children.len());
        for child in &self.children {
            let array = child.eval(input).await?;
            columns.push(array);
        }
        self.eval_inner(columns, vis).await
    }

    #[cfg_or_panic(not(madsim))]
    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        let mut columns = Vec::with_capacity(self.children.len());
        for child in &self.children {
            let datum = child.eval_row(input).await?;
            columns.push(datum);
        }
        let arg_row = OwnedRow::new(columns);
        let chunk = DataChunk::from_rows(std::slice::from_ref(&arg_row), &self.arg_types);
        let arg_columns = chunk.columns().to_vec();
        let output_array = self.eval_inner(arg_columns, chunk.visibility()).await?;
        Ok(output_array.to_datum())
    }
}

impl UdfExpression {
    async fn eval_inner(
        &self,
        columns: Vec<ArrayRef>,
        vis: &risingwave_common::buffer::Bitmap,
    ) -> Result<ArrayRef> {
        let chunk = DataChunk::new(columns, vis.clone());
        let compacted_chunk = chunk.compact_cow();
        let compacted_columns: Vec<arrow_array::ArrayRef> = compacted_chunk
            .columns()
            .iter()
            .map(|c| {
                c.as_ref()
                    .try_into()
                    .expect("failed covert ArrayRef to arrow_array::ArrayRef")
            })
            .collect();
        let opts =
            arrow_array::RecordBatchOptions::default().with_row_count(Some(vis.count_ones()));
        let input = arrow_array::RecordBatch::try_new_with_options(
            self.arg_schema.clone(),
            compacted_columns,
            &opts,
        )
        .expect("failed to build record batch");

        let output = self
            .client
            .call(&self.identifier, input)
            .instrument_await(self.span.clone())
            .await?;
        if output.num_rows() != vis.count_ones() {
            bail!(
                "UDF returned {} rows, but expected {}",
                output.num_rows(),
                vis.len(),
            );
        }

        let data_chunk =
            DataChunk::try_from(&output).expect("failed to convert UDF output to DataChunk");
        let output = data_chunk.uncompact(vis.clone());

        let Some(array) = output.columns().get(0) else {
            bail!("UDF returned no columns");
        };
        if !array.data_type().equals_datatype(&self.return_type) {
            bail!(
                "UDF returned {:?}, but expected {:?}",
                array.data_type(),
                self.return_type,
            );
        }

        Ok(array.clone())
    }
}

#[cfg_or_panic(not(madsim))]
impl Build for UdfExpression {
    fn build(
        prost: &ExprNode,
        build_child: impl Fn(&ExprNode) -> Result<BoxedExpression>,
    ) -> Result<Self> {
        let return_type = DataType::from(prost.get_return_type().unwrap());
        let udf = prost.get_rex_node().unwrap().as_udf().unwrap();

        // connect to UDF service
        let client = get_or_create_client(&udf.link)?;

        let arg_schema = Arc::new(Schema::new(
            udf.arg_types
                .iter()
                .map::<Result<_>, _>(|t| {
                    Ok(Field::new(
                        "",
                        DataType::from(t)
                            .try_into()
                            .map_err(risingwave_udf::Error::unsupported)?,
                        true,
                    ))
                })
                .try_collect::<Fields>()?,
        ));

        Ok(Self {
            children: udf.children.iter().map(build_child).try_collect()?,
            arg_types: udf.arg_types.iter().map(|t| t.into()).collect(),
            return_type,
            arg_schema,
            client,
            identifier: udf.identifier.clone(),
            span: format!("expr_udf_call ({})", udf.identifier).into(),
        })
    }
}

#[cfg(not(madsim))]
/// Get or create a client for the given UDF service.
///
/// There is a global cache for clients, so that we can reuse the same client for the same service.
pub(crate) fn get_or_create_client(link: &str) -> Result<Arc<ArrowFlightUdfClient>> {
    static CLIENTS: LazyLock<Mutex<HashMap<String, Weak<ArrowFlightUdfClient>>>> =
        LazyLock::new(Default::default);
    let mut clients = CLIENTS.lock().unwrap();
    if let Some(client) = clients.get(link).and_then(|c| c.upgrade()) {
        // reuse existing client
        Ok(client)
    } else {
        // create new client
        let client = Arc::new(ArrowFlightUdfClient::connect_lazy(link)?);
        clients.insert(link.into(), Arc::downgrade(&client));
        Ok(client)
    }
}

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

use std::convert::TryFrom;
use std::sync::Arc;

use arrow_schema::{Field, Schema, SchemaRef};
use risingwave_common::array::{ArrayImpl, ArrayRef, DataChunk};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum};
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::ExprNode;
use risingwave_udf::ArrowFlightUdfClient;

use super::{build_from_prost, BoxedExpression};
use crate::expr::Expression;
use crate::{bail, ensure, ExprError, Result};

#[derive(Debug)]
pub struct UdfExpression {
    children: Vec<BoxedExpression>,
    arg_types: Vec<DataType>,
    return_type: DataType,
    arg_schema: SchemaRef,
    client: ArrowFlightUdfClient,
    identifier: String,
}

#[cfg(not(madsim))]
#[async_trait::async_trait]
impl Expression for UdfExpression {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let vis = input.vis().to_bitmap();
        let mut columns = Vec::with_capacity(self.children.len());
        for child in &self.children {
            let array = child.eval_checked(input).await?;
            columns.push(array.as_ref().into());
        }
        self.eval_inner(columns, vis).await
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        let mut columns = Vec::with_capacity(self.children.len());
        for child in &self.children {
            let datum = child.eval_row(input).await?;
            columns.push(datum);
        }
        let arg_row = OwnedRow::new(columns);
        let chunk = DataChunk::from_rows(std::slice::from_ref(&arg_row), &self.arg_types);
        let arg_columns = chunk.columns().iter().map(|c| c.as_ref().into()).collect();
        let output_array = self
            .eval_inner(arg_columns, chunk.vis().to_bitmap())
            .await?;
        Ok(output_array.to_datum())
    }
}

impl UdfExpression {
    async fn eval_inner(
        &self,
        columns: Vec<arrow_array::ArrayRef>,
        vis: risingwave_common::buffer::Bitmap,
    ) -> Result<ArrayRef> {
        let opts = arrow_array::RecordBatchOptions::default().with_row_count(Some(vis.len()));
        let input =
            arrow_array::RecordBatch::try_new_with_options(self.arg_schema.clone(), columns, &opts)
                .expect("failed to build record batch");
        let output = self.client.call(&self.identifier, input).await?;
        if output.num_rows() != vis.len() {
            bail!(
                "UDF returned {} rows, but expected {}",
                output.num_rows(),
                vis.len(),
            );
        }
        let Some(arrow_array) = output.columns().get(0) else {
            bail!("UDF returned no columns");
        };
        let mut array = ArrayImpl::try_from(arrow_array)?;
        array.set_bitmap(array.null_bitmap() & vis);
        Ok(Arc::new(array))
    }
}

#[cfg(not(madsim))]
impl<'a> TryFrom<&'a ExprNode> for UdfExpression {
    type Error = ExprError;

    fn try_from(prost: &'a ExprNode) -> Result<Self> {
        ensure!(prost.get_expr_type().unwrap() == Type::Udf);
        let return_type = DataType::from(prost.get_return_type().unwrap());
        let RexNode::Udf(udf) = prost.get_rex_node().unwrap() else {
            bail!("expect UDF");
        };
        // connect to UDF service
        let arg_schema = Arc::new(Schema::new(
            udf.arg_types
                .iter()
                .map(|t| Field::new("", DataType::from(t).into(), true))
                .collect(),
        ));
        let client = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(ArrowFlightUdfClient::connect(&udf.link))
        })?;
        Ok(Self {
            children: udf.children.iter().map(build_from_prost).try_collect()?,
            arg_types: udf.arg_types.iter().map(|t| t.into()).collect(),
            return_type,
            arg_schema,
            client,
            identifier: udf.identifier.clone(),
        })
    }
}

#[cfg(madsim)]
#[async_trait::async_trait]
impl Expression for UdfExpression {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        panic!("UDF is not supported in simulation yet");
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        panic!("UDF is not supported in simulation yet");
    }
}

#[cfg(madsim)]
impl<'a> TryFrom<&'a ExprNode> for UdfExpression {
    type Error = ExprError;

    fn try_from(prost: &'a ExprNode) -> Result<Self> {
        panic!("UDF is not supported in simulation yet");
    }
}

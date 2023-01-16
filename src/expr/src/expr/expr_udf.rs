// Copyright 2023 Singularity Data
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

use arrow_schema::{Field, Schema};
use risingwave_common::array::{ArrayRef, DataChunk};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum};
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::ExprNode;
use risingwave_udf::{ArrowFlightUdfClient, FunctionId};

use super::{build_from_prost, BoxedExpression};
use crate::expr::Expression;
use crate::{bail, ensure, ExprError, Result};

#[derive(Debug)]
pub struct UdfExpression {
    children: Vec<BoxedExpression>,
    // name: String,
    arg_types: Vec<DataType>,
    return_type: DataType,
    client: ArrowFlightUdfClient,
    function_id: FunctionId,
}

impl Expression for UdfExpression {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let columns: Vec<_> = self
            .children
            .iter()
            .map(|c| c.eval_checked(input).map(|a| ("", a.as_ref().into())))
            .try_collect()?;
        let input =
            arrow_array::RecordBatch::try_from_iter(columns).expect("failed to build record batch");
        let output = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.client.call(&self.function_id, input))
        })?;
        let array = output
            .columns()
            .get(0)
            .ok_or(risingwave_udf::Error::NoColumn)?;
        Ok(Arc::new(array.into()))
    }

    fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        let chunk = DataChunk::from_rows(std::slice::from_ref(input), &self.arg_types);
        let output_array = self.eval(&chunk)?;
        Ok(output_array.to_datum())
    }
}

impl UdfExpression {}

impl<'a> TryFrom<&'a ExprNode> for UdfExpression {
    type Error = ExprError;

    fn try_from(prost: &'a ExprNode) -> Result<Self> {
        ensure!(prost.get_expr_type().unwrap() == Type::Udf);
        let return_type = DataType::from(prost.get_return_type().unwrap());
        let RexNode::Udf(udf) = prost.get_rex_node().unwrap() else {
            bail!("expect UDF");
        };
        // connect to UDF service and check the function
        let (client, function_id) = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let client = ArrowFlightUdfClient::connect(&udf.path).await?;
                let args = Schema::new(
                    udf.arg_types
                        .iter()
                        .map(|t| Field::new("", DataType::from(t).into(), true))
                        .collect(),
                );
                let returns = Schema::new(vec![Field::new("", (&return_type).into(), true)]);
                let id = client.check(&udf.name, &args, &returns).await?;
                Ok((client, id)) as risingwave_udf::Result<_>
            })
        })?;
        Ok(Self {
            children: udf.children.iter().map(build_from_prost).try_collect()?,
            // name: udf.name.clone(),
            arg_types: udf.arg_types.iter().map(|t| t.into()).collect(),
            return_type,
            client,
            function_id,
        })
    }
}

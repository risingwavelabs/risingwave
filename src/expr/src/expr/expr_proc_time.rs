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

use std::sync::Arc;

use risingwave_common::array::{ArrayRef, DataChunk};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum, ScalarImpl};
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::ExprNode;

use super::{Expression, CONTEXT};
use crate::{bail, ensure, ExprError, Result};

#[derive(Debug)]
pub struct ProcTimeExpression;

impl ProcTimeExpression {
    pub fn new() -> Self {
        ProcTimeExpression
    }
}

impl<'a> TryFrom<&'a ExprNode> for ProcTimeExpression {
    type Error = ExprError;

    fn try_from(prost: &'a ExprNode) -> Result<Self> {
        ensure!(prost.get_expr_type().unwrap() == Type::ProcTime);
        ensure!(DataType::from(prost.get_return_type().unwrap()) == DataType::Timestamptz);
        let RexNode::FuncCall(func_call_node) = prost.get_rex_node().unwrap() else {
            bail!("Expected RexNode::FuncCall");
        };
        ensure!(func_call_node.get_children().is_empty());

        Ok(ProcTimeExpression::new())
    }
}

#[async_trait::async_trait]
impl Expression for ProcTimeExpression {
    fn return_type(&self) -> DataType {
        DataType::Timestamptz
    }

    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let capacity = input.capacity();
        let mut array_builder = self.return_type().create_array_builder(capacity);

        let proc_time = CONTEXT
            .try_with(|context| context.get_physical_time())
            .map_err(|_| ExprError::Context)?;
        let datum = Some(ScalarImpl::Int64(proc_time as i64));

        array_builder.append_datum_n(capacity, datum);
        Ok(Arc::new(array_builder.finish()))
    }

    async fn eval_row(&self, _input: &OwnedRow) -> Result<Datum> {
        let proc_time = CONTEXT
            .try_with(|context| context.get_physical_time())
            .map_err(|_| ExprError::Context)?;
        let datum = Some(ScalarImpl::Int64(proc_time as i64));

        Ok(datum)
    }
}

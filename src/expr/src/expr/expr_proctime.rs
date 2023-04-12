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

use risingwave_common::array::DataChunk;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum, ScalarImpl};
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::ExprNode;

use super::{Expression, ValueImpl, CONTEXT};
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
        ensure!(prost.get_expr_type().unwrap() == Type::Proctime);
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

    async fn eval_v2(&self, input: &DataChunk) -> Result<ValueImpl> {
        let proctime = CONTEXT
            .try_with(|context| context.get_unix_millis() * 1000)
            .map_err(|_| ExprError::Context)?;
        let datum = Some(ScalarImpl::Int64(proctime as i64));

        Ok(ValueImpl::Scalar {
            value: datum,
            capacity: input.capacity(),
        })
    }

    async fn eval_row(&self, _input: &OwnedRow) -> Result<Datum> {
        let proctime = CONTEXT
            .try_with(|context| context.get_unix_millis() * 1000)
            .map_err(|_| ExprError::Context)?;
        let datum = Some(ScalarImpl::Int64(proctime as i64));

        Ok(datum)
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::DataChunk;
    use risingwave_common::types::ScalarRefImpl;
    use risingwave_common::util::epoch::Epoch;

    use super::*;
    use crate::expr::{ExprContext, CONTEXT};

    #[tokio::test]
    async fn test_expr_proctime() {
        let proctime_expr = ProcTimeExpression::new();
        let epoch = Epoch::now();
        let time_us = epoch.as_unix_millis() * 1000;
        let time_datum = Some(ScalarRefImpl::Int64(time_us as i64));
        let context = ExprContext::new(epoch);
        let chunk = DataChunk::new_dummy(3);

        let array = CONTEXT
            .scope(context, proctime_expr.eval(&chunk))
            .await
            .unwrap();

        for datum_ref in array.iter() {
            assert_eq!(datum_ref, time_datum)
        }
    }
}

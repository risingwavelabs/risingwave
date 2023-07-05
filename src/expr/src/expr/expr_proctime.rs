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
use risingwave_common::util::epoch;
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::ExprNode;

use super::{Expression, ValueImpl};
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
        ensure!(prost.get_function_type().unwrap() == Type::Proctime);
        ensure!(DataType::from(prost.get_return_type().unwrap()) == DataType::Timestamptz);
        let RexNode::FuncCall(func_call_node) = prost.get_rex_node().unwrap() else {
            bail!("Expected RexNode::FuncCall");
        };
        ensure!(func_call_node.get_children().is_empty());

        Ok(ProcTimeExpression::new())
    }
}

/// Get the processing time in Timestamptz scalar from the task-local epoch.
fn proc_time_from_epoch() -> Result<ScalarImpl> {
    epoch::task_local::curr_epoch()
        .map(|e| e.as_scalar())
        .ok_or(ExprError::Context)
}

#[async_trait::async_trait]
impl Expression for ProcTimeExpression {
    fn return_type(&self) -> DataType {
        DataType::Timestamptz
    }

    async fn eval_v2(&self, input: &DataChunk) -> Result<ValueImpl> {
        proc_time_from_epoch().map(|s| ValueImpl::Scalar {
            value: Some(s),
            capacity: input.capacity(),
        })
    }

    async fn eval_row(&self, _input: &OwnedRow) -> Result<Datum> {
        proc_time_from_epoch().map(Some)
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::DataChunk;
    use risingwave_common::types::ScalarRefImpl;
    use risingwave_common::util::epoch::{Epoch, EpochPair};

    use super::*;

    #[tokio::test]
    async fn test_expr_proctime() {
        let proctime_expr = ProcTimeExpression::new();
        let curr_epoch = Epoch::now();
        let epoch = EpochPair {
            curr: curr_epoch.0,
            prev: 0,
        };
        let chunk = DataChunk::new_dummy(3);

        let array = epoch::task_local::scope(epoch, proctime_expr.eval(&chunk))
            .await
            .unwrap();

        let time_us = curr_epoch.as_unix_millis() * 1000;
        let time_datum = Some(ScalarRefImpl::Int64(time_us as i64));
        for datum_ref in array.iter() {
            assert_eq!(datum_ref, time_datum)
        }
    }
}

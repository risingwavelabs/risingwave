// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::convert::TryFrom;

use risingwave_common::array::{ArrayRef, DataChunk};
use risingwave_common::ensure;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::types::DataType;
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::ExprNode;

use crate::expr::Expression;

/// `InputRefExpression` references to a column in input relation
#[derive(Debug)]
pub struct InputRefExpression {
    return_type: DataType,
    idx: usize,
}

impl Expression for InputRefExpression {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        Ok(input.column_at(self.idx).array())
    }
}

impl InputRefExpression {
    pub fn new(return_type: DataType, idx: usize) -> Self {
        InputRefExpression { return_type, idx }
    }

    pub fn eval_immut(&self, input: &DataChunk) -> Result<ArrayRef> {
        Ok(input.column_at(self.idx).array())
    }
}

impl<'a> TryFrom<&'a ExprNode> for InputRefExpression {
    type Error = RwError;

    fn try_from(prost: &'a ExprNode) -> Result<Self> {
        ensure!(prost.get_expr_type()? == Type::InputRef);

        let ret_type = DataType::from(prost.get_return_type()?);
        if let RexNode::InputRef(input_ref_node) = prost.get_rex_node()? {
            Ok(Self {
                return_type: ret_type,
                idx: input_ref_node.column_idx as usize,
            })
        } else {
            Err(RwError::from(ErrorCode::InternalError(
                "expects a input ref node".to_string(),
            )))
        }
    }
}

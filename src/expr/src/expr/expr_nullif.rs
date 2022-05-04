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
use itertools::Itertools;

use risingwave_common::array::{Array, ArrayImpl, ArrayRef, DataChunk};
use risingwave_common::error::{internal_error, ErrorCode, Result, RwError};
use risingwave_common::types::DataType;
use risingwave_common::{ensure, ensure_eq, try_match_expand};
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::ExprNode;

use crate::expr::{build_from_prost as expr_build_from_prost, BoxedExpression, Expression};

#[derive(Debug)]
pub struct NullifExpression {
    return_type: DataType,
    left: BoxedExpression,
    right: BoxedExpression,
}

impl Expression for NullifExpression {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let array_left = self.left.eval(input)?;
        let array_right = self.right.eval(input)?;

        if let ArrayImpl::Bool(bool_array) = array_right.as_ref() {
            bool_array.iter().map(|c|{

            }).collect_vec()
            Ok(struct_array.get_children_by_index(self.index))
        } else {
            Err(internal_error("expects a struct array ref"))
        }
    }
}

impl NullifExpression {
    pub fn new(return_type: DataType, left: BoxedExpression, right: BoxedExpression) -> Self {
        NullifExpression {
            return_type,
            left,
            right,
        }
    }
}

impl<'a> TryFrom<&'a ExprNode> for NullifExpression {
    type Error = RwError;

    fn try_from(prost: &'a ExprNode) -> Result<Self> {
        ensure!(prost.get_expr_type()? == Type::Nullif);

        let ret_type = DataType::from(prost.get_return_type()?);
        let func_call_node = try_match_expand!(prost.get_rex_node().unwrap(), RexNode::FuncCall)?;

        let children = func_call_node.children.to_vec();
        // Nullif `func_call_node` have 2 child nodes.
        ensure_eq!(children.len(), 2);
        let left = expr_build_from_prost(&children[0])?;
        let right = expr_build_from_prost(&children[1])?;
        Ok(NullifExpression::new(ret_type, left, right))
    }
}
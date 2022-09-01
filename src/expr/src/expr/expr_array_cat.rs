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

use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::array::{ArrayRef, DataChunk, ListValue, Row};
use risingwave_common::types::{DataType, Datum, ScalarImpl};
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::ExprNode;

use crate::expr::{build_from_prost as expr_build_from_prost, BoxedExpression, Expression};
use crate::{bail, ensure, ExprError, Result};

#[derive(Debug)]
pub struct ArrayCatExpression {
    return_type: DataType,
    left: BoxedExpression,
    right: BoxedExpression,
}

impl ArrayCatExpression {
    pub fn new(return_type: DataType, left: BoxedExpression, right: BoxedExpression) -> Self {
        Self {
            return_type,
            left,
            right,
        }
    }

    fn concat(&self, left: Datum, right: Datum) -> Datum {
        match (left, right) {
            (None, right) => right,
            (left, None) => left,
            (Some(ScalarImpl::List(left)), Some(ScalarImpl::List(right))) => {
                let mut values = left.values().to_vec();
                values.extend_from_slice(right.values());
                Some(ScalarImpl::List(ListValue::new(values)))
            }
            (_, _) => {
                // input data types should be checked in frontend
                panic!("the operands must be two arrays with the same data type");
            }
        }
    }
}

impl Expression for ArrayCatExpression {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let left_array = self.left.eval_checked(input)?;
        let right_array = self.right.eval_checked(input)?;
        let mut builder = self
            .return_type
            .create_array_builder(left_array.len() + right_array.len());
        for (left, right) in left_array.iter().zip_eq(right_array.iter()) {
            builder.append_datum(&self.concat(
                left.map(|x| x.into_scalar_impl()),
                right.map(|x| x.into_scalar_impl()),
            ))?;
        }
        Ok(Arc::new(builder.finish()?.into()))
    }

    fn eval_row(&self, input: &Row) -> Result<Datum> {
        let left_data = self.left.eval_row(input)?;
        let right_data = self.right.eval_row(input)?;
        Ok(self.concat(left_data, right_data))
    }
}

impl<'a> TryFrom<&'a ExprNode> for ArrayCatExpression {
    type Error = ExprError;

    fn try_from(prost: &'a ExprNode) -> Result<Self> {
        ensure!(prost.get_expr_type()? == Type::ArrayCat);
        let ret_type = DataType::from(prost.get_return_type()?);
        let RexNode::FuncCall(func_call_node) = prost.get_rex_node()? else {
            bail!("expects a RexNode::FuncCall");
        };
        let children = func_call_node.get_children();
        ensure!(children.len() == 2);
        let left = expr_build_from_prost(&children[0])?;
        let right = expr_build_from_prost(&children[1])?;
        Ok(Self::new(ret_type, left, right))
    }
}

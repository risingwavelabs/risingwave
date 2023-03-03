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
use std::collections::HashSet;

use risingwave_common::array::*;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum, DatumRef, ScalarRefImpl, ToDatumRef};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::ExprNode;

use crate::expr::{build_from_prost, BoxedExpression, Expression};
use crate::{bail, ensure, ExprError, Result};

#[derive(Debug)]
pub struct ArrayDistinctExpression {
    array: BoxedExpression,
    return_type: DataType,
}

impl<'a> TryFrom<&'a ExprNode> for ArrayDistinctExpression {
    type Error = ExprError;

    fn try_from(prost: &'a ExprNode) -> Result<Self> {
        ensure!(prost.get_expr_type().unwrap() == Type::ArrayDistinct);
        let RexNode::FuncCall(func_call_node) = prost.get_rex_node().unwrap() else {
            bail!("Expected RexNode::FuncCall");
        };
        let children = func_call_node.get_children();
        ensure!(children.len() == 1);
        let array = build_from_prost(&children[0])?;
        let return_type = array.return_type();
        Ok(Self {
            array,
            return_type
        })
    }
}

impl Expression for ArrayDistinctExpression {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let array = self.array.eval_checked(input)?;
        let mut builder = self
            .return_type
            .create_array_builder(array.len());
        for (vis,arr) in input
            .vis()
            .iter()
            .zip_eq_fast(array.iter())
        {
            if !vis {
                builder.append_null();
            } else {
                builder.append_datum(&self.evaluate(arr));
            }
        }
        Ok(Arc::new(builder.finish()))
    }

    fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        let array_data = self.array.eval_row(input)?;
        Ok(self.evaluate(array_data.to_datum_ref()))
    }
}

impl ArrayDistinctExpression {
    fn evaluate(&self, array: DatumRef<'_>) -> Datum {
        match array {
           Some(ScalarRefImpl::List(array)) => Some(
            ListValue::new(
                array.values_ref()
                .into_iter()
                .map(|x| x.map(ScalarRefImpl::into_scalar_impl))
                .collect::<HashSet<_>>()
                .into_iter()
                .collect()
            ).into()
           ),
            _ => {
                panic!("the operand must be a list type");
            }
        }
    }
}

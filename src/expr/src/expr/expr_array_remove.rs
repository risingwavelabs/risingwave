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

use risingwave_common::array::{ArrayRef, DataChunk, ListValue};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{
    DataType, Datum, DatumRef, ScalarRefImpl, ToDatumRef, ToOwnedDatum,
};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::expr::expr_node::RexNode;
use risingwave_pb::expr::ExprNode;

use crate::expr::{build_from_prost as expr_build_from_prost, BoxedExpression, Expression};
use crate::{bail, ensure, ExprError, Result};

#[derive(Debug)]
pub struct ArrayRemoveExpression {
    return_type: DataType,
    left: BoxedExpression,
    right: BoxedExpression,
}

impl ArrayRemoveExpression {
    fn new(return_type: DataType, left: BoxedExpression, right: BoxedExpression) -> Self {
        Self {
            return_type,
            left,
            right,
        }
    }

    /// Removes all elements equal to the given value from the array.
    /// Note the behavior is slightly different from PG.
    ///
    /// Examples:
    ///
    /// ```slt
    /// query T
    /// select array_remove(array[array[1],array[2],array[3],array[2],null::int[]], array[1]);
    /// ----
    /// {{2},{3},{2},NULL}
    ///
    /// query T
    /// select array_remove(array[array[1],array[2],array[3],array[2],null::int[]], array[2]);
    /// ----
    /// {{1},{3},NULL}
    ///
    /// query T
    /// select array_remove(array[array[1],array[2],array[3],array[2],null::int[]], null::int[]);
    /// ----
    /// {{1},{2},{3},{2}}
    ///
    /// query T
    /// select array_remove(array[array[1],array[2],array[3],array[2],null::int[]], array[4]);
    /// ----
    /// {{1},{2},{3},{2},NULL}
    ///
    /// query T
    /// select array_remove(null::int[], 1);
    /// ----
    /// NULL
    ///
    /// query T
    /// select array_remove(ARRAY[array[1],array[2],array[3],array[2],null::int[]], array[3.14]);
    /// ----
    /// {{1},{2},{3},{2},NULL}
    ///
    /// query T
    /// select array_remove(array[1,NULL,NULL,3], NULL::int);
    /// ----
    /// {1,3}
    ///
    /// statement error
    /// select array_remove(array[array[1],array[2],array[3],array[2],null::int[]], 1);
    ///
    /// statement error
    /// select array_remove(array[array[1],array[2],array[3],array[2],null::int[]], array[array[3]]);
    ///
    /// statement error
    /// select array_remove(ARRAY[array[1],array[2],array[3],array[2],null::int[]], array[true]);
    /// ```
    fn evaluate(left: DatumRef<'_>, right: DatumRef<'_>) -> Datum {
        match left {
            Some(ScalarRefImpl::List(left)) => Some(
                ListValue::new(
                    left.iter_elems_ref()
                        .filter(|x| x != &right)
                        .map(|x| x.to_owned_datum())
                        .collect(),
                )
                .into(),
            ),
            _ => None,
        }
    }
}

#[async_trait::async_trait]
impl Expression for ArrayRemoveExpression {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let left_array = self.left.eval_checked(input).await?;
        let right_array = self.right.eval_checked(input).await?;
        let mut builder = self.return_type.create_array_builder(input.capacity());
        for (vis, (left, right)) in input
            .vis()
            .iter()
            .zip_eq_fast(left_array.iter().zip_eq_fast(right_array.iter()))
        {
            if !vis {
                builder.append_null();
            } else {
                builder.append_datum(Self::evaluate(left, right));
            }
        }
        Ok(Arc::new(builder.finish()))
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        let left_data = self.left.eval_row(input).await?;
        let right_data = self.right.eval_row(input).await?;
        Ok(Self::evaluate(
            left_data.to_datum_ref(),
            right_data.to_datum_ref(),
        ))
    }
}

impl<'a> TryFrom<&'a ExprNode> for ArrayRemoveExpression {
    type Error = ExprError;

    fn try_from(prost: &'a ExprNode) -> Result<Self> {
        let RexNode::FuncCall(func_call_node) = prost.get_rex_node()? else {
            bail!("expects a RexNode::FuncCall");
        };
        let children = func_call_node.get_children();
        ensure!(children.len() == 2);
        let left = expr_build_from_prost(&children[0])?;
        let right = expr_build_from_prost(&children[1])?;
        let ret_type = DataType::from(prost.get_return_type()?);
        Ok(Self::new(ret_type, left, right))
    }
}

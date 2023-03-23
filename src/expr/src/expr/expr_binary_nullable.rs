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

//! For expression that only accept two nullable arguments as input.

use std::sync::Arc;

use risingwave_common::array::*;
use risingwave_common::buffer::Bitmap;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum, Scalar};
use risingwave_expr_macro::build_function;
use risingwave_pb::expr::expr_node::Type;

use super::{BoxedExpression, Expression};
use crate::vector_op::conjunction::{and, or};
use crate::Result;

pub struct BinaryShortCircuitExpression {
    expr_ia1: BoxedExpression,
    expr_ia2: BoxedExpression,
    expr_type: Type,
}

impl std::fmt::Debug for BinaryShortCircuitExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BinaryShortCircuitExpression")
            .field("expr_ia1", &self.expr_ia1)
            .field("expr_ia2", &self.expr_ia2)
            .field("expr_type", &self.expr_type)
            .finish()
    }
}

#[async_trait::async_trait]
impl Expression for BinaryShortCircuitExpression {
    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let left = self.expr_ia1.eval_checked(input).await?;
        let left = left.as_bool();

        let res_vis: Vis = match self.expr_type {
            // For `Or` operator, if res of left part is not null and is true, we do not want to
            // calculate right part because the result must be true.
            Type::Or => (!left.to_bitmap()).into(),
            // For `And` operator, If res of left part is not null and is false, we do not want
            // to calculate right part because the result must be false.
            Type::And => (left.data() | !left.null_bitmap()).into(),
            _ => unimplemented!(),
        };
        let new_vis = input.vis() & res_vis;
        let mut input1 = input.clone();
        input1.set_vis(new_vis);

        let right = self.expr_ia2.eval_checked(&input1).await?;
        let right = right.as_bool();
        assert_eq!(left.len(), right.len());

        let mut bitmap = match input.visibility() {
            Some(vis) => vis.clone(),
            None => Bitmap::ones(input.capacity()),
        };
        bitmap &= left.null_bitmap();
        bitmap &= right.null_bitmap();

        let c = match self.expr_type {
            Type::Or => {
                let data = left.to_bitmap() | right.to_bitmap();
                bitmap |= &data; // is_true || is_true
                BoolArray::new(data, bitmap)
            }
            Type::And => {
                let data = left.to_bitmap() & right.to_bitmap();
                bitmap |= !left.data() & left.null_bitmap(); // is_false
                bitmap |= !right.data() & right.null_bitmap(); // is_false
                BoolArray::new(data, bitmap)
            }
            _ => unimplemented!(),
        };
        Ok(Arc::new(c.into()))
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        let ret_ia1 = self.expr_ia1.eval_row(input).await?.map(|x| x.into_bool());
        match self.expr_type {
            Type::Or if ret_ia1 == Some(true) => return Ok(Some(true.to_scalar_value())),
            Type::And if ret_ia1 == Some(false) => return Ok(Some(false.to_scalar_value())),
            _ => {}
        }
        let ret_ia2 = self.expr_ia2.eval_row(input).await?.map(|x| x.into_bool());
        match self.expr_type {
            Type::Or => Ok(or(ret_ia1, ret_ia2).map(|x| x.to_scalar_value())),
            Type::And => Ok(and(ret_ia1, ret_ia2).map(|x| x.to_scalar_value())),
            _ => unimplemented!(),
        }
    }
}

#[build_function("and(boolean, boolean) -> boolean")]
fn build_and_expr(_: DataType, children: Vec<BoxedExpression>) -> Result<BoxedExpression> {
    let mut iter = children.into_iter();
    Ok(Box::new(BinaryShortCircuitExpression {
        expr_ia1: iter.next().unwrap(),
        expr_ia2: iter.next().unwrap(),
        expr_type: Type::And,
    }))
}

#[build_function("or(boolean, boolean) -> boolean")]
fn build_or_expr(_: DataType, children: Vec<BoxedExpression>) -> Result<BoxedExpression> {
    let mut iter = children.into_iter();
    Ok(Box::new(BinaryShortCircuitExpression {
        expr_ia1: iter.next().unwrap(),
        expr_ia2: iter.next().unwrap(),
        expr_type: Type::Or,
    }))
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::DataChunk;
    use risingwave_common::test_prelude::DataChunkTestExt;

    use crate::expr::build_from_pretty;

    #[tokio::test]
    async fn test_and() {
        let (input, target) = DataChunk::from_pretty(
            "
            B B B
            t t t
            t f f
            t . .
            f t f
            f f f
            f . f
            . t .
            . f f
            . . .
        ",
        )
        .split_column_at(2);
        let expr = build_from_pretty("(and:boolean #0:boolean #1:boolean)");
        let result = expr.eval(&input).await.unwrap();
        assert_eq!(result, target.column_at(0).array());
    }

    #[tokio::test]
    async fn test_or() {
        let (input, target) = DataChunk::from_pretty(
            "
            B B B
            t t t
            t f t
            t . t
            f t t
            f f f
            f . .
            . t t
            . f .
            . . .
        ",
        )
        .split_column_at(2);
        let expr = build_from_pretty("(or:boolean #0:boolean #1:boolean)");
        let result = expr.eval(&input).await.unwrap();
        assert_eq!(result, target.column_at(0).array());
    }

    #[tokio::test]
    async fn test_is_distinct_from() {
        let (input, target) = DataChunk::from_pretty(
            "
            i i B
            . . f
            . 1 t
            1 . t
            2 2 f
            3 4 t
        ",
        )
        .split_column_at(2);
        let expr = build_from_pretty("(is_distinct_from:boolean #0:int4 #1:int4)");
        let result = expr.eval(&input).await.unwrap();
        assert_eq!(result, target.column_at(0).array());
    }

    #[tokio::test]
    async fn test_is_not_distinct_from() {
        let (input, target) = DataChunk::from_pretty(
            "
            i i B
            . . t
            . 1 f
            1 . f
            2 2 t
            3 4 f
            ",
        )
        .split_column_at(2);
        let expr = build_from_pretty("(is_not_distinct_from:boolean #0:int4 #1:int4)");
        let result = expr.eval(&input).await.unwrap();
        assert_eq!(result, target.column_at(0).array());
    }

    #[tokio::test]
    async fn test_format_type() {
        let (input, target) = DataChunk::from_pretty(
            "
            i       i T
            16      0 boolean
            21      . smallint
            9527    0 ???
            .       0 .
            ",
        )
        .split_column_at(2);
        let expr = build_from_pretty("(format_type:varchar #0:int4 #1:int4)");
        let result = expr.eval(&input).await.unwrap();
        assert_eq!(result, target.column_at(0).array());
    }
}

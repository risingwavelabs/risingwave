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
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum, Scalar};
use risingwave_expr_macro::build_function;
use risingwave_pb::expr::expr_node::Type;

use super::{BoxedExpression, Expression};
use crate::Result;

/// This is just an implementation detail. The semantic is not guaranteed at SQL level because
/// optimizer may have rearranged the boolean expressions. #6202
#[derive(Debug)]
pub struct BinaryShortCircuitExpression {
    expr_ia1: BoxedExpression,
    expr_ia2: BoxedExpression,
    expr_type: Type,
}

#[async_trait::async_trait]
impl Expression for BinaryShortCircuitExpression {
    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let left = self.expr_ia1.eval(input).await?;
        let left = left.as_bool();

        let res_vis = match self.expr_type {
            // For `Or` operator, if res of left part is not null and is true, we do not want to
            // calculate right part because the result must be true.
            Type::Or => !left.to_bitmap(),
            // For `And` operator, If res of left part is not null and is false, we do not want
            // to calculate right part because the result must be false.
            Type::And => left.data() | !left.null_bitmap(),
            _ => unimplemented!(),
        };
        let new_vis = input.visibility() & res_vis;
        let mut input1 = input.clone();
        input1.set_visibility(new_vis);

        let right = self.expr_ia2.eval(&input1).await?;
        let right = right.as_bool();
        assert_eq!(left.len(), right.len());

        let mut bitmap = input.visibility() & left.null_bitmap() & right.null_bitmap();

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

// #[function("and(boolean, boolean) -> boolean")]
fn and(l: Option<bool>, r: Option<bool>) -> Option<bool> {
    match (l, r) {
        (Some(lb), Some(lr)) => Some(lb & lr),
        (Some(true), None) => None,
        (None, Some(true)) => None,
        (Some(false), None) => Some(false),
        (None, Some(false)) => Some(false),
        (None, None) => None,
    }
}

// #[function("or(boolean, boolean) -> boolean")]
fn or(l: Option<bool>, r: Option<bool>) -> Option<bool> {
    match (l, r) {
        (Some(lb), Some(lr)) => Some(lb | lr),
        (Some(true), None) => Some(true),
        (None, Some(true)) => Some(true),
        (Some(false), None) => None,
        (None, Some(false)) => None,
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::DataChunk;
    use risingwave_common::test_prelude::DataChunkTestExt;

    use super::*;
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
        let expr = build_from_pretty("(and:boolean $0:boolean $1:boolean)");
        let result = expr.eval(&input).await.unwrap();
        assert_eq!(&result, target.column_at(0));
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
        let expr = build_from_pretty("(or:boolean $0:boolean $1:boolean)");
        let result = expr.eval(&input).await.unwrap();
        assert_eq!(&result, target.column_at(0));
    }

    #[test]
    fn test_and_() {
        assert_eq!(Some(true), and(Some(true), Some(true)));
        assert_eq!(Some(false), and(Some(true), Some(false)));
        assert_eq!(Some(false), and(Some(false), Some(false)));
        assert_eq!(None, and(Some(true), None));
        assert_eq!(Some(false), and(Some(false), None));
        assert_eq!(None, and(None, None));
    }

    #[test]
    fn test_or_() {
        assert_eq!(Some(true), or(Some(true), Some(true)));
        assert_eq!(Some(true), or(Some(true), Some(false)));
        assert_eq!(Some(false), or(Some(false), Some(false)));
        assert_eq!(Some(true), or(Some(true), None));
        assert_eq!(None, or(Some(false), None));
        assert_eq!(None, or(None, None));
    }
}

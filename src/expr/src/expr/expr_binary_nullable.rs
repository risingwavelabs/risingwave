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
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{DataType, Scalar};
    use risingwave_pb::expr::expr_node::Type;

    use crate::expr::{build, Expression, InputRefExpression};

    #[tokio::test]
    async fn test_and() {
        let lhs = vec![
            Some(true),
            Some(true),
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            None,
            None,
            None,
        ];
        let rhs = vec![
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
        ];
        let target = vec![
            Some(true),
            Some(false),
            None,
            Some(false),
            Some(false),
            Some(false),
            None,
            Some(false),
            None,
        ];

        let expr = build(
            Type::And,
            DataType::Boolean,
            vec![
                InputRefExpression::new(DataType::Boolean, 0).boxed(),
                InputRefExpression::new(DataType::Boolean, 1).boxed(),
            ],
        )
        .unwrap();

        for i in 0..lhs.len() {
            let row = OwnedRow::new(vec![
                lhs[i].map(|x| x.to_scalar_value()),
                rhs[i].map(|x| x.to_scalar_value()),
            ]);
            let res = expr.eval_row(&row).await.unwrap();
            let expected = target[i].map(|x| x.to_scalar_value());
            assert_eq!(res, expected);
        }
    }

    #[tokio::test]
    async fn test_or() {
        let lhs = vec![
            Some(true),
            Some(true),
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            None,
            None,
            None,
        ];
        let rhs = vec![
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
        ];
        let target = vec![
            Some(true),
            Some(true),
            Some(true),
            Some(true),
            Some(false),
            None,
            Some(true),
            None,
            None,
        ];

        let expr = build(
            Type::Or,
            DataType::Boolean,
            vec![
                InputRefExpression::new(DataType::Boolean, 0).boxed(),
                InputRefExpression::new(DataType::Boolean, 1).boxed(),
            ],
        )
        .unwrap();

        for i in 0..lhs.len() {
            let row = OwnedRow::new(vec![
                lhs[i].map(|x| x.to_scalar_value()),
                rhs[i].map(|x| x.to_scalar_value()),
            ]);
            let res = expr.eval_row(&row).await.unwrap();
            let expected = target[i].map(|x| x.to_scalar_value());
            assert_eq!(res, expected);
        }
    }

    #[tokio::test]
    async fn test_is_distinct_from() {
        let lhs = vec![None, None, Some(1), Some(2), Some(3)];
        let rhs = vec![None, Some(1), None, Some(2), Some(4)];
        let target = vec![Some(false), Some(true), Some(true), Some(false), Some(true)];

        let expr = build(
            Type::IsDistinctFrom,
            DataType::Boolean,
            vec![
                InputRefExpression::new(DataType::Int32, 0).boxed(),
                InputRefExpression::new(DataType::Int32, 1).boxed(),
            ],
        )
        .unwrap();

        for i in 0..lhs.len() {
            let row = OwnedRow::new(vec![
                lhs[i].map(|x| x.to_scalar_value()),
                rhs[i].map(|x| x.to_scalar_value()),
            ]);
            let res = expr.eval_row(&row).await.unwrap();
            let expected = target[i].map(|x| x.to_scalar_value());
            assert_eq!(res, expected);
        }
    }

    #[tokio::test]
    async fn test_is_not_distinct_from() {
        let lhs = vec![None, None, Some(1), Some(2), Some(3)];
        let rhs = vec![None, Some(1), None, Some(2), Some(4)];
        let target = vec![
            Some(true),
            Some(false),
            Some(false),
            Some(true),
            Some(false),
        ];

        let expr = build(
            Type::IsNotDistinctFrom,
            DataType::Boolean,
            vec![
                InputRefExpression::new(DataType::Int32, 0).boxed(),
                InputRefExpression::new(DataType::Int32, 1).boxed(),
            ],
        )
        .unwrap();

        for i in 0..lhs.len() {
            let row = OwnedRow::new(vec![
                lhs[i].map(|x| x.to_scalar_value()),
                rhs[i].map(|x| x.to_scalar_value()),
            ]);
            let res = expr.eval_row(&row).await.unwrap();
            let expected = target[i].map(|x| x.to_scalar_value());
            assert_eq!(res, expected);
        }
    }

    #[tokio::test]
    async fn test_format_type() {
        let l = vec![Some(16), Some(21), Some(9527), None];
        let r = vec![Some(0), None, Some(0), Some(0)];
        let target: Vec<Option<String>> = vec![
            Some("boolean".into()),
            Some("smallint".into()),
            Some("???".into()),
            None,
        ];
        let expr = build(
            Type::FormatType,
            DataType::Varchar,
            vec![
                InputRefExpression::new(DataType::Int32, 0).boxed(),
                InputRefExpression::new(DataType::Int32, 1).boxed(),
            ],
        )
        .unwrap();

        for i in 0..l.len() {
            let row = OwnedRow::new(vec![
                l[i].map(|x| x.to_scalar_value()),
                r[i].map(|x| x.to_scalar_value()),
            ]);
            let res = expr.eval_row(&row).await.unwrap();
            let expected = target[i].as_ref().map(|x| x.into());
            assert_eq!(res, expected);
        }
    }
}

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

    fn concat(left: Datum, right: Datum) -> Datum {
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
        for (vis, (left, right)) in input
            .vis()
            .iter()
            .zip_eq(left_array.iter().zip_eq(right_array.iter()))
        {
            if !vis {
                builder.append_null()?;
            } else {
                builder.append_datum(&Self::concat(
                    left.map(|x| x.into_scalar_impl()),
                    right.map(|x| x.into_scalar_impl()),
                ))?;
            }
        }
        Ok(Arc::new(builder.finish()?.into()))
    }

    fn eval_row(&self, input: &Row) -> Result<Datum> {
        let left_data = self.left.eval_row(input)?;
        let right_data = self.right.eval_row(input)?;
        Ok(Self::concat(left_data, right_data))
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

#[cfg(test)]
mod tests {
    use risingwave_common::array::DataChunk;
    use risingwave_pb::expr::expr_node::{RexNode, Type as ProstType};
    use risingwave_pb::expr::{ConstantValue, ExprNode, FunctionCall};

    use super::*;
    use crate::expr::{Expression, LiteralExpression};

    fn make_i64_expr_node(value: i64) -> ExprNode {
        ExprNode {
            expr_type: ProstType::ConstantValue as i32,
            return_type: Some(DataType::Int64.to_protobuf()),
            rex_node: Some(RexNode::Constant(ConstantValue {
                body: value.to_be_bytes().to_vec(),
            })),
        }
    }

    #[test]
    fn test_array_cat_try_from() {
        let left_array = ExprNode {
            expr_type: ProstType::Array as i32,
            return_type: Some(
                DataType::List {
                    datatype: Box::new(DataType::Int64),
                }
                .to_protobuf(),
            ),
            rex_node: Some(RexNode::FuncCall(FunctionCall {
                children: vec![make_i64_expr_node(42)],
            })),
        };
        let right_array = ExprNode {
            expr_type: ProstType::Array as i32,
            return_type: Some(
                DataType::List {
                    datatype: Box::new(DataType::Int64),
                }
                .to_protobuf(),
            ),
            rex_node: Some(RexNode::FuncCall(FunctionCall {
                children: vec![make_i64_expr_node(43), make_i64_expr_node(44)],
            })),
        };
        let expr = ExprNode {
            expr_type: ProstType::ArrayCat as i32,
            return_type: Some(
                DataType::List {
                    datatype: Box::new(DataType::Int64),
                }
                .to_protobuf(),
            ),
            rex_node: Some(RexNode::FuncCall(FunctionCall {
                children: vec![left_array, right_array],
            })),
        };
        assert!(ArrayCatExpression::try_from(&expr).is_ok());
    }

    fn make_i64_array_expr(values: Vec<i64>) -> BoxedExpression {
        LiteralExpression::new(
            DataType::List {
                datatype: Box::new(DataType::Int64),
            },
            Some(ListValue::new(values.into_iter().map(|x| Some(x.into())).collect()).into()),
        )
        .boxed()
    }

    #[test]
    fn test_array_cat_array_of_primitives() {
        let left_array = make_i64_array_expr(vec![42]);
        let right_array = make_i64_array_expr(vec![43, 44]);
        let expr = ArrayCatExpression::new(
            DataType::List {
                datatype: Box::new(DataType::Int64),
            },
            left_array,
            right_array,
        );

        let chunk = DataChunk::new_dummy(4)
            .with_visibility([true, false, true, true].into_iter().collect());
        let expected_array = Some(ScalarImpl::List(ListValue::new(vec![
            Some(42i64.into()),
            Some(43i64.into()),
            Some(44i64.into()),
        ])));
        let expected = vec![
            expected_array.clone(),
            None,
            expected_array.clone(),
            expected_array.clone(),
        ];
        let actual = expr
            .eval(&chunk)
            .unwrap()
            .iter()
            .map(|v| v.map(|s| s.into_scalar_impl()))
            .collect_vec();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_array_cat_null_arg() {
        let test_the_expr = |expr: ArrayCatExpression| {
            let chunk = DataChunk::new_dummy(4)
                .with_visibility([true, false, true, true].into_iter().collect());
            let expected_array = Some(ScalarImpl::List(ListValue::new(vec![Some(42i64.into())])));
            let expected = vec![
                expected_array.clone(),
                None,
                expected_array.clone(),
                expected_array.clone(),
            ];
            let actual = expr
                .eval(&chunk)
                .unwrap()
                .iter()
                .map(|v| v.map(|s| s.into_scalar_impl()))
                .collect_vec();
            assert_eq!(actual, expected);
        };

        let expr = ArrayCatExpression::new(
            DataType::List {
                datatype: Box::new(DataType::Int64),
            },
            make_i64_array_expr(vec![42]),
            LiteralExpression::new(
                DataType::List {
                    datatype: Box::new(DataType::Int64),
                },
                None,
            )
            .boxed(),
        );
        test_the_expr(expr);

        let expr = ArrayCatExpression::new(
            DataType::List {
                datatype: Box::new(DataType::Int64),
            },
            LiteralExpression::new(
                DataType::List {
                    datatype: Box::new(DataType::Int64),
                },
                None,
            )
            .boxed(),
            make_i64_array_expr(vec![42]),
        );
        test_the_expr(expr);
    }

    #[test]
    fn test_array_cat_array_of_arrays() {
        let ret_type = DataType::List {
            datatype: Box::new(DataType::List {
                datatype: Box::new(DataType::Int64),
            }),
        };
        let left_array = LiteralExpression::new(
            ret_type.clone(),
            Some(
                ListValue::new(vec![Some(ListValue::new(vec![Some(42i64.into())]).into())]).into(),
            ),
        )
        .boxed();
        let right_array = LiteralExpression::new(
            ret_type.clone(),
            Some(
                ListValue::new(vec![Some(ListValue::new(vec![Some(43i64.into())]).into())]).into(),
            ),
        )
        .boxed();
        let expr = ArrayCatExpression::new(ret_type.clone(), left_array, right_array);

        let chunk = DataChunk::new_dummy(4)
            .with_visibility([true, false, true, true].into_iter().collect());
        let expected_array = Some(ScalarImpl::List(ListValue::new(vec![
            Some(ListValue::new(vec![Some(42i64.into())]).into()),
            Some(ListValue::new(vec![Some(43i64.into())]).into()),
        ])));
        let expected = vec![
            expected_array.clone(),
            None,
            expected_array.clone(),
            expected_array.clone(),
        ];
        let actual = expr
            .eval(&chunk)
            .unwrap()
            .iter()
            .map(|v| v.map(|s| s.into_scalar_impl()))
            .collect_vec();
        assert_eq!(actual, expected);
    }
}

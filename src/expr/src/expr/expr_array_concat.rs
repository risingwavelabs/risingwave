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
use risingwave_common::types::{to_datum_ref, DataType, Datum, DatumRef, ScalarRefImpl};
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::ExprNode;

use crate::expr::{build_from_prost as expr_build_from_prost, BoxedExpression, Expression};
use crate::{bail, ensure, ExprError, Result};

#[derive(Debug, Copy, Clone)]
enum Operation { // 5 combinations
    ConcatArray,
    AppendArray,
    PrependArray,
    AppendValue,
    PrependValue,
}

pub struct ArrayConcatExpression {
    return_type: DataType,
    left: BoxedExpression,
    right: BoxedExpression,
    op: Operation,
    op_func: fn(DatumRef, DatumRef) -> Datum,
}

impl std::fmt::Debug for ArrayConcatExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrayConcatExpression")
            .field("return_type", &self.return_type)
            .field("left", &self.left)
            .field("right", &self.right)
            .field("op", &self.op)
            .finish()
    }
}

impl ArrayConcatExpression {
    fn new(
        return_type: DataType,
        left: BoxedExpression,
        right: BoxedExpression,
        op: Operation,
    ) -> Self {
        Self {
            return_type,
            left,
            right,
            op,
            op_func: match op {
                Operation::ConcatArray => Self::concat_array,
                Operation::AppendArray => Self::append_array,
                Operation::PrependArray => Self::prepend_array,
                Operation::AppendValue => Self::append_value,
                Operation::PrependValue => Self::prepend_value,
            },
        }
    }

    /// Concatenates two arrays with same data type.
    /// The behavior is the same as PG.
    ///
    /// Examples:
    /// - `select array_cat(array[66], array[123]);` => `[66,123]`
    /// - `select array_cat(array[66], null::int[]);` => `[66]`
    /// - `select array_cat(null::int[], array[123]);` => `[123]`
    /// TODO: Concat arrays with similar types: https://github.com/risingwavelabs/risingwave/issues/5100
    fn concat_array(left: DatumRef, right: DatumRef) -> Datum {
        match (left, right) {
            (None, right) => right.map(ScalarRefImpl::into_scalar_impl),
            (left, None) => left.map(ScalarRefImpl::into_scalar_impl),
            (Some(ScalarRefImpl::List(left)), Some(ScalarRefImpl::List(right))) => Some(
                ListValue::new(
                    left.values_ref()
                        .into_iter()
                        .chain(right.values_ref().into_iter())
                        .map(|x| x.map(ScalarRefImpl::into_scalar_impl))
                        .collect(),
                )
                .into(),
            ),
            _ => {
                panic!("the operands must be two arrays with the same data type.");
            }
        }
    }

    /// Appends an array as the back element of an array of array.
    /// Note the behavior is slightly different from PG.
    ///
    /// Examples:
    /// - `select array_cat(array[array[66]], array[233]);` => `[[66], [233]]`
    /// - `select array_cat(array[array[66]], null::int[]);` => `[[66]]` ignore NULL, same as PG
    /// - `select array_cat(null::int[][], array[233]);` => `[[233]]` different from PG
    /// - `select array_cat(null::int[][], null::int[]);` => `null` same as PG
    fn append_array(left: DatumRef, right: DatumRef) -> Datum {
        match (left, right) {
            (None, None) => None,
            (None, right) => {
                Some(ListValue::new(vec![right.map(ScalarRefImpl::into_scalar_impl)]).into())
            }
            (left, None) => left.map(ScalarRefImpl::into_scalar_impl),
            (Some(ScalarRefImpl::List(left)), right) => Some(
                ListValue::new(
                    left.values_ref()
                        .into_iter()
                        .chain(std::iter::once(right))
                        .map(|x| x.map(ScalarRefImpl::into_scalar_impl))
                        .collect(),
                )
                .into(),
            ),
            _ => {
                panic!("the rhs must be compatible to append to lhs");
            }
        }
    }

    /// Appends a value as the back element of an array.
    /// The behavior is the same as PG.
    ///
    /// Examples:
    /// - `select array_append(array[66], 123);` => `[66, 123]`
    /// - `select array_append(array[66], null::int);` => `[66, null]`
    /// - `select array_append(null::int[], 233);` => `[233]`
    /// - `select array_append(null::int[], null::int);` => `[null]`
    fn append_value(left: DatumRef, right: DatumRef) -> Datum {
        match (left, right) {
            (None, right) => {
                Some(ListValue::new(vec![right.map(ScalarRefImpl::into_scalar_impl)]).into())
            }
            (Some(ScalarRefImpl::List(left)), right) => Some(
                ListValue::new(
                    left.values_ref()
                        .into_iter()
                        .chain(std::iter::once(right))
                        .map(|x| x.map(ScalarRefImpl::into_scalar_impl))
                        .collect(),
                )
                .into(),
            ),
            _ => {
                panic!("the rhs must be compatible to append to lhs");
            }
        }
    }

    /// Prepends an array as the front element of an array of array.
    /// Note the behavior is slightly different from PG.
    ///
    /// Examples:
    /// - `select array_cat(array[233], array[array[66]]);` => `[[233], [66]]`
    /// - `select array_cat(null::int[], array[array[66]]);` => `[[66]]` ignore NULL, same as PG
    /// - `select array_cat(array[233], null::int[][]);` => `[[233]]` different from PG
    /// - `select array_cat(null::int[], null::int[][]);` => `null` same as PG
    fn prepend_array(left: DatumRef, right: DatumRef) -> Datum {
        match (left, right) {
            (None, None) => None,
            (left, None) => {
                Some(ListValue::new(vec![left.map(ScalarRefImpl::into_scalar_impl)]).into())
            }
            (None, right) => right.map(ScalarRefImpl::into_scalar_impl),
            (left, Some(ScalarRefImpl::List(right))) => Some(
                ListValue::new(
                    std::iter::once(left)
                        .chain(right.values_ref().into_iter())
                        .map(|x| x.map(ScalarRefImpl::into_scalar_impl))
                        .collect(),
                )
                .into(),
            ),
            _ => {
                panic!("the lhs must be compatible to prepend to rhs");
            }
        }
    }

    /// Prepends a value as the front element of an array.
    /// The behavior is the same as PG.
    ///
    /// Examples:
    /// - `select array_prepend(123, array[66]);` => `[123, 66]`
    /// - `select array_prepend(null::int, array[66]);` => `[null, 66]`
    /// - `select array_prepend(233, null::int[]);` => `[233]`
    /// - `select array_prepend(null::int, null::int[]);` => `[null]`
    fn prepend_value(left: DatumRef, right: DatumRef) -> Datum {
        match (left, right) {
            (left, None) => {
                Some(ListValue::new(vec![left.map(ScalarRefImpl::into_scalar_impl)]).into())
            }
            (left, Some(ScalarRefImpl::List(right))) => Some(
                ListValue::new(
                    std::iter::once(left)
                        .chain(right.values_ref().into_iter())
                        .map(|x| x.map(ScalarRefImpl::into_scalar_impl))
                        .collect(),
                )
                .into(),
            ),
            _ => {
                panic!("the lhs must be compatible to prepend to rhs");
            }
        }
    }

    fn evaluate(&self, left: DatumRef, right: DatumRef) -> Datum {
        (self.op_func)(left, right)
    }
}

impl Expression for ArrayConcatExpression {
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
                builder.append_datum(&self.evaluate(left, right))?;
            }
        }
        Ok(Arc::new(builder.finish()?))
    }

    fn eval_row(&self, input: &Row) -> Result<Datum> {
        let left_data = self.left.eval_row(input)?;
        let right_data = self.right.eval_row(input)?;
        Ok(self.evaluate(to_datum_ref(&left_data), to_datum_ref(&right_data)))
    }
}

impl<'a> TryFrom<&'a ExprNode> for ArrayConcatExpression {
    type Error = ExprError;

    fn try_from(prost: &'a ExprNode) -> Result<Self> {
        let RexNode::FuncCall(func_call_node) = prost.get_rex_node()? else {
            bail!("expects a RexNode::FuncCall");
        };
        let children = func_call_node.get_children();
        ensure!(children.len() == 2);
        let left = expr_build_from_prost(&children[0])?;
        let right = expr_build_from_prost(&children[1])?;
        let left_type = left.return_type();
        let right_type = right.return_type();
        let ret_type = DataType::from(prost.get_return_type()?);
        let op = match prost.get_expr_type()? {
            // the types are checked in frontend, so no need for type checking here
            Type::ArrayCat => {
                if left_type == right_type {
                    Operation::ConcatArray
                } else if left_type == ret_type {
                    Operation::AppendArray
                } else if right_type == ret_type {
                    Operation::PrependArray
                } else {
                    bail!("function call node invalid");
                }
            }
            Type::ArrayAppend => Operation::AppendValue,
            Type::ArrayPrepend => Operation::PrependValue,
            _ => bail!("expects `ArrayCat`|`ArrayAppend`|`ArrayPrepend`"),
        };
        Ok(Self::new(ret_type, left, right, op))
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::DataChunk;
    use risingwave_common::types::ScalarImpl;
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

    fn make_i64_array_expr_node(values: Vec<i64>) -> ExprNode {
        ExprNode {
            expr_type: ProstType::Array as i32,
            return_type: Some(
                DataType::List {
                    datatype: Box::new(DataType::Int64),
                }
                .to_protobuf(),
            ),
            rex_node: Some(RexNode::FuncCall(FunctionCall {
                children: values.into_iter().map(make_i64_expr_node).collect(),
            })),
        }
    }

    fn make_i64_array_array_expr_node(values: Vec<Vec<i64>>) -> ExprNode {
        ExprNode {
            expr_type: ProstType::Array as i32,
            return_type: Some(
                DataType::List {
                    datatype: Box::new(DataType::List {
                        datatype: Box::new(DataType::Int64),
                    }),
                }
                .to_protobuf(),
            ),
            rex_node: Some(RexNode::FuncCall(FunctionCall {
                children: values.into_iter().map(make_i64_array_expr_node).collect(),
            })),
        }
    }

    #[test]
    fn test_array_concat_try_from() {
        {
            let left = make_i64_array_expr_node(vec![42]);
            let right = make_i64_array_expr_node(vec![43]);
            let expr = ExprNode {
                expr_type: ProstType::ArrayCat as i32,
                return_type: Some(
                    DataType::List {
                        datatype: Box::new(DataType::Int64),
                    }
                    .to_protobuf(),
                ),
                rex_node: Some(RexNode::FuncCall(FunctionCall {
                    children: vec![left, right],
                })),
            };
            assert!(ArrayConcatExpression::try_from(&expr).is_ok());
        }

        {
            let left = make_i64_array_array_expr_node(vec![vec![42]]);
            let right = make_i64_array_array_expr_node(vec![vec![43]]);
            let expr = ExprNode {
                expr_type: ProstType::ArrayCat as i32,
                return_type: Some(
                    DataType::List {
                        datatype: Box::new(DataType::Int64),
                    }
                    .to_protobuf(),
                ),
                rex_node: Some(RexNode::FuncCall(FunctionCall {
                    children: vec![left, right],
                })),
            };
            assert!(ArrayConcatExpression::try_from(&expr).is_ok());
        }

        {
            let left = make_i64_array_expr_node(vec![42]);
            let right = make_i64_expr_node(43);
            let expr = ExprNode {
                expr_type: ProstType::ArrayAppend as i32,
                return_type: Some(
                    DataType::List {
                        datatype: Box::new(DataType::Int64),
                    }
                    .to_protobuf(),
                ),
                rex_node: Some(RexNode::FuncCall(FunctionCall {
                    children: vec![left, right],
                })),
            };
            assert!(ArrayConcatExpression::try_from(&expr).is_ok());
        }

        {
            let left = make_i64_array_array_expr_node(vec![vec![42]]);
            let right = make_i64_array_expr_node(vec![43]);
            let expr = ExprNode {
                expr_type: ProstType::ArrayAppend as i32,
                return_type: Some(
                    DataType::List {
                        datatype: Box::new(DataType::Int64),
                    }
                    .to_protobuf(),
                ),
                rex_node: Some(RexNode::FuncCall(FunctionCall {
                    children: vec![left, right],
                })),
            };
            assert!(ArrayConcatExpression::try_from(&expr).is_ok());
        }

        {
            let left = make_i64_expr_node(43);
            let right = make_i64_array_expr_node(vec![42]);
            let expr = ExprNode {
                expr_type: ProstType::ArrayPrepend as i32,
                return_type: Some(
                    DataType::List {
                        datatype: Box::new(DataType::Int64),
                    }
                    .to_protobuf(),
                ),
                rex_node: Some(RexNode::FuncCall(FunctionCall {
                    children: vec![left, right],
                })),
            };
            assert!(ArrayConcatExpression::try_from(&expr).is_ok());
        }

        {
            let left = make_i64_array_expr_node(vec![43]);
            let right = make_i64_array_array_expr_node(vec![vec![42]]);
            let expr = ExprNode {
                expr_type: ProstType::ArrayPrepend as i32,
                return_type: Some(
                    DataType::List {
                        datatype: Box::new(DataType::Int64),
                    }
                    .to_protobuf(),
                ),
                rex_node: Some(RexNode::FuncCall(FunctionCall {
                    children: vec![left, right],
                })),
            };
            assert!(ArrayConcatExpression::try_from(&expr).is_ok());
        }
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
    fn test_array_concat_array_of_primitives() {
        let left = make_i64_array_expr(vec![42]);
        let right = make_i64_array_expr(vec![43, 44]);
        let expr = ArrayConcatExpression::new(
            DataType::List {
                datatype: Box::new(DataType::Int64),
            },
            left,
            right,
            Operation::ConcatArray,
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
            expected_array,
        ];
        let actual = expr
            .eval(&chunk)
            .unwrap()
            .iter()
            .map(|v| v.map(|s| s.into_scalar_impl()))
            .collect_vec();
        assert_eq!(actual, expected);
    }

    // More test cases are in e2e tests.
}

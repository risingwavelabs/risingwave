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

use itertools::Itertools;
use risingwave_common::array::*;
use risingwave_common::types::ScalarRefImpl;
use risingwave_expr_macro::function;

/// Returns a new array removing all the duplicates from the input array
///
/// ```sql
/// array_distinct ( array anyarray) → array
/// ```
///
/// Examples:
///
/// ```slt
/// query T
/// select array_distinct(array[NULL]);
/// ----
/// {NULL}
///
/// query T
/// select array_distinct(array[1,2,1,1]);
/// ----
/// {1,2}
///
/// query T
/// select array_distinct(array[1,2,1,NULL]);
/// ----
/// {1,2,NULL}
///
/// query T
/// select array_distinct(null::int[]);
/// ----
/// NULL
///
/// query error polymorphic type
/// select array_distinct(null);
/// ```

#[function("array_distinct(list) -> list")]
pub fn array_distinct(list: ListRef<'_>) -> ListValue {
    ListValue::new(
        list.values_ref()
            .into_iter()
            .map(|x| x.map(ScalarRefImpl::into_scalar_impl))
            .unique()
            .collect(),
    )
}

#[cfg(test)]
mod tests {

    use itertools::Itertools;
    use risingwave_common::array::DataChunk;
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_pb::data::Datum as ProstDatum;
    use risingwave_pb::expr::expr_node::{RexNode, Type as ProstType};
    use risingwave_pb::expr::{ExprNode, FunctionCall};

    use super::*;
    use crate::expr::{Expression, LiteralExpression};

    fn make_i64_expr_node(value: i64) -> ExprNode {
        ExprNode {
            expr_type: ProstType::ConstantValue as i32,
            return_type: Some(DataType::Int64.to_protobuf()),
            rex_node: Some(RexNode::Constant(ProstDatum {
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
    fn test_array_distinct_try_from() {
        {
            let array = make_i64_array_expr_node(vec![12]);
            let expr = ExprNode {
                expr_type: ProstType::ArrayDistinct as i32,
                return_type: Some(
                    DataType::List {
                        datatype: Box::new(DataType::Int64),
                    }
                    .to_protobuf(),
                ),
                rex_node: Some(RexNode::FuncCall(FunctionCall {
                    children: vec![array],
                })),
            };
            assert!(ArrayDistinctExpression::try_from(&expr).is_ok());
        }

        {
            let array = make_i64_array_array_expr_node(vec![vec![42], vec![42]]);
            let expr = ExprNode {
                expr_type: ProstType::ArrayDistinct as i32,
                return_type: Some(
                    DataType::List {
                        datatype: Box::new(DataType::Int64),
                    }
                    .to_protobuf(),
                ),
                rex_node: Some(RexNode::FuncCall(FunctionCall {
                    children: vec![array],
                })),
            };
            assert!(ArrayDistinctExpression::try_from(&expr).is_ok());
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
    fn test_array_distinct_array_of_primitives() {
        let array = make_i64_array_expr(vec![42, 43, 42]);
        let expr = ArrayDistinctExpression {
            return_type: DataType::List {
                datatype: Box::new(DataType::Int64),
            },
            array,
        };

        let chunk = DataChunk::new_dummy(4)
            .with_visibility([true, false, true, true].into_iter().collect());
        let expected_array = Some(ScalarImpl::List(ListValue::new(vec![
            Some(42i64.into()),
            Some(43i64.into()),
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

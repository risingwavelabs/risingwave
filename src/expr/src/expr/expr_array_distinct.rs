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

use itertools::Itertools;
use risingwave_common::array::*;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum, DatumRef, ScalarRefImpl, ToDatumRef};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::ExprNode;

use crate::expr::{build_from_prost, BoxedExpression, Expression};
use crate::{bail, ensure, ExprError, Result};

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
        Ok(Self { array, return_type })
    }
}

#[async_trait::async_trait]
impl Expression for ArrayDistinctExpression {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let array = self.array.eval_checked(input).await?;
        let mut builder = self.return_type.create_array_builder(array.len());
        for (vis, arr) in input.vis().iter().zip_eq_fast(array.iter()) {
            if !vis {
                builder.append_null();
            } else {
                builder.append_datum(&self.evaluate(arr));
            }
        }
        Ok(Arc::new(builder.finish()))
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        let array_data = self.array.eval_row(input).await?;
        Ok(self.evaluate(array_data.to_datum_ref()))
    }
}

impl ArrayDistinctExpression {
    fn evaluate(&self, array: DatumRef<'_>) -> Datum {
        match array {
            Some(ScalarRefImpl::List(array)) => Some(
                ListValue::new(
                    array
                        .values_ref()
                        .into_iter()
                        .map(|x| x.map(ScalarRefImpl::into_scalar_impl))
                        .unique()
                        .collect(),
                )
                .into(),
            ),
            None => None,
            Some(_) => unreachable!("the operand must be a list type"),
        }
    }
}

#[cfg(test)]
mod tests {

    use itertools::Itertools;
    use risingwave_common::array::DataChunk;
    use risingwave_common::types::ScalarImpl;
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

    #[tokio::test]
    async fn test_array_distinct_array_of_primitives() {
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
            .await
            .unwrap()
            .iter()
            .map(|v| v.map(|s| s.into_scalar_impl()))
            .collect_vec();
        assert_eq!(actual, expected);
    }

    // More test cases are in e2e tests.
}

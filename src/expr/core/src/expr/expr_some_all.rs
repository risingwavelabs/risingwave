// Copyright 2025 RisingWave Labs
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

use risingwave_common::array::{Array, ArrayRef, BoolArray, DataChunk};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum, Scalar, ScalarRefImpl};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::{bail, ensure};
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::{ExprNode, FunctionCall};

use super::build::get_children_and_return_type;
use super::{BoxedExpression, Build, Expression};
use crate::Result;

#[derive(Debug)]
pub struct SomeAllExpression {
    left_expr: BoxedExpression,
    right_expr: BoxedExpression,
    expr_type: Type,
    func: BoxedExpression,
}

impl SomeAllExpression {
    pub fn new(
        left_expr: BoxedExpression,
        right_expr: BoxedExpression,
        expr_type: Type,
        func: BoxedExpression,
    ) -> Self {
        SomeAllExpression {
            left_expr,
            right_expr,
            expr_type,
            func,
        }
    }

    // Notice that this function may not exhaust the iterator,
    // so never pass an iterator created `by_ref`.
    fn resolve_bools(&self, bools: impl Iterator<Item = Option<bool>>) -> Option<bool> {
        match self.expr_type {
            Type::Some => {
                let mut any_none = false;
                for b in bools {
                    match b {
                        Some(true) => return Some(true),
                        Some(false) => continue,
                        None => any_none = true,
                    }
                }
                if any_none { None } else { Some(false) }
            }
            Type::All => {
                let mut all_true = true;
                for b in bools {
                    if b == Some(false) {
                        return Some(false);
                    }
                    if b != Some(true) {
                        all_true = false;
                    }
                }
                if all_true { Some(true) } else { None }
            }
            _ => unreachable!(),
        }
    }
}

#[async_trait::async_trait]
impl Expression for SomeAllExpression {
    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    async fn eval(&self, data_chunk: &DataChunk) -> Result<ArrayRef> {
        let arr_left = self.left_expr.eval(data_chunk).await?;
        let arr_right = self.right_expr.eval(data_chunk).await?;
        let mut num_array = vec![None; data_chunk.capacity()];

        let arr_right_inner = arr_right.as_list();
        let DataType::List(datatype) = arr_right_inner.data_type() else {
            unreachable!()
        };
        let capacity = arr_right_inner.flatten().len();

        let mut unfolded_arr_left_builder = arr_left.create_builder(capacity);
        let mut unfolded_arr_right_builder = datatype.create_array_builder(capacity);

        let mut unfolded_left_right =
            |left: Option<ScalarRefImpl<'_>>, right: Option<ScalarRefImpl<'_>>, index: usize| {
                if right.is_none() {
                    return;
                }

                let array = right.unwrap().into_list();
                let flattened = array.flatten();
                let len = flattened.len();

                num_array[index] = Some(len);
                unfolded_arr_left_builder.append_n(len, left);
                for item in flattened.iter() {
                    unfolded_arr_right_builder.append(item)
                }
            };

        if data_chunk.is_compacted() {
            for (idx, (left, right)) in arr_left.iter().zip_eq_fast(arr_right.iter()).enumerate() {
                unfolded_left_right(left, right, idx);
            }
        } else {
            for idx in data_chunk.visibility().iter_ones() {
                let left = arr_left.value_at(idx);
                let right = arr_right.value_at(idx);
                unfolded_left_right(left, right, idx);
            }
        }

        assert_eq!(num_array.len(), data_chunk.capacity());

        let unfolded_arr_left = unfolded_arr_left_builder.finish();
        let unfolded_arr_right = unfolded_arr_right_builder.finish();

        // Unfolded array are actually compacted, and the visibility of the output array will be
        // further restored by `num_array`.
        assert_eq!(unfolded_arr_left.len(), unfolded_arr_right.len());
        let unfolded_compact_len = unfolded_arr_left.len();

        let data_chunk = DataChunk::new(
            vec![unfolded_arr_left.into(), unfolded_arr_right.into()],
            unfolded_compact_len,
        );

        let func_results = self.func.eval(&data_chunk).await?;
        let bools = func_results.as_bool();
        let mut offset = 0;
        Ok(Arc::new(
            num_array
                .into_iter()
                .map(|num| match num {
                    Some(num) => {
                        let range = offset..offset + num;
                        offset += num;
                        self.resolve_bools(range.map(|i| bools.value_at(i)))
                    }
                    None => None,
                })
                .collect::<BoolArray>()
                .into(),
        ))
    }

    async fn eval_row(&self, row: &OwnedRow) -> Result<Datum> {
        let datum_left = self.left_expr.eval_row(row).await?;
        let datum_right = self.right_expr.eval_row(row).await?;
        let Some(array_right) = datum_right else {
            return Ok(None);
        };
        let array_right = array_right.into_list().into_array();
        let len = array_right.len();

        // expand left to array
        let array_left = {
            let mut builder = self.left_expr.return_type().create_array_builder(len);
            builder.append_n(len, datum_left);
            builder.finish().into_ref()
        };

        let chunk = DataChunk::new(vec![array_left, Arc::new(array_right)], len);
        let bools = self.func.eval(&chunk).await?;

        Ok(self
            .resolve_bools(bools.as_bool().iter())
            .map(|b| b.to_scalar_value()))
    }
}

impl Build for SomeAllExpression {
    fn build(
        prost: &ExprNode,
        build_child: impl Fn(&ExprNode) -> Result<BoxedExpression>,
    ) -> Result<Self> {
        let outer_expr_type = prost.get_function_type().unwrap();
        let (outer_children, outer_return_type) = get_children_and_return_type(prost)?;
        ensure!(matches!(outer_return_type, DataType::Boolean));

        let mut inner_expr_type = outer_children[0].get_function_type().unwrap();
        let (mut inner_children, mut inner_return_type) =
            get_children_and_return_type(&outer_children[0])?;
        let mut stack = vec![];
        while inner_children.len() != 2 {
            stack.push((inner_expr_type, inner_return_type));
            inner_expr_type = inner_children[0].get_function_type().unwrap();
            (inner_children, inner_return_type) = get_children_and_return_type(&inner_children[0])?;
        }

        let left_expr = build_child(&inner_children[0])?;
        let right_expr = build_child(&inner_children[1])?;

        let DataType::List(right_expr_return_type) = right_expr.return_type() else {
            bail!("Expect Array Type");
        };

        let eval_func = {
            let left_expr_input_ref = ExprNode {
                function_type: Type::Unspecified as i32,
                return_type: Some(left_expr.return_type().to_protobuf()),
                rex_node: Some(RexNode::InputRef(0)),
            };
            let right_expr_input_ref = ExprNode {
                function_type: Type::Unspecified as i32,
                return_type: Some(right_expr_return_type.to_protobuf()),
                rex_node: Some(RexNode::InputRef(1)),
            };
            let mut root_expr_node = ExprNode {
                function_type: inner_expr_type as i32,
                return_type: Some(inner_return_type.to_protobuf()),
                rex_node: Some(RexNode::FuncCall(FunctionCall {
                    children: vec![left_expr_input_ref, right_expr_input_ref],
                })),
            };
            while let Some((expr_type, return_type)) = stack.pop() {
                root_expr_node = ExprNode {
                    function_type: expr_type as i32,
                    return_type: Some(return_type.to_protobuf()),
                    rex_node: Some(RexNode::FuncCall(FunctionCall {
                        children: vec![root_expr_node],
                    })),
                }
            }
            build_child(&root_expr_node)?
        };

        Ok(SomeAllExpression::new(
            left_expr,
            right_expr,
            outer_expr_type,
            eval_func,
        ))
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::row::Row;
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_common::types::ToOwnedDatum;
    use risingwave_common::util::iter_util::ZipEqDebug;
    use risingwave_expr::expr::build_from_pretty;

    use super::*;

    #[tokio::test]
    async fn test_some() {
        let expr = SomeAllExpression::new(
            build_from_pretty("0:int4"),
            build_from_pretty("$0:boolean"),
            Type::Some,
            build_from_pretty("$1:boolean"),
        );
        let (input, expected) = DataChunk::from_pretty(
            "B[]        B
             .          .
             {}         f
             {NULL}     .
             {NULL,f}   .
             {NULL,t}   t
             {t,f}      t
             {f,t}      t", // <- regression test for #14214
        )
        .split_column_at(1);

        // test eval
        let output = expr.eval(&input).await.unwrap();
        assert_eq!(&output, expected.column_at(0));

        // test eval_row
        for (row, expected) in input.rows().zip_eq_debug(expected.rows()) {
            let result = expr.eval_row(&row.to_owned_row()).await.unwrap();
            assert_eq!(result, expected.datum_at(0).to_owned_datum());
        }
    }

    #[tokio::test]
    async fn test_all() {
        let expr = SomeAllExpression::new(
            build_from_pretty("0:int4"),
            build_from_pretty("$0:boolean"),
            Type::All,
            build_from_pretty("$1:boolean"),
        );
        let (input, expected) = DataChunk::from_pretty(
            "B[]        B
             .          .
             {}         t
             {NULL}     .
             {NULL,t}   .
             {NULL,f}   f
             {f,f}      f
             {t}        t", // <- regression test for #14214
        )
        .split_column_at(1);

        // test eval
        let output = expr.eval(&input).await.unwrap();
        assert_eq!(&output, expected.column_at(0));

        // test eval_row
        for (row, expected) in input.rows().zip_eq_debug(expected.rows()) {
            let result = expr.eval_row(&row.to_owned_row()).await.unwrap();
            assert_eq!(result, expected.datum_at(0).to_owned_datum());
        }
    }
}

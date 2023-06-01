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

use std::convert::TryFrom;
use std::ops::BitAnd;
use std::sync::Arc;

use risingwave_common::array::{ArrayRef, DataChunk, Vis, VisRef};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum};
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::ExprNode;

use crate::expr::{build_from_prost as expr_build_from_prost, BoxedExpression, Expression};
use crate::{bail, ensure, ExprError, Result};

#[derive(Debug)]
pub struct CoalesceExpression {
    return_type: DataType,
    children: Vec<BoxedExpression>,
}

#[async_trait::async_trait]
impl Expression for CoalesceExpression {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let init_vis = input.vis();
        let mut input = input.clone();
        let len = input.capacity();
        let mut selection: Vec<Option<usize>> = vec![None; len];
        let mut children_array = Vec::with_capacity(self.children.len());
        for (child_idx, child) in self.children.iter().enumerate() {
            let res = child.eval_checked(&input).await?;
            let res_bitmap = res.null_bitmap();
            let orig_vis = input.vis();
            let res_bitmap_ref: VisRef<'_> = res_bitmap.into();
            orig_vis
                .as_ref()
                .bitand(res_bitmap_ref)
                .iter_ones()
                .for_each(|pos| {
                    selection[pos] = Some(child_idx);
                });
            let res_vis: Vis = (!res_bitmap).into();
            let new_vis = orig_vis & res_vis;
            input.set_vis(new_vis);
            children_array.push(res);
        }
        let mut builder = self.return_type.create_array_builder(len);
        for (i, sel) in selection.iter().enumerate() {
            if init_vis.is_set(i) && let Some(child_idx) = sel {
                builder.append(children_array[*child_idx].value_at(i));
            } else {
                builder.append_null()
            }
        }
        Ok(Arc::new(builder.finish()))
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        for child in &self.children {
            let datum = child.eval_row(input).await?;
            if datum.is_some() {
                return Ok(datum);
            }
        }
        Ok(None)
    }
}

impl CoalesceExpression {
    pub fn new(return_type: DataType, children: Vec<BoxedExpression>) -> Self {
        CoalesceExpression {
            return_type,
            children,
        }
    }
}

impl<'a> TryFrom<&'a ExprNode> for CoalesceExpression {
    type Error = ExprError;

    fn try_from(prost: &'a ExprNode) -> Result<Self> {
        ensure!(prost.get_function_type().unwrap() == Type::Coalesce);

        let ret_type = DataType::from(prost.get_return_type().unwrap());
        let RexNode::FuncCall(func_call_node) = prost.get_rex_node().unwrap() else {
            bail!("Expected RexNode::FuncCall");
        };

        let children = func_call_node
            .children
            .to_vec()
            .iter()
            .map(expr_build_from_prost)
            .collect::<Result<Vec<_>>>()?;
        Ok(CoalesceExpression::new(ret_type, children))
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::DataChunk;
    use risingwave_common::row::OwnedRow;
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_common::types::{Scalar, ScalarImpl};
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::data::PbDataType;
    use risingwave_pb::expr::expr_node::RexNode;
    use risingwave_pb::expr::expr_node::Type::Coalesce;
    use risingwave_pb::expr::{ExprNode, FunctionCall};

    use crate::expr::expr_coalesce::CoalesceExpression;
    use crate::expr::test_utils::make_input_ref;
    use crate::expr::Expression;

    pub fn make_coalesce_function(children: Vec<ExprNode>, ret: TypeName) -> ExprNode {
        ExprNode {
            function_type: Coalesce as i32,
            return_type: Some(PbDataType {
                type_name: ret as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::FuncCall(FunctionCall { children })),
        }
    }

    #[tokio::test]
    async fn test_coalesce_expr() {
        let input_node1 = make_input_ref(0, TypeName::Int32);
        let input_node2 = make_input_ref(1, TypeName::Int32);
        let input_node3 = make_input_ref(2, TypeName::Int32);

        let data_chunk = DataChunk::from_pretty(
            "i i i
             1 . .
             . 2 .
             . . 3
             . . .",
        );

        let nullif_expr = CoalesceExpression::try_from(&make_coalesce_function(
            vec![input_node1, input_node2, input_node3],
            TypeName::Int32,
        ))
        .unwrap();
        let res = nullif_expr.eval(&data_chunk).await.unwrap();
        assert_eq!(res.datum_at(0), Some(ScalarImpl::Int32(1)));
        assert_eq!(res.datum_at(1), Some(ScalarImpl::Int32(2)));
        assert_eq!(res.datum_at(2), Some(ScalarImpl::Int32(3)));
        assert_eq!(res.datum_at(3), None);
    }

    #[tokio::test]
    async fn test_eval_row_coalesce_expr() {
        let input_node1 = make_input_ref(0, TypeName::Int32);
        let input_node2 = make_input_ref(1, TypeName::Int32);
        let input_node3 = make_input_ref(2, TypeName::Int32);

        let nullif_expr = CoalesceExpression::try_from(&make_coalesce_function(
            vec![input_node1, input_node2, input_node3],
            TypeName::Int32,
        ))
        .unwrap();

        let row_inputs = vec![
            vec![Some(1), None, None, None],
            vec![None, Some(2), None, None],
            vec![None, None, Some(3), None],
            vec![None, None, None, None],
        ];

        let expected = vec![
            Some(ScalarImpl::Int32(1)),
            Some(ScalarImpl::Int32(2)),
            Some(ScalarImpl::Int32(3)),
            None,
        ];

        for (i, row_input) in row_inputs.iter().enumerate() {
            let datum_vec = row_input
                .iter()
                .map(|o| o.map(|int| int.to_scalar_value()))
                .collect();
            let row = OwnedRow::new(datum_vec);

            let result = nullif_expr.eval_row(&row).await.unwrap();
            assert_eq!(result, expected[i]);
        }
    }
}

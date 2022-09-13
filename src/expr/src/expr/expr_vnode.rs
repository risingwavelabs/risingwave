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

use risingwave_common::array::{
    ArrayBuilder, ArrayImpl, ArrayRef, DataChunk, I16ArrayBuilder, Row,
};
use risingwave_common::types::{DataType, Datum};
use risingwave_common::util::hash_util::CRC32FastBuilder;
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::ExprNode;

use super::Expression;
use crate::expr::InputRefExpression;
use crate::{bail, ensure, ExprError, Result};

#[derive(Debug)]
pub struct VnodeExpression {
    dist_key_indices: Vec<usize>,
}

impl VnodeExpression {
    pub fn new(dist_key_indices: Vec<usize>) -> Self {
        VnodeExpression { dist_key_indices }
    }
}

impl<'a> TryFrom<&'a ExprNode> for VnodeExpression {
    type Error = ExprError;

    fn try_from(prost: &'a ExprNode) -> Result<Self> {
        ensure!(prost.get_expr_type().unwrap() == Type::Vnode);
        ensure!(DataType::from(prost.get_return_type().unwrap()) == DataType::Int16);

        let RexNode::FuncCall(func_call_node) = prost.get_rex_node().unwrap() else {
            bail!("Expected RexNode::FuncCall");
        };

        for child in func_call_node.get_children() {
            ensure!(child.get_expr_type().unwrap() == Type::InputRef);
        }

        let dist_key_input_refs = func_call_node
            .get_children()
            .iter()
            .map(InputRefExpression::try_from)
            .map(|res| res.map(|input| input.index()))
            .try_collect()?;

        Ok(VnodeExpression::new(dist_key_input_refs))
    }
}

impl Expression for VnodeExpression {
    fn return_type(&self) -> DataType {
        DataType::Int16
    }

    fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let hash_values = input.get_hash_values(&self.dist_key_indices, CRC32FastBuilder {})?;
        let mut builder = I16ArrayBuilder::new(input.capacity());
        hash_values
            .into_iter()
            .try_for_each(|h| builder.append(Some(h.to_vnode() as i16)))?;
        Ok(Arc::new(ArrayImpl::from(builder.finish())))
    }

    fn eval_row(&self, input: &Row) -> Result<Datum> {
        let dist_key_row = input.by_indices(&self.dist_key_indices);
        // FIXME: currently the implementation of the hash function in Row::hash_row differs from
        // Array::hash_at, so their result might be different. #3457
        let vnode = dist_key_row.hash_row(&CRC32FastBuilder {}).to_vnode() as i16;
        Ok(Some(vnode.into()))
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{DataChunk, DataChunkTestExt};
    use risingwave_common::types::VIRTUAL_NODE_COUNT;
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::data::DataType as ProstDataType;
    use risingwave_pb::expr::expr_node::RexNode;
    use risingwave_pb::expr::expr_node::Type::Vnode;
    use risingwave_pb::expr::{ExprNode, FunctionCall};

    use super::VnodeExpression;
    use crate::expr::test_utils::make_input_ref;
    use crate::expr::Expression;

    pub fn make_vnode_function(children: Vec<ExprNode>) -> ExprNode {
        ExprNode {
            expr_type: Vnode as i32,
            return_type: Some(ProstDataType {
                type_name: TypeName::Int16 as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::FuncCall(FunctionCall { children })),
        }
    }

    #[test]
    fn test_vnode_expr_eval() {
        let input_node1 = make_input_ref(0, TypeName::Int32);
        let input_node2 = make_input_ref(0, TypeName::Int64);
        let input_node3 = make_input_ref(0, TypeName::Varchar);
        let vnode_expr = VnodeExpression::try_from(&make_vnode_function(vec![
            input_node1,
            input_node2,
            input_node3,
        ]))
        .unwrap();
        let chunk = DataChunk::from_pretty(
            "i  I  T
             1  10 abc
             2  32 def
             3  88 ghi",
        );
        let actual = vnode_expr.eval(&chunk).unwrap();
        actual.iter().for_each(|vnode| {
            let vnode = vnode.unwrap().into_int16();
            assert!(vnode >= 0);
            assert!((vnode as usize) < VIRTUAL_NODE_COUNT);
        });
    }

    #[test]
    fn test_vnode_expr_eval_row() {
        let input_node1 = make_input_ref(0, TypeName::Int32);
        let input_node2 = make_input_ref(0, TypeName::Int64);
        let input_node3 = make_input_ref(0, TypeName::Varchar);
        let vnode_expr = VnodeExpression::try_from(&make_vnode_function(vec![
            input_node1,
            input_node2,
            input_node3,
        ]))
        .unwrap();
        let chunk = DataChunk::from_pretty(
            "i  I  T
             1  10 abc
             2  32 def
             3  88 ghi",
        );
        let rows: Vec<_> = chunk.rows().map(|row| row.to_owned_row()).collect();
        for row in rows {
            let actual = vnode_expr.eval_row(&row).unwrap();
            let vnode = actual.unwrap().into_int16();
            assert!(vnode >= 0);
            assert!((vnode as usize) < VIRTUAL_NODE_COUNT);
        }
    }
}

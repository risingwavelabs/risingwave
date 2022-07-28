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
    ArrayBuilder, ArrayImpl, ArrayRef, DataChunk, I32ArrayBuilder, Row,
};
use risingwave_common::types::{DataType, Datum};
use risingwave_common::util::hash_util::CRC32FastBuilder;
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::ExprNode;

use super::{build_from_prost as expr_build_from_prost, BoxedExpression, Expression};
use crate::{bail, ensure, ExprError, Result};

#[derive(Debug)]
pub struct VnodeExpression {
    dist_key_exprs: Vec<BoxedExpression>,
}

impl VnodeExpression {
    pub fn new(dist_key_exprs: Vec<BoxedExpression>) -> Self {
        VnodeExpression { dist_key_exprs }
    }
}

impl<'a> TryFrom<&'a ExprNode> for VnodeExpression {
    type Error = ExprError;

    fn try_from(prost: &'a ExprNode) -> Result<Self> {
        ensure!(prost.get_expr_type().unwrap() == Type::Vnode);
        ensure!(DataType::from(prost.get_return_type().unwrap()) == DataType::Int32);

        let RexNode::FuncCall(func_call_node) = prost.get_rex_node().unwrap() else {
            bail!("Expected RexNode::FuncCall");
        };

        let dist_key_exprs = func_call_node
            .children
            .iter()
            .map(expr_build_from_prost)
            .collect::<Result<Vec<_>>>()?;

        Ok(VnodeExpression::new(dist_key_exprs))
    }
}

impl Expression for VnodeExpression {
    fn return_type(&self) -> DataType {
        DataType::Int32
    }

    fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let dist_key_columns = self
            .dist_key_exprs
            .iter()
            .map(|c| c.eval_checked(input))
            .collect::<Result<Vec<_>>>()?;

        let row_len = input.capacity();
        let vis = input.vis();
        let mut builder = I32ArrayBuilder::new(row_len);

        for row_idx in 0..row_len {
            if !vis.is_set(row_idx) {
                builder.append(None)?;
                continue;
            }

            let dist_key = dist_key_columns
                .iter()
                .map(|col| col.datum_at(row_idx))
                .collect();
            let dist_key_row = Row::new(dist_key);
            let vnode = dist_key_row.hash_row(&CRC32FastBuilder {}).to_vnode() as i32;
            builder.append(Some(vnode.into()))?;
        }
        let output = builder.finish()?;
        Ok(Arc::new(ArrayImpl::from(output)))
    }

    fn eval_row(&self, input: &Row) -> Result<Datum> {
        let dist_key = self
            .dist_key_exprs
            .iter()
            .map(|c| c.eval_row(input))
            .collect::<Result<Vec<_>>>()?;
        let dist_key_row = Row::new(dist_key);
        let vnode = dist_key_row.hash_row(&CRC32FastBuilder {}).to_vnode() as i32;
        Ok(Some(vnode.into()))
    }
}

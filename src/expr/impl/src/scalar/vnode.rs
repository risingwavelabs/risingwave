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

use anyhow::Context;
use itertools::Itertools;
use risingwave_common::array::{ArrayBuilder, ArrayImpl, ArrayRef, DataChunk, I16ArrayBuilder};
use risingwave_common::hash::VirtualNode;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum};
use risingwave_expr::expr::{
    AsyncExpression, AsyncExpressionBoxExt, BoxedExpression, ExpressionInfo, SyncExpression,
    SyncExpressionBoxExt, try_into_sync_exprs,
};
use risingwave_expr::{Result, build_function, expr_context};

#[derive(Debug)]
struct VnodeExpression<E> {
    /// `Some` if it's from the first argument of user-facing function `VnodeUser` (`rw_vnode`),
    /// `None` if it's from the internal function `Vnode`.
    vnode_count: Option<usize>,

    /// A list of expressions to get the distribution key columns. Typically `InputRef`.
    children: Vec<E>,

    /// Normally, we pass the distribution key indices to `VirtualNode::compute_xx` functions.
    /// But in this case, all children columns are used to compute vnode. So we cache a vector of
    /// all indices here and pass it later to reduce allocation.
    all_indices: Vec<usize>,
}

#[build_function("vnode(...) -> int2")]
fn build(_: DataType, children: Vec<BoxedExpression>) -> Result<BoxedExpression> {
    let all_indices = (0..children.len()).collect();
    match try_into_sync_exprs(children) {
        Ok(children) => Ok(VnodeExpression {
            vnode_count: None,
            all_indices,
            children,
        }
        .boxed()),
        Err(children) => Ok(VnodeExpression {
            vnode_count: None,
            all_indices,
            children,
        }
        .boxed()),
    }
}

#[build_function("vnode_user(...) -> int2")]
fn build_user(_: DataType, children: Vec<BoxedExpression>) -> Result<BoxedExpression> {
    let mut children = children.into_iter();

    let vnode_count = children
        .next()
        .unwrap() // always exist, argument number enforced in binder
        .eval_const() // required to be constant
        .context("the first argument (vnode count) must be a constant")?
        .context("the first argument (vnode count) must not be NULL")?
        .into_int32(); // always int32, casted during type inference

    if !(1i32..=VirtualNode::MAX_COUNT as i32).contains(&vnode_count) {
        return Err(anyhow::anyhow!(
            "the first argument (vnode count) must be in range 1..={}",
            VirtualNode::MAX_COUNT
        )
        .into());
    }

    let children = children.collect_vec();
    let all_indices = (0..children.len()).collect();
    match try_into_sync_exprs(children) {
        Ok(children) => Ok(VnodeExpression {
            vnode_count: Some(vnode_count.try_into().unwrap()),
            all_indices,
            children,
        }
        .boxed()),
        Err(children) => Ok(VnodeExpression {
            vnode_count: Some(vnode_count.try_into().unwrap()),
            all_indices,
            children,
        }
        .boxed()),
    }
}

impl<E: ExpressionInfo> ExpressionInfo for VnodeExpression<E> {
    fn return_type(&self) -> DataType {
        DataType::Int16
    }
}

macro_rules! eval_vnode {
    ($mode:ident, $this:expr, $input:expr) => {{
        let mut arrays = Vec::with_capacity($this.children.len());
        for child in &$this.children {
            arrays.push(risingwave_expr::forward!($mode, child, eval($input))?);
        }
        let input = DataChunk::new(arrays, $input.visibility().clone());

        let vnodes = VirtualNode::compute_chunk(&input, &$this.all_indices, $this.vnode_count()?);
        let mut builder = I16ArrayBuilder::new(input.capacity());
        vnodes
            .into_iter()
            .for_each(|vnode| builder.append(Some(vnode.to_scalar())));
        Ok(Arc::new(ArrayImpl::from(builder.finish())))
    }};
}

macro_rules! eval_row_vnode {
    ($mode:ident, $this:expr, $input:expr) => {{
        let mut datums = Vec::with_capacity($this.children.len());
        for child in &$this.children {
            datums.push(risingwave_expr::forward!($mode, child, eval_row($input))?);
        }
        let input = OwnedRow::new(datums);

        Ok(Some(
            VirtualNode::compute_row(input, &$this.all_indices, $this.vnode_count()?)
                .to_scalar()
                .into(),
        ))
    }};
}

impl<E: SyncExpression> SyncExpression for VnodeExpression<E> {
    fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        eval_vnode!(sync, self, input)
    }

    fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        eval_row_vnode!(sync, self, input)
    }
}

impl<E: AsyncExpression> AsyncExpression for VnodeExpression<E> {
    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        eval_vnode!(async, self, input)
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        eval_row_vnode!(async, self, input)
    }
}

impl<E> VnodeExpression<E> {
    fn vnode_count(&self) -> Result<usize> {
        if let Some(vnode_count) = self.vnode_count {
            Ok(vnode_count)
        } else {
            expr_context::vnode_count()
        }
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{DataChunk, DataChunkTestExt};
    use risingwave_common::row::Row;
    use risingwave_expr::expr::build_from_pretty;
    use risingwave_expr::expr_context::VNODE_COUNT;

    #[tokio::test]
    async fn test_vnode_expr_eval() {
        let vnode_count = 32;
        let expr = build_from_pretty("(vnode:int2 $0:int4 $0:int8 $0:varchar)");
        let input = DataChunk::from_pretty(
            "i  I  T
             1  10 abc
             2  32 def
             3  88 ghi",
        );

        // test eval
        let output = VNODE_COUNT::scope(vnode_count, expr.eval(&input))
            .await
            .unwrap();
        for vnode in output.iter() {
            let vnode = vnode.unwrap().into_int16();
            assert!((0..vnode_count as i16).contains(&vnode));
        }

        // test eval_row
        for row in input.rows() {
            let result = VNODE_COUNT::scope(vnode_count, expr.eval_row(&row.to_owned_row()))
                .await
                .unwrap();
            let vnode = result.unwrap().into_int16();
            assert!((0..vnode_count as i16).contains(&vnode));
        }
    }
}

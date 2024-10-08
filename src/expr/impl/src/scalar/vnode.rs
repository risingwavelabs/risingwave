// Copyright 2024 RisingWave Labs
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

use risingwave_common::array::{ArrayBuilder, ArrayImpl, ArrayRef, DataChunk, I16ArrayBuilder};
use risingwave_common::hash::VirtualNode;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum};
use risingwave_expr::expr::{BoxedExpression, Expression};
use risingwave_expr::expr_context::vnode_count;
use risingwave_expr::{build_function, Result};

#[derive(Debug)]
struct VnodeExpression {
    /// A list of expressions to get the distribution key columns. Typically `InputRef`.
    children: Vec<BoxedExpression>,

    /// Normally, we pass the distribution key indices to `VirtualNode::compute_xx` functions.
    /// But in this case, all children columns are used to compute vnode. So we cache a vector of
    /// all indices here and pass it later to reduce allocation.
    all_indices: Vec<usize>,
}

#[build_function("vnode(...) -> int2")]
fn build(_: DataType, children: Vec<BoxedExpression>) -> Result<BoxedExpression> {
    Ok(Box::new(VnodeExpression {
        all_indices: (0..children.len()).collect(),
        children,
    }))
}

#[async_trait::async_trait]
impl Expression for VnodeExpression {
    fn return_type(&self) -> DataType {
        DataType::Int16
    }

    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let mut arrays = Vec::with_capacity(self.children.len());
        for child in &self.children {
            arrays.push(child.eval(input).await?);
        }
        let input = DataChunk::new(arrays, input.visibility().clone());

        let vnodes = VirtualNode::compute_chunk(&input, &self.all_indices, vnode_count());
        let mut builder = I16ArrayBuilder::new(input.capacity());
        vnodes
            .into_iter()
            .for_each(|vnode| builder.append(Some(vnode.to_scalar())));
        Ok(Arc::new(ArrayImpl::from(builder.finish())))
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        let mut datums = Vec::with_capacity(self.children.len());
        for child in &self.children {
            datums.push(child.eval_row(input).await?);
        }
        let input = OwnedRow::new(datums);

        Ok(Some(
            VirtualNode::compute_row(input, &self.all_indices, vnode_count())
                .to_scalar()
                .into(),
        ))
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

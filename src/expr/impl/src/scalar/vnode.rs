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

use risingwave_common::array::{ArrayBuilder, ArrayImpl, ArrayRef, DataChunk, I16ArrayBuilder};
use risingwave_common::hash::VirtualNode;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum};
use risingwave_expr::expr::{BoxedExpression, Expression};
use risingwave_expr::{build_function, Result};

#[derive(Debug)]
struct VnodeExpression {
    dist_key_indices: Vec<usize>,
}

#[build_function("vnode(...) -> int2")]
fn build(_: DataType, children: Vec<BoxedExpression>) -> Result<BoxedExpression> {
    let dist_key_indices = children
        .into_iter()
        .map(|child| child.input_ref_index().unwrap())
        .collect();

    Ok(Box::new(VnodeExpression { dist_key_indices }))
}

#[async_trait::async_trait]
impl Expression for VnodeExpression {
    fn return_type(&self) -> DataType {
        DataType::Int16
    }

    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let vnodes = VirtualNode::compute_chunk(input, &self.dist_key_indices);
        let mut builder = I16ArrayBuilder::new(input.capacity());
        vnodes
            .into_iter()
            .for_each(|vnode| builder.append(Some(vnode.to_scalar())));
        Ok(Arc::new(ArrayImpl::from(builder.finish())))
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        Ok(Some(
            VirtualNode::compute_row(input, &self.dist_key_indices)
                .to_scalar()
                .into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{DataChunk, DataChunkTestExt};
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::row::Row;
    use risingwave_expr::expr::build_from_pretty;

    #[tokio::test]
    async fn test_vnode_expr_eval() {
        let expr = build_from_pretty("(vnode:int2 $0:int4 $0:int8 $0:varchar)");
        let input = DataChunk::from_pretty(
            "i  I  T
             1  10 abc
             2  32 def
             3  88 ghi",
        );

        // test eval
        let output = expr.eval(&input).await.unwrap();
        for vnode in output.iter() {
            let vnode = vnode.unwrap().into_int16();
            assert!((0..VirtualNode::COUNT as i16).contains(&vnode));
        }

        // test eval_row
        for row in input.rows() {
            let result = expr.eval_row(&row.to_owned_row()).await.unwrap();
            let vnode = result.unwrap().into_int16();
            assert!((0..VirtualNode::COUNT as i16).contains(&vnode));
        }
    }
}

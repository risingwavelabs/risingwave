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

use std::collections::BinaryHeap;
use std::sync::Arc;
use std::vec::Vec;

use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::error::{Result, RwError};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::encoding_for_comparison::encode_chunk;
use risingwave_common::util::sort_util::OrderPair;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};
use crate::task::BatchTaskContext;

/// Top-N Executor
///
/// Use a N-heap to store the smallest N rows.
pub struct TopNExecutor {
    child: BoxedExecutor,
    order_pairs: Vec<OrderPair>,
    offset: usize,
    limit: usize,
    schema: Schema,
    identity: String,
}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for TopNExecutor {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<'_, C>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        let [child]: [_; 1] = inputs.try_into().unwrap();

        let top_n_node =
            try_match_expand!(source.plan_node().get_node_body().unwrap(), NodeBody::TopN)?;

        let order_pairs = top_n_node
            .column_orders
            .iter()
            .map(OrderPair::from_prost)
            .collect();
        Ok(Box::new(Self::new(
            child,
            order_pairs,
            top_n_node.get_offset() as usize,
            top_n_node.get_limit() as usize,
            source.plan_node().get_identity().clone(),
        )))
    }
}

impl TopNExecutor {
    pub fn new(
        child: BoxedExecutor,
        order_pairs: Vec<OrderPair>,
        offset: usize,
        limit: usize,
        identity: String,
    ) -> Self {
        let schema = child.schema().clone();
        Self {
            child,
            order_pairs,
            offset,
            limit,
            schema,
            identity,
        }
    }
}

impl Executor for TopNExecutor {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.do_execute()
    }
}

/// A max-heap used to find the smallest `limit+offset` items.
pub struct TopNHeap {
    heap: BinaryHeap<HeapElem>,
    limit: usize,
    offset: usize,
}

impl TopNHeap {
    pub fn new(limit: usize, offset: usize) -> Self {
        assert!(limit > 0);
        Self {
            heap: BinaryHeap::with_capacity(limit + offset),
            limit,
            offset,
        }
    }

    pub fn push(&mut self, elem: HeapElem) {
        if self.heap.len() < self.limit + self.offset {
            self.heap.push(elem);
        } else {
            let mut peek = self.heap.peek_mut().unwrap();
            if elem < *peek {
                *peek = elem;
            }
        }
    }

    /// Returns the elements in the range `[offset, offset+limit)`.
    pub fn dump(self) -> impl Iterator<Item = HeapElem> {
        self.heap
            .into_iter_sorted()
            .collect_vec()
            .into_iter()
            .rev()
            .skip(self.offset)
    }
}

pub struct HeapElem {
    pub encoded_row: Vec<u8>,
    pub chunk: Arc<DataChunk>,
    pub row_id: usize,
}

impl PartialEq for HeapElem {
    fn eq(&self, other: &Self) -> bool {
        self.encoded_row.eq(&other.encoded_row)
    }
}

impl Eq for HeapElem {}

impl PartialOrd for HeapElem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.encoded_row.partial_cmp(&other.encoded_row)
    }
}

impl Ord for HeapElem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.encoded_row.cmp(&other.encoded_row)
    }
}

impl TopNExecutor {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(self: Box<Self>) {
        if self.limit == 0 {
            return Ok(());
        }
        let mut heap = TopNHeap::new(self.limit, self.offset);

        #[for_await]
        for chunk in self.child.execute() {
            let chunk = Arc::new(chunk?.compact());
            for (row_id, encoded_row) in encode_chunk(&chunk, &self.order_pairs)
                .into_iter()
                .enumerate()
            {
                heap.push(HeapElem {
                    encoded_row,
                    chunk: chunk.clone(),
                    row_id,
                });
            }
        }

        let mut chunk_builder = DataChunkBuilder::with_default_size(self.schema.data_types());
        for HeapElem { chunk, row_id, .. } in heap.dump() {
            if let Some(spilled) =
                chunk_builder.append_one_row_ref(chunk.row_at_unchecked_vis(row_id))
            {
                yield spilled
            }
        }
        if let Some(spilled) = chunk_builder.consume_all() {
            yield spilled
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::stream::StreamExt;
    use itertools::Itertools;
    use risingwave_common::array::{Array, DataChunk};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::OrderType;

    use super::*;
    use crate::executor::test_utils::MockExecutor;

    #[tokio::test]
    async fn test_simple_top_n_executor() {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
            ],
        };
        let mut mock_executor = MockExecutor::new(schema);
        mock_executor.add(DataChunk::from_pretty(
            "i i
             1 5
             2 4
             3 3
             4 2
             5 1",
        ));
        let order_pairs = vec![
            OrderPair {
                column_idx: 1,
                order_type: OrderType::Ascending,
            },
            OrderPair {
                column_idx: 0,
                order_type: OrderType::Ascending,
            },
        ];
        let top_n_executor = Box::new(TopNExecutor::new(
            Box::new(mock_executor),
            order_pairs,
            1,
            3,
            "TopNExecutor".to_string(),
        ));
        let fields = &top_n_executor.schema().fields;
        assert_eq!(fields[0].data_type, DataType::Int32);
        assert_eq!(fields[1].data_type, DataType::Int32);

        let mut stream = top_n_executor.execute();
        let res = stream.next().await;

        assert!(matches!(res, Some(_)));
        if let Some(res) = res {
            let res = res.unwrap();
            assert_eq!(res.cardinality(), 3);
            assert_eq!(
                res.column_at(0).array().as_int32().iter().collect_vec(),
                vec![Some(4), Some(3), Some(2)]
            );
        }

        let res = stream.next().await;
        assert!(matches!(res, None));
    }

    #[tokio::test]
    async fn test_limit_0() {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
            ],
        };
        let mut mock_executor = MockExecutor::new(schema);
        mock_executor.add(DataChunk::from_pretty(
            "i i
             1 5
             2 4
             3 3
             4 2
             5 1",
        ));
        let order_pairs = vec![
            OrderPair {
                column_idx: 1,
                order_type: OrderType::Ascending,
            },
            OrderPair {
                column_idx: 0,
                order_type: OrderType::Ascending,
            },
        ];
        let top_n_executor = Box::new(TopNExecutor::new(
            Box::new(mock_executor),
            order_pairs,
            1,
            0,
            "TopNExecutor".to_string(),
        ));
        let fields = &top_n_executor.schema().fields;
        assert_eq!(fields[0].data_type, DataType::Int32);
        assert_eq!(fields[1].data_type, DataType::Int32);

        let mut stream = top_n_executor.execute();
        let res = stream.next().await;

        assert!(matches!(res, None));
    }
}

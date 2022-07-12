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

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::vec::Vec;

use futures_async_stream::try_stream;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::error::{Result, RwError};
use risingwave_common::util::chunk_coalesce::DEFAULT_CHUNK_BUFFER_SIZE;
use risingwave_common::util::sort_util::{HeapElem, OrderPair};
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};
use crate::task::BatchTaskContext;

struct TopNHeap {
    order_pairs: Arc<Vec<OrderPair>>,
    min_heap: BinaryHeap<Reverse<HeapElem>>,
    size: usize,
}

impl TopNHeap {
    fn insert(&mut self, elem: HeapElem) {
        if self.min_heap.len() < self.size {
            self.min_heap.push(Reverse(elem));
        } else if elem > self.min_heap.peek().unwrap().0 {
            self.min_heap.push(Reverse(elem));
            self.min_heap.pop();
        }
    }

    pub fn fit(&mut self, chunk: DataChunk) {
        DataChunk::rechunk(&[chunk], 1)
            .unwrap()
            .into_iter()
            .for_each(|c| {
                let elem = HeapElem {
                    order_pairs: self.order_pairs.clone(),
                    chunk: c,
                    chunk_idx: 0usize, // useless
                    elem_idx: 0usize,
                    encoded_chunk: None,
                };
                self.insert(elem);
            });
    }

    pub fn dump(&mut self, offset: usize) -> Option<DataChunk> {
        if self.min_heap.is_empty() {
            return None;
        }
        let mut chunks = self
            .min_heap
            .drain_sorted()
            .map(|e| e.0.chunk)
            .collect::<Vec<_>>();
        chunks.reverse();

        if offset >= chunks.len() {
            return None;
        }

        // Skip the first `offset` elements
        if let Ok(mut res) = DataChunk::rechunk(&chunks[offset..], self.size - offset) {
            assert_eq!(res.len(), 1);
            Some(res.remove(0))
        } else {
            None
        }
    }
}

pub struct TopNExecutor {
    child: BoxedExecutor,
    top_n_heap: TopNHeap,
    identity: String,
    chunk_size: usize,
    offset: usize,
}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for TopNExecutor {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<C>,
        mut inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        ensure!(
            inputs.len() == 1,
            "TopNExecutor should have only one child!"
        );

        let top_n_node =
            try_match_expand!(source.plan_node().get_node_body().unwrap(), NodeBody::TopN)?;

        let order_pairs = top_n_node
            .column_orders
            .iter()
            .map(OrderPair::from_prost)
            .collect();
        Ok(Box::new(Self::new(
            inputs.remove(0),
            order_pairs,
            top_n_node.get_limit() as usize,
            top_n_node.get_offset() as usize,
            source.plan_node().get_identity().clone(),
            DEFAULT_CHUNK_BUFFER_SIZE,
        )))
    }
}

impl TopNExecutor {
    fn new(
        child: BoxedExecutor,
        order_pairs: Vec<OrderPair>,
        limit: usize,
        offset: usize,
        identity: String,
        chunk_size: usize,
    ) -> Self {
        Self {
            top_n_heap: TopNHeap {
                min_heap: BinaryHeap::new(),
                size: limit + offset,
                order_pairs: Arc::new(order_pairs),
            },
            child,
            identity,
            chunk_size,
            offset,
        }
    }
}

impl Executor for TopNExecutor {
    fn schema(&self) -> &Schema {
        self.child.schema()
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.do_execute()
    }
}

impl TopNExecutor {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(mut self: Box<Self>) {
        #[for_await]
        for data_chunk in self.child.execute() {
            let data_chunk = data_chunk?;
            self.top_n_heap.fit(data_chunk);
        }

        if let Some(data_chunk) = self.top_n_heap.dump(self.offset) {
            let batch_chunks = DataChunk::rechunk(&[data_chunk], DEFAULT_CHUNK_BUFFER_SIZE)?;
            for ret_chunk in batch_chunks {
                yield ret_chunk
            }
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
            3,
            1,
            "TopNExecutor2".to_string(),
            DEFAULT_CHUNK_BUFFER_SIZE,
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
}

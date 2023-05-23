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

use std::cmp::Ordering;
use std::sync::Arc;
use std::vec::Vec;

use futures_async_stream::try_stream;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::error::{Result, RwError};
use risingwave_common::estimate_size::collections::MemMonitoredHeap;
use risingwave_common::estimate_size::EstimateSize;
use risingwave_common::memory::MemoryContext;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::memcmp_encoding::encode_chunk;
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_common_proc_macro::EstimateSize;
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
    column_orders: Vec<ColumnOrder>,
    offset: usize,
    limit: usize,
    with_ties: bool,
    schema: Schema,
    identity: String,
    chunk_size: usize,
    mem_ctx: MemoryContext,
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

        let column_orders = top_n_node
            .column_orders
            .iter()
            .map(ColumnOrder::from_protobuf)
            .collect();

        let identity = source.plan_node().get_identity();

        Ok(Box::new(Self::new(
            child,
            column_orders,
            top_n_node.get_offset() as usize,
            top_n_node.get_limit() as usize,
            top_n_node.get_with_ties(),
            identity.clone(),
            source.context.get_config().developer.chunk_size,
            source.context().create_executor_mem_context(identity),
        )))
    }
}

impl TopNExecutor {
    pub fn new(
        child: BoxedExecutor,
        column_orders: Vec<ColumnOrder>,
        offset: usize,
        limit: usize,
        with_ties: bool,
        identity: String,
        chunk_size: usize,
        mem_ctx: MemoryContext,
    ) -> Self {
        let schema = child.schema().clone();
        Self {
            child,
            column_orders,
            offset,
            limit,
            with_ties,
            schema,
            identity,
            chunk_size,
            mem_ctx,
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

pub const MAX_TOPN_INIT_HEAP_CAPACITY: usize = 1024;

/// A max-heap used to find the smallest `limit+offset` items.
pub struct TopNHeap {
    heap: MemMonitoredHeap<HeapElem>,
    limit: usize,
    offset: usize,
    with_ties: bool,
}

impl TopNHeap {
    pub fn new(limit: usize, offset: usize, with_ties: bool, mem_ctx: MemoryContext) -> Self {
        assert!(limit > 0);
        Self {
            heap: MemMonitoredHeap::with_capacity(
                (limit + offset).min(MAX_TOPN_INIT_HEAP_CAPACITY),
                mem_ctx,
            ),
            limit,
            offset,
            with_ties,
        }
    }

    pub fn push(&mut self, elem: HeapElem) {
        if self.heap.len() < self.limit + self.offset {
            self.heap.push(elem);
        } else {
            // heap is full
            if !self.with_ties {
                let peek = self.heap.pop().unwrap();
                if elem < peek {
                    self.heap.push(elem);
                } else {
                    self.heap.push(peek);
                }
            } else {
                let peek = self.heap.peek().unwrap().clone();
                match elem.cmp(&peek) {
                    Ordering::Less => {
                        let mut ties_with_peek = vec![];
                        // pop all the ties with peek
                        ties_with_peek.push(self.heap.pop().unwrap());
                        while let Some(e) = self.heap.peek() && e.encoded_row == peek.encoded_row {
                            ties_with_peek.push(self.heap.pop().unwrap());
                        }
                        self.heap.push(elem);
                        // If the size is smaller than limit, we can push all the elements back.
                        if self.heap.len() < self.limit {
                            self.heap.extend(ties_with_peek);
                        }
                    }
                    Ordering::Equal => {
                        // It's a tie.
                        self.heap.push(elem);
                    }
                    Ordering::Greater => {}
                }
            }
        }
    }

    /// Returns the elements in the range `[offset, offset+limit)`.
    pub fn dump(self) -> impl Iterator<Item = HeapElem> {
        self.heap.into_sorted_vec().into_iter().skip(self.offset)
    }
}

#[derive(Clone, EstimateSize)]
pub struct HeapElem {
    encoded_row: Vec<u8>,
    row: OwnedRow,
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

impl HeapElem {
    pub fn new(encoded_row: Vec<u8>, row: impl Row) -> Self {
        Self {
            encoded_row,
            row: row.into_owned_row(),
        }
    }

    pub fn encoded_row(&self) -> &[u8] {
        &self.encoded_row
    }

    pub fn row(&self) -> impl Row + '_ {
        &self.row
    }
}

impl TopNExecutor {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(self: Box<Self>) {
        if self.limit == 0 {
            return Ok(());
        }
        let mut heap = TopNHeap::new(
            self.limit,
            self.offset,
            self.with_ties,
            self.mem_ctx.clone(),
        );

        #[for_await]
        for chunk in self.child.execute() {
            let chunk = Arc::new(chunk?.compact());
            for (row_id, encoded_row) in encode_chunk(&chunk, &self.column_orders)?
                .into_iter()
                .enumerate()
            {
                heap.push(HeapElem {
                    encoded_row,
                    row: chunk.row_at(row_id).0.to_owned_row(),
                });
            }
        }

        let mut chunk_builder = DataChunkBuilder::new(self.schema.data_types(), self.chunk_size);
        for HeapElem { row, .. } in heap.dump() {
            if let Some(spilled) = chunk_builder.append_one_row(row) {
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

    const CHUNK_SIZE: usize = 1024;

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
        let column_orders = vec![
            ColumnOrder {
                column_index: 1,
                order_type: OrderType::ascending(),
            },
            ColumnOrder {
                column_index: 0,
                order_type: OrderType::ascending(),
            },
        ];
        let top_n_executor = Box::new(TopNExecutor::new(
            Box::new(mock_executor),
            column_orders,
            1,
            3,
            false,
            "TopNExecutor".to_string(),
            CHUNK_SIZE,
            MemoryContext::none(),
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
                res.column_at(0).as_int32().iter().collect_vec(),
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
        let column_orders = vec![
            ColumnOrder {
                column_index: 1,
                order_type: OrderType::ascending(),
            },
            ColumnOrder {
                column_index: 0,
                order_type: OrderType::ascending(),
            },
        ];
        let top_n_executor = Box::new(TopNExecutor::new(
            Box::new(mock_executor),
            column_orders,
            1,
            0,
            false,
            "TopNExecutor".to_string(),
            CHUNK_SIZE,
            MemoryContext::none(),
        ));
        let fields = &top_n_executor.schema().fields;
        assert_eq!(fields[0].data_type, DataType::Int32);
        assert_eq!(fields[1].data_type, DataType::Int32);

        let mut stream = top_n_executor.execute();
        let res = stream.next().await;

        assert!(matches!(res, None));
    }
}

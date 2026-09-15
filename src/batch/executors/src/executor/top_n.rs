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

use std::cmp::Ordering;
use std::sync::Arc;

use futures_async_stream::try_stream;
use prometheus::core::Atomic;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::memory::{MemMonitoredHeap, MemoryContext};
use risingwave_common::metrics::TrAdderAtomic;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::memcmp_encoding::{MemcmpEncoded, encode_chunk};
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::error::{BatchError, Result};
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};

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

impl BoxedExecutorBuilder for TopNExecutor {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
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
            source.context().get_config().developer.chunk_size,
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

    // Only used for swapping out the heap in hashmap, due to a bug in hashmap which forbids us from
    // using `into_iter`. We should remove this after Hashmap upgraded and fixed the bug.
    pub fn empty() -> Self {
        Self {
            heap: MemMonitoredHeap::with_capacity(0, MemoryContext::none()),
            limit: 0,
            offset: 0,
            with_ties: false,
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
                // let inner = self.heap.inner();
                // let mut peek = inner.peek_mut().unwrap();
                // if elem < *peek {
                //     *peek = elem;
                // }
            } else {
                let peek = self.heap.peek().unwrap().clone();
                match elem.cmp(&peek) {
                    Ordering::Less => {
                        let mut ties_with_peek = vec![];
                        // pop all the ties with peek
                        ties_with_peek.push(self.heap.pop().unwrap());
                        while let Some(e) = self.heap.peek()
                            && e.encoded_row == peek.encoded_row
                        {
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
    ///
    /// # Warning
    ///
    /// `deallocate` only subtracts the backing-storage size. The elements may already have been
    /// dropped, so it cannot determine the size of their separately allocated memory. Those charges
    /// need separate cleanup, either explicitly or through a private memory context. A future
    /// redesign should make this handling automatic.
    ///
    /// In this executor example, the heap is charged directly to a new private `mem_ctx`. Skipped
    /// and consumed elements stay charged until the context is dropped on normal return, errors,
    /// or cancellation, even though the elements themselves are dropped earlier.
    ///
    /// ```rust,ignore
    /// let mem_ctx = MemoryContext::new(Some(parent), TrAdderAtomic::new(0));
    /// let mut heap = TopNHeap::new(3, 1, false, mem_ctx.clone());
    /// for elem in input_elements {
    ///     heap.push(elem);
    /// }
    /// for elem in heap.dump() {
    ///     let output = chunk_builder.append_one_row(elem.row());
    ///     drop(elem);
    ///     if let Some(output) = output {
    ///         yield output;
    ///     }
    /// }
    /// ```
    pub fn dump(self) -> impl Iterator<Item = HeapElem> {
        self.heap
            .into_sorted_vec()
            .into_iter()
            .rev()
            .skip(self.offset)
    }
}

#[derive(Clone, EstimateSize)]
pub struct HeapElem {
    encoded_row: MemcmpEncoded,
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
        Some(self.cmp(other))
    }
}

impl Ord for HeapElem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.encoded_row.cmp(&other.encoded_row)
    }
}

impl HeapElem {
    pub fn new(encoded_row: MemcmpEncoded, row: impl Row) -> Self {
        Self {
            encoded_row,
            row: row.into_owned_row(),
        }
    }

    pub fn row(&self) -> impl Row + '_ {
        &self.row
    }
}

impl TopNExecutor {
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        if self.limit == 0 {
            return Ok(());
        }
        // Keep payload charges until this execution exits, including errors and cancellation.
        let mem_ctx = MemoryContext::new(Some(self.mem_ctx.clone()), TrAdderAtomic::new(0));
        let mut heap = TopNHeap::new(self.limit, self.offset, self.with_ties, mem_ctx.clone());

        #[for_await]
        for chunk in self.child.execute() {
            let chunk = Arc::new(chunk?.compact_vis());
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
        for elem in heap.dump() {
            let output = chunk_builder.append_one_row(elem.row());
            drop(elem);
            if let Some(output) = output {
                yield output
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
    use risingwave_common::array::Array;
    use risingwave_common::catalog::Field;
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::OrderType;

    use super::*;
    use crate::executor::test_utils::MockExecutor;

    const CHUNK_SIZE: usize = 1024;

    mod memory_budget {
        use super::*;
        use crate::executor::test_utils::memory_cleanup::{self, Exit};

        /// Verifies that Top-N releases retained rows and sort keys above a zero-byte child budget
        /// after input errors, cancellation while waiting for input or after an output, and EOF.
        /// The shared contexts stay alive and unrelated parent usage is preserved.
        ///
        /// Equivalent query: `SELECT k FROM input ORDER BY k LIMIT 2 OFFSET 1`.
        /// Input: `[A, B, C]`, where each value repeats its lowercase letter 128 times.
        /// Expected output: `[B, C]` at EOF, or `[B]` before output cancellation.
        /// Input-error and pending-input cases stop at input EOF before producing output.
        #[tokio::test]
        async fn test_over_budget_top_n_memory_cleanup() {
            let (parent, child) = memory_cleanup::contexts();
            assert_eq!(child.mem_limit(), 0);
            for exit in [
                Exit::InputError,
                Exit::PendingInput,
                Exit::Output,
                Exit::Eof,
            ] {
                let (a, b, c) = ("a".repeat(128), "b".repeat(128), "c".repeat(128));
                let input = memory_cleanup::input(
                    Box::new(MockExecutor::with_chunk(
                        DataChunk::from_pretty(&format!("T\n{a}\n{b}\n{c}")),
                        Schema::new(vec![Field::unnamed(DataType::Varchar)]),
                    )),
                    exit,
                    child.clone(),
                );
                let exec = TopNExecutor::new(
                    input,
                    vec![ColumnOrder::new(0, OrderType::ascending())],
                    1,
                    2,
                    false,
                    "top-n-cleanup".into(),
                    1,
                    child.clone(),
                );
                let expected = match exit {
                    Exit::InputError | Exit::PendingInput => None,
                    Exit::Output => Some(DataChunk::from_pretty(&format!("T\n{b}"))),
                    Exit::Eof => Some(DataChunk::from_pretty(&format!("T\n{b}\n{c}"))),
                };
                memory_cleanup::assert_released(
                    Box::new(exec).execute(),
                    exit,
                    expected.as_ref(),
                    &child,
                    &parent,
                )
                .await;
            }
        }

        /// Verifies that Top-N keeps row and sort-key payloads charged above a zero-byte child
        /// budget throughout output, including skipped and consumed rows, then releases all charges
        /// at EOF. Each offset runs twice with the same shared contexts to check for accumulation.
        ///
        /// Equivalent query: `SELECT k FROM input ORDER BY k LIMIT 3 OFFSET $1`, with offsets 0, 1, 3.
        /// Input: `[A, B, C]`, each repeating its lowercase letter 128 times.
        /// Expected outputs: `[A, B, C]`, `[B, C]`, and no rows, respectively.
        #[tokio::test]
        async fn test_over_budget_top_n_accounting_during_execution() {
            let (parent, child) = memory_cleanup::contexts();
            assert_eq!(child.mem_limit(), 0);
            for offset in [0, 1, 3] {
                for _ in 0..2 {
                    assert_eq!(child.get_bytes_used(), 0);
                    assert_eq!(parent.get_bytes_used(), 16);
                    let keys = ["a".repeat(128), "b".repeat(128), "c".repeat(128)];
                    let chunk = DataChunk::from_pretty(&format!("T\n{}", keys.join("\n")));
                    let orders = vec![ColumnOrder::new(0, OrderType::ascending())];
                    let payload_charge = encode_chunk(&chunk, &orders)
                        .unwrap()
                        .into_iter()
                        .enumerate()
                        .map(|(row_id, encoded)| {
                            HeapElem::new(encoded, chunk.row_at(row_id).0).estimated_heap_size()
                                as i64
                        })
                        .sum::<i64>();
                    assert!(payload_charge > 0);
                    // Also observe live accounting at input EOF when the offset skips all rows.
                    let input = memory_cleanup::input(
                        Box::new(MockExecutor::with_chunk(
                            chunk,
                            Schema::new(vec![Field::unnamed(DataType::Varchar)]),
                        )),
                        Exit::Eof,
                        child.clone(),
                    );
                    let exec = TopNExecutor::new(
                        input,
                        orders,
                        offset,
                        3,
                        false,
                        "top-n-accounting".into(),
                        1,
                        child.clone(),
                    );
                    let mut output = Box::new(exec).execute();
                    for key in keys.iter().skip(offset) {
                        assert_eq!(
                            output.next().await.unwrap().unwrap(),
                            DataChunk::from_pretty(&format!("T\n{key}"))
                        );
                        // All payloads stay charged until execution exits, including skipped and
                        // consumed rows. The iterator's backing storage adds its own charge.
                        assert!(child.get_bytes_used() > payload_charge);
                        assert_eq!(parent.get_bytes_used(), child.get_bytes_used() + 16);
                        assert!(!child.check_memory_usage());
                    }
                    assert!(output.next().await.is_none());
                    // Check before dropping the stream or either shared context.
                    assert_eq!(child.get_bytes_used(), 0);
                    assert_eq!(parent.get_bytes_used(), 16);
                    drop(output);
                }
            }
        }
    }

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
            "TopNExecutor".to_owned(),
            CHUNK_SIZE,
            MemoryContext::none(),
        ));
        let fields = &top_n_executor.schema().fields;
        assert_eq!(fields[0].data_type, DataType::Int32);
        assert_eq!(fields[1].data_type, DataType::Int32);

        let mut stream = top_n_executor.execute();
        let res = stream.next().await;

        assert!(res.is_some());
        if let Some(res) = res {
            let res = res.unwrap();
            assert_eq!(res.cardinality(), 3);
            assert_eq!(
                res.column_at(0).as_int32().iter().collect_vec(),
                vec![Some(4), Some(3), Some(2)]
            );
        }

        let res = stream.next().await;
        assert!(res.is_none());
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
            "TopNExecutor".to_owned(),
            CHUNK_SIZE,
            MemoryContext::none(),
        ));
        let fields = &top_n_executor.schema().fields;
        assert_eq!(fields[0].data_type, DataType::Int32);
        assert_eq!(fields[1].data_type, DataType::Int32);

        let mut stream = top_n_executor.execute();
        let res = stream.next().await;

        assert!(res.is_none());
    }
}

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

use std::mem;
use std::sync::Arc;

use futures_async_stream::try_stream;
use futures_util::StreamExt;
use itertools::Itertools;
use prometheus::core::Atomic;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::memory::{MemMonitoredHeap, MemoryContext, MonitoredGlobalAlloc};
use risingwave_common::metrics::TrAdderAtomic;
use risingwave_common::types::ToOwnedDatum;
use risingwave_common::util::sort_util::{ColumnOrder, HeapElem};
use risingwave_common_estimate_size::EstimateSize;

use super::{BoxedDataChunkStream, BoxedExecutor, Executor};
use crate::error::{BatchError, Result};

pub struct MergeSortExecutor {
    inputs: Vec<BoxedExecutor>,
    column_orders: Arc<Vec<ColumnOrder>>,
    identity: String,
    schema: Schema,
    chunk_size: usize,
    mem_context: MemoryContext,
    min_heap: MemMonitoredHeap<HeapElem>,
    current_chunks: Vec<Option<DataChunk>, MonitoredGlobalAlloc>,
}

impl Executor for MergeSortExecutor {
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

impl MergeSortExecutor {
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_execute(mut self: Box<Self>) {
        let mut inputs = vec![];
        mem::swap(&mut inputs, &mut self.inputs);
        let mut input_streams = inputs
            .into_iter()
            .map(|input| input.execute())
            .collect_vec();
        debug_assert!(
            self.current_chunks.is_empty(),
            "merge-sort input slots must be empty before execution"
        );
        for input_idx in 0..input_streams.len() {
            self.current_chunks.push(None);
            // Initial chunks need the same charge as replacements: both are released on retirement.
            self.get_input_chunk(&mut input_streams, input_idx).await?;
            if let Some(chunk) = &self.current_chunks[input_idx] {
                let next_row_idx = chunk.next_visible_row_idx(0);
                self.push_row_into_heap(input_idx, next_row_idx.unwrap());
            }
        }

        while !self.min_heap.is_empty() {
            // It is possible that we cannot produce this much as
            // we may run out of input data chunks from sources.
            let mut want_to_produce = self.chunk_size;

            let mut builders: Vec<_> = self
                .schema
                .fields
                .iter()
                .map(|field| field.data_type.create_array_builder(self.chunk_size))
                .collect();
            let mut array_len = 0;
            while want_to_produce > 0 && !self.min_heap.is_empty() {
                let top_elem = self.min_heap.pop().unwrap();
                let child_idx = top_elem.chunk_idx();
                let cur_chunk = top_elem.chunk();
                let row_idx = top_elem.elem_idx();
                for (idx, builder) in builders.iter_mut().enumerate() {
                    let chunk_arr = cur_chunk.column_at(idx);
                    let chunk_arr = chunk_arr.as_ref();
                    let datum = chunk_arr.value_at(row_idx).to_owned_datum();
                    builder.append(&datum);
                }
                want_to_produce -= 1;
                array_len += 1;
                // check whether we have another row from the same chunk being popped
                let possible_next_row_idx = cur_chunk.next_visible_row_idx(row_idx + 1);
                match possible_next_row_idx {
                    Some(next_row_idx) => {
                        self.push_row_into_heap(child_idx, next_row_idx);
                    }
                    None => {
                        self.get_input_chunk(&mut input_streams, child_idx).await?;
                        if let Some(chunk) = &self.current_chunks[child_idx] {
                            let next_row_idx = chunk.next_visible_row_idx(0);
                            self.push_row_into_heap(child_idx, next_row_idx.unwrap());
                        }
                    }
                }
            }

            let columns = builders
                .into_iter()
                .map(|builder| builder.finish().into())
                .collect::<Vec<_>>();
            let chunk = DataChunk::new(columns, array_len);
            yield chunk
        }
    }

    async fn get_input_chunk(
        &mut self,
        input_streams: &mut Vec<BoxedDataChunkStream>,
        input_idx: usize,
    ) -> Result<()> {
        assert!(input_idx < input_streams.len());
        let res = input_streams[input_idx].next().await;
        let old = match res {
            Some(chunk) => {
                let chunk = chunk?;
                assert_ne!(chunk.cardinality(), 0);
                let new_chunk_size = chunk.estimated_heap_size() as i64;
                let old = self.current_chunks[input_idx].replace(chunk);
                self.mem_context.add_unchecked(new_chunk_size);
                old
            }
            None => std::mem::take(&mut self.current_chunks[input_idx]),
        };

        if let Some(chunk) = old {
            // Reduce the heap size of retired chunk
            self.mem_context
                .add_unchecked(-(chunk.estimated_heap_size() as i64));
        }

        Ok(())
    }

    fn push_row_into_heap(&mut self, input_idx: usize, row_idx: usize) {
        assert!(input_idx < self.current_chunks.len());
        let chunk_ref = self.current_chunks[input_idx].as_ref().unwrap();
        self.min_heap.push(HeapElem::new(
            self.column_orders.clone(),
            chunk_ref.clone(),
            input_idx,
            row_idx,
            None,
        ));
    }
}

impl MergeSortExecutor {
    pub fn new(
        inputs: Vec<BoxedExecutor>,
        column_orders: Arc<Vec<ColumnOrder>>,
        schema: Schema,
        identity: String,
        chunk_size: usize,
        mem_context: MemoryContext,
    ) -> Self {
        // Create the private context before allocating either container. Once this executor and
        // its allocators are dropped, even unfinished chunk and heap charges leave the parent.
        let mem_context = MemoryContext::new(Some(mem_context), TrAdderAtomic::new(0));
        let inputs_num = inputs.len();
        Self {
            inputs,
            column_orders,
            identity,
            schema,
            chunk_size,
            min_heap: MemMonitoredHeap::with_capacity(inputs_num, mem_context.clone()),
            current_chunks: Vec::with_capacity_in(inputs_num, mem_context.global_allocator()),
            mem_context,
        }
    }
}

#[cfg(test)]
mod memory_budget {
    use risingwave_common::array::DataChunkTestExt;
    use risingwave_common::catalog::Field;
    use risingwave_common::metrics::LabelGuardedIntGauge;
    use risingwave_common::types::DataType;

    use super::*;
    use crate::executor::test_utils::MockExecutor;

    /// Verifies that a replacement-input error or dropping the stream while waiting for a
    /// replacement or after an output releases chunks and heap memory, preserving parent usage.
    ///
    /// Equivalent query: `SELECT v FROM input ORDER BY v` (the merge step on sorted input).
    /// Input: `[1]` for the error/wait cases; fetching the next chunk fails or waits before output.
    /// Input: `[1, 2]` for the output case; expected first output: `[1]`, with row 2 still buffered.
    #[tokio::test]
    async fn test_merge_sort_cleanup_on_error_and_cancellation() {
        use crate::executor::test_utils::memory_cleanup::{self, Exit};

        let (parent, child) = memory_cleanup::contexts();
        for exit in [Exit::InputError, Exit::PendingInput, Exit::Output] {
            let chunk = if matches!(exit, Exit::Output) {
                DataChunk::from_pretty("i\n1\n2")
            } else {
                DataChunk::from_pretty("i\n1")
            };
            let schema = Schema::new(vec![Field::unnamed(DataType::Int32)]);
            let input = memory_cleanup::input(
                Box::new(MockExecutor::with_chunk(chunk, schema.clone())),
                exit,
                child.clone(),
            );
            let exec = MergeSortExecutor::new(
                vec![input],
                Arc::new(vec![ColumnOrder::new(0, Default::default())]),
                schema,
                "merge-cleanup".into(),
                1,
                child.clone(),
            );
            let expected = matches!(exit, Exit::Output).then(|| DataChunk::from_pretty("i\n1"));
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

    /// Verifies that merge sort returns the expected rows even above budget, with memory usage
    /// greater than the expected current-chunk charge while loading, replacing, and retiring chunks.
    /// Completion releases all charges while the shared context remains alive.
    ///
    /// Equivalent query: `SELECT v FROM input ORDER BY v` (the merge step on sorted input).
    /// Input chunks: `[1, 2]`, then `[3, 4, 5]`, from one already-sorted input stream.
    /// Expected output: `[1, 2, 3, 4, 5]`, returned one row at a time.
    #[tokio::test]
    async fn test_over_budget_chunk_accounting_during_execution() {
        let parent = MemoryContext::root(LabelGuardedIntGauge::test_int_gauge::<4>(), 64);
        assert!(parent.add(16));
        let child = MemoryContext::new_with_mem_limit(
            Some(parent.clone()),
            LabelGuardedIntGauge::test_int_gauge::<4>(),
            0,
        );
        let first = DataChunk::from_pretty("i\n1\n2");
        let second = DataChunk::from_pretty("i\n3\n4\n5");
        let first_size = first.estimated_heap_size() as i64;
        let second_size = second.estimated_heap_size() as i64;
        let schema = Schema::new(vec![Field::unnamed(DataType::Int32)]);
        let mut input = MockExecutor::new(schema.clone());
        input.add(first);
        input.add(second);
        let exec = MergeSortExecutor::new(
            vec![Box::new(input)],
            Arc::new(vec![ColumnOrder::new(0, Default::default())]),
            schema,
            "accounting-test".into(),
            1,
            child.clone(),
        );
        let mut output = Box::new(exec).execute();
        // Each output contains one row. Exhausting a chunk fetches its replacement before yielding.
        for (row, chunk_charge) in [
            (1, first_size),  // The initial chunk is still current.
            (2, second_size), // The initial chunk has been replaced.
            (3, second_size),
            (4, second_size),
            (5, 0), // EOF retires the last chunk, leaving only container backing allocations.
        ] {
            assert_eq!(
                output.next().await.unwrap().unwrap(),
                DataChunk::from_pretty(&format!("i\n{row}"))
            );
            // Container backing allocations add to the current chunk's charge.
            assert!(child.get_bytes_used() > chunk_charge);
            assert_eq!(parent.get_bytes_used(), child.get_bytes_used() + 16);
            if chunk_charge > 0 {
                // The chunk alone exceeds the child's zero-byte limit.
                assert!(chunk_charge > child.mem_limit() as i64);
                assert!(!child.check_memory_usage());
            }
        }
        // All accounting assertions above run before executor/context destruction.
        assert!(output.next().await.is_none());
        assert_eq!(child.get_bytes_used(), 0);
        assert_eq!(parent.get_bytes_used(), 16);
    }
}

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
use risingwave_common::util::sort_util::{ColumnOrder, HeapElem};
use risingwave_common_estimate_size::EstimateSize;

use super::{BoxedDataChunkStream, BoxedExecutor, Executor};
use crate::error::{BatchError, Result};
use crate::spill::spill_file::BoundedChunkBuilder;

pub struct MergeSortExecutor {
    inputs: Vec<BoxedExecutor>,
    column_orders: Arc<Vec<ColumnOrder>>,
    identity: String,
    schema: Schema,
    chunk_size: usize,
    max_chunk_bytes: usize,
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

        let mut builder = BoundedChunkBuilder::new(
            self.schema.data_types(),
            self.chunk_size,
            self.max_chunk_bytes,
        );
        while let Some(top_elem) = self.min_heap.pop() {
            let child_idx = top_elem.chunk_idx();
            let cur_chunk = top_elem.chunk();
            let row_idx = top_elem.elem_idx();
            let output = builder.append_row(cur_chunk.row_at_unchecked_vis(row_idx));
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
            // It may hold the last reference to a chunk that was just replaced.
            drop(top_elem);
            if let Some(chunk) = output {
                yield chunk;
            }
        }
        if let Some(chunk) = builder.consume_all() {
            yield chunk;
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
            max_chunk_bytes: usize::MAX,
            min_heap: MemMonitoredHeap::with_capacity(inputs_num, mem_context.clone()),
            current_chunks: Vec::with_capacity_in(inputs_num, mem_context.global_allocator()),
            mem_context,
        }
    }

    /// Also ends an output chunk once its arrays hold `max_chunk_bytes`.
    pub fn with_max_chunk_bytes(mut self, max_chunk_bytes: usize) -> Self {
        self.max_chunk_bytes = max_chunk_bytes;
        self
    }
}

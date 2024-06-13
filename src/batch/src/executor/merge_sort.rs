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
use futures_async_stream::try_stream;
use futures_util::StreamExt;
use itertools::Itertools;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::memory::{MemMonitoredHeap, MemoryContext, MonitoredGlobalAlloc};
use risingwave_common::util::sort_util::{ColumnOrder, HeapElem};
use risingwave_common_estimate_size::EstimateSize;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};
use crate::error::{BatchError, Result};
use crate::task::BatchTaskContext;

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
        let mut input_streams = self.inputs.into_iter().map(|input| input.execute()).collect_vec();
        for mut input_stream in &mut input_streams {
            match input_stream.next().await {
                Some(chunk) => {
                    let chunk = chunk?;
                    self.current_chunks.push(Some(chunk));
                }
                None => {
                    self.current_chunks.push(None);
                }
            }
        }


    }

    async fn get_input_chunk(&mut self, input_streams: &mut Vec<BoxedDataChunkStream>, input_idx: usize) -> Result<()> {
        assert!(input_idx < input_streams.len());
        let res = input_streams[input_idx].next().await;
        let old = match res {
            Some(chunk) => {
                let chunk = chunk?;
                assert_ne!(chunk.cardinality(), 0);
                let new_chunk_size = chunk.estimated_heap_size() as i64;
                let old = std::mem::replace(&mut self.current_chunks[input_idx], Some(chunk));
                self.mem_context.add(new_chunk_size);
                old
            }
            None => std::mem::take(&mut self.current_chunks[input_idx]),
        };

        if let Some(chunk) = old {
            // Reduce the heap size of retired chunk
            self.mem_context.add(-(chunk.estimated_heap_size() as i64));
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
        column_orders: Vec<ColumnOrder>,
        schema: Schema,
        identity: String,
        chunk_size: usize,
        mem_context: MemoryContext,
    ) -> Self {
        let inputs_num = inputs.len();
        Self {
            inputs,
            column_orders: Arc::new(column_orders),
            identity,
            schema,
            chunk_size,
            min_heap: MemMonitoredHeap::with_capacity(inputs_num, mem_context.clone()),
            current_chunks: Vec::with_capacity_in(inputs_num, mem_context.global_allocator()),
            mem_context,
        }
    }
}


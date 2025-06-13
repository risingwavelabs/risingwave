// Copyright 2025 RisingWave Labs
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

use std::collections::VecDeque;

use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;

use super::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
    register_executor,
};
use crate::error::{BatchError, Result};
use crate::exchange_source::{ExchangeData, ExchangeSource};
use crate::task::TaskId;

#[derive(Debug, Clone)]
pub struct FakeExchangeSource {
    chunks: Vec<Option<DataChunk>>,
}

impl FakeExchangeSource {
    pub fn new(chunks: Vec<Option<DataChunk>>) -> Self {
        Self { chunks }
    }
}

impl ExchangeSource for FakeExchangeSource {
    async fn take_data(&mut self) -> Result<Option<ExchangeData>> {
        if let Some(chunk) = self.chunks.pop() {
            Ok(chunk.map(ExchangeData::DataChunk))
        } else {
            Ok(None)
        }
    }

    fn get_task_id(&self) -> TaskId {
        TaskId::default()
    }
}

// Following executors are only for testing.
register_executor!(BlockExecutor, BlockExecutor);
register_executor!(BusyLoopExecutor, BusyLoopExecutor);

/// Mock the input of executor.
/// You can bind one or more `MockExecutor` as the children of the executor to test,
/// (`HashAgg`, e.g), so that allow testing without instantiating real `SeqScan`s and real storage.
pub struct MockExecutor {
    chunks: VecDeque<DataChunk>,
    schema: Schema,
    identity: String,
}

impl MockExecutor {
    pub fn new(schema: Schema) -> Self {
        Self {
            chunks: VecDeque::new(),
            schema,
            identity: "MockExecutor".to_owned(),
        }
    }

    pub fn with_chunk(chunk: DataChunk, schema: Schema) -> Self {
        let mut ret = Self::new(schema);
        ret.add(chunk);
        ret
    }

    pub fn add(&mut self, chunk: DataChunk) {
        self.chunks.push_back(chunk);
    }
}

impl Executor for MockExecutor {
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

impl MockExecutor {
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        for data_chunk in self.chunks {
            yield data_chunk;
        }
    }
}

pub struct BlockExecutor;

impl Executor for BlockExecutor {
    fn schema(&self) -> &Schema {
        unimplemented!("Not used in test")
    }

    fn identity(&self) -> &str {
        "BlockExecutor"
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.do_execute().boxed()
    }
}

impl BlockExecutor {
    #[try_stream(ok = DataChunk, error = BatchError)]
    async fn do_execute(self) {
        // infinite loop to block
        #[allow(clippy::empty_loop)]
        loop {}
    }
}

impl BoxedExecutorBuilder for BlockExecutor {
    async fn new_boxed_executor(
        _source: &ExecutorBuilder<'_>,
        _inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        Ok(Box::new(BlockExecutor))
    }
}

pub struct BusyLoopExecutor;

impl Executor for BusyLoopExecutor {
    fn schema(&self) -> &Schema {
        unimplemented!("Not used in test")
    }

    fn identity(&self) -> &str {
        "BusyLoopExecutor"
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.do_execute().boxed()
    }
}

impl BusyLoopExecutor {
    #[try_stream(ok = DataChunk, error = BatchError)]
    async fn do_execute(self) {
        // infinite loop to generate data
        loop {
            yield DataChunk::new_dummy(1);
        }
    }
}

impl BoxedExecutorBuilder for BusyLoopExecutor {
    async fn new_boxed_executor(
        _source: &ExecutorBuilder<'_>,
        _inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        Ok(Box::new(BusyLoopExecutor))
    }
}

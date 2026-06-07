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

use futures::StreamExt;
use futures::future::{BoxFuture, FutureExt};
use futures::stream::BoxStream;
use futures_async_stream::try_stream;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_pb::batch_plan::PbScanRange;
use risingwave_pb::plan_common::StorageTableDesc;
pub use risingwave_storage::table::batch_table::PkScanRange as ScanRange;

use crate::error::{BatchError, Result};
use crate::executor::{
    BatchPipelineOperator, BoxedDataChunkStream, BoxedExecutor, Executor, PushContext, PushSink,
    PushStatus, execute_push_as_pull, push_chunk_stream_with_operators,
};

pub type BoxedDataChunkListStream = BoxStream<'static, Result<Vec<DataChunk>>>;

/// Read at least `rows` rows.
#[try_stream(boxed, ok = Vec<DataChunk>, error = BatchError)]
pub async fn batch_read(mut stream: BoxedDataChunkStream, rows: usize) {
    let mut cnt = 0;
    let mut chunk_list = vec![];
    while let Some(build_chunk) = stream.next().await {
        let build_chunk = build_chunk?;
        cnt += build_chunk.cardinality();
        chunk_list.push(build_chunk);
        if cnt < rows {
            continue;
        } else {
            yield chunk_list;
            cnt = 0;
            chunk_list = vec![];
        }
    }
    if !chunk_list.is_empty() {
        yield chunk_list;
    }
}

pub struct BufferChunkExecutor {
    schema: Schema,
    chunk_list: Vec<DataChunk>,
}

impl Executor for BufferChunkExecutor {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        "BufferChunkExecutor"
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.do_execute()
    }

    fn execute_push<'a>(
        self: Box<Self>,
        _context: PushContext,
        sink: &'a mut dyn PushSink,
    ) -> BoxFuture<'a, Result<PushStatus>> {
        async move {
            for chunk in self.chunk_list {
                if sink.push(chunk).await?.is_finished() {
                    break;
                }
            }
            sink.finish().await
        }
        .boxed()
    }
}

impl BufferChunkExecutor {
    pub fn new(schema: Schema, chunk_list: Vec<DataChunk>) -> Self {
        Self { schema, chunk_list }
    }

    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_execute(self) {
        for chunk in self.chunk_list {
            yield chunk
        }
    }
}

pub struct DummyExecutor {
    pub schema: Schema,
}

impl Executor for DummyExecutor {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        "dummy"
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        DummyExecutor::do_nothing()
    }

    fn execute_push<'a>(
        self: Box<Self>,
        _context: PushContext,
        sink: &'a mut dyn PushSink,
    ) -> BoxFuture<'a, Result<PushStatus>> {
        async move { sink.finish().await }.boxed()
    }
}

impl DummyExecutor {
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_nothing() {}
}

pub struct WrapStreamExecutor {
    schema: Schema,
    stream: BoxedDataChunkStream,
}

impl WrapStreamExecutor {
    pub fn new(schema: Schema, stream: BoxedDataChunkStream) -> Self {
        Self { schema, stream }
    }
}

impl Executor for WrapStreamExecutor {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        "WrapStreamExecutor"
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.stream
    }

    fn execute_push<'a>(
        self: Box<Self>,
        context: PushContext,
        sink: &'a mut dyn PushSink,
    ) -> BoxFuture<'a, Result<PushStatus>> {
        self.execute_push_with_operators(context, vec![], sink)
    }

    fn execute_push_with_operators<'a>(
        self: Box<Self>,
        context: PushContext,
        operators: Vec<Box<dyn BatchPipelineOperator>>,
        sink: &'a mut dyn PushSink,
    ) -> BoxFuture<'a, Result<PushStatus>> {
        push_chunk_stream_with_operators(self.stream, operators, context, sink).boxed()
    }
}

pub fn wrap_push_executor(executor: BoxedExecutor, context: PushContext) -> BoxedExecutor {
    let schema = executor.schema().clone();
    let stream = execute_push_as_pull(executor, context.clone(), context.morsel_budget());
    Box::new(WrapStreamExecutor::new(schema, stream))
}

pub fn build_scan_ranges_from_pb(
    scan_ranges: &Vec<PbScanRange>,
    table_desc: &StorageTableDesc,
) -> Result<Vec<ScanRange>> {
    Ok(ScanRange::build_from_protobuf(scan_ranges, table_desc)?)
}

pub fn build_scan_range_from_pb(
    scan_range: &PbScanRange,
    table_desc: &StorageTableDesc,
) -> Result<ScanRange> {
    Ok(ScanRange::from_protobuf(scan_range, table_desc)?)
}

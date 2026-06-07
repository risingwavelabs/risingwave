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

use futures::future::{BoxFuture, FutureExt};
use futures_async_stream::try_stream;
use futures_util::stream::StreamExt;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_connector::source::iceberg::{
    FileScanBackend, extract_bucket_and_file_name, new_gcs_operator, read_parquet_file,
};
use risingwave_pb::batch_plan::file_scan_node;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::error::{BatchError, Result};
use crate::executor::{
    BatchPipelineOperatorChain, BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder,
    Executor, ExecutorBuilder, Morsel, MorselSource, PushContext, PushSink, PushStatus,
    drive_morsel_source_with_operators, push_chunk_stream_with_operators,
};

#[derive(PartialEq, Debug)]
pub enum FileFormat {
    Parquet,
}

/// Gcs file scan executor. Currently only support parquet file format.
pub struct GcsFileScanExecutor {
    file_format: FileFormat,
    file_location: Vec<String>,
    gcs_credential: String,
    batch_size: usize,
    schema: Schema,
    identity: String,
}

impl Executor for GcsFileScanExecutor {
    fn schema(&self) -> &risingwave_common::catalog::Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> super::BoxedDataChunkStream {
        self.do_execute().boxed()
    }

    fn execute_push<'a>(
        self: Box<Self>,
        context: PushContext,
        sink: &'a mut dyn PushSink,
    ) -> BoxFuture<'a, crate::error::Result<PushStatus>> {
        self.execute_push_with_operators(context, BatchPipelineOperatorChain::empty(), sink)
    }

    fn execute_push_with_operators<'a>(
        self: Box<Self>,
        context: PushContext,
        operators: BatchPipelineOperatorChain,
        sink: &'a mut dyn PushSink,
    ) -> BoxFuture<'a, crate::error::Result<PushStatus>> {
        if context.morsel_parallelism() > 1 {
            return async move {
                let Self {
                    file_format,
                    file_location,
                    gcs_credential,
                    batch_size,
                    ..
                } = *self;
                let source = GcsFileScanMorselSource::new(
                    file_format,
                    file_location,
                    gcs_credential,
                    batch_size,
                );
                drive_morsel_source_with_operators(source, operators, context, sink).await
            }
            .boxed();
        }
        push_chunk_stream_with_operators(self.do_execute().boxed(), operators, context, sink)
            .boxed()
    }
}

struct GcsFileScanMorselSource {
    file_format: FileFormat,
    file_location: std::vec::IntoIter<String>,
    gcs_credential: String,
    batch_size: usize,
    active_file_stream: Option<BoxedDataChunkStream>,
    next_sequence: u64,
}

impl GcsFileScanMorselSource {
    fn new(
        file_format: FileFormat,
        file_location: Vec<String>,
        gcs_credential: String,
        batch_size: usize,
    ) -> Self {
        Self {
            file_format,
            file_location: file_location.into_iter(),
            gcs_credential,
            batch_size,
            active_file_stream: None,
            next_sequence: 0,
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn read_file(file: String, gcs_credential: String, batch_size: usize) {
        let (bucket, file_name) = extract_bucket_and_file_name(&file, &FileScanBackend::Gcs)?;
        let op = new_gcs_operator(gcs_credential, bucket)?;
        let chunk_stream =
            read_parquet_file(op, file_name, None, None, false, batch_size, 0, None, None).await?;
        #[for_await]
        for stream_chunk in chunk_stream {
            let stream_chunk = stream_chunk?;
            let (data_chunk, _) = stream_chunk.into_parts();
            yield data_chunk;
        }
    }
}

impl MorselSource for GcsFileScanMorselSource {
    fn next_morsel<'a>(
        &'a mut self,
        context: &'a PushContext,
    ) -> BoxFuture<'a, Result<Option<Morsel>>> {
        async move {
            assert_eq!(self.file_format, FileFormat::Parquet);
            context.check_shutdown()?;
            loop {
                let active_file_chunk = match &mut self.active_file_stream {
                    Some(stream) => stream.next().await,
                    None => None,
                };
                if let Some(chunk) = active_file_chunk {
                    context.check_shutdown()?;
                    let sequence = self.next_sequence;
                    self.next_sequence += 1;
                    return Ok(Some(Morsel::new(sequence, chunk?)));
                }
                if self.active_file_stream.is_some() {
                    self.active_file_stream = None;
                    continue;
                }

                let Some(file) = self.file_location.next() else {
                    return Ok(None);
                };
                self.active_file_stream = Some(Self::read_file(
                    file,
                    self.gcs_credential.clone(),
                    self.batch_size,
                ));
            }
        }
        .boxed()
    }
}

impl GcsFileScanExecutor {
    pub fn new(
        file_format: FileFormat,
        file_location: Vec<String>,
        gcs_credential: String,
        batch_size: usize,
        schema: Schema,
        identity: String,
    ) -> Self {
        Self {
            file_format,
            file_location,
            gcs_credential,
            batch_size,
            schema,
            identity,
        }
    }

    #[try_stream(ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        assert_eq!(self.file_format, FileFormat::Parquet);
        for file in self.file_location {
            let (bucket, file_name) = extract_bucket_and_file_name(&file, &FileScanBackend::Gcs)?;
            let op = new_gcs_operator(self.gcs_credential.clone(), bucket.clone())?;
            let chunk_stream = read_parquet_file(
                op,
                file_name,
                None,
                None,
                false,
                self.batch_size,
                0,
                None,
                None,
            )
            .await?;
            #[for_await]
            for stream_chunk in chunk_stream {
                let stream_chunk = stream_chunk?;
                let (data_chunk, _) = stream_chunk.into_parts();
                yield data_chunk;
            }
        }
    }
}

pub struct GcsFileScanExecutorBuilder {}

impl BoxedExecutorBuilder for GcsFileScanExecutorBuilder {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        _inputs: Vec<BoxedExecutor>,
    ) -> crate::error::Result<BoxedExecutor> {
        let file_scan_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::GcsFileScan
        )?;

        Ok(Box::new(GcsFileScanExecutor::new(
            match file_scan_node::FileFormat::try_from(file_scan_node.file_format).unwrap() {
                file_scan_node::FileFormat::Parquet => FileFormat::Parquet,
                file_scan_node::FileFormat::Unspecified => unreachable!(),
            },
            file_scan_node.file_location.clone(),
            file_scan_node.credential.clone(),
            source.context().get_config().developer.chunk_size,
            Schema::from_iter(file_scan_node.columns.iter().map(Field::from)),
            source.plan_node().get_identity().clone(),
        )))
    }
}

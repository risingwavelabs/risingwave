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

use anyhow::anyhow;
use futures_async_stream::try_stream;
use futures_util::stream::StreamExt;
use parquet::arrow::ProjectionMask;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::catalog::{Field, Schema};
use risingwave_connector::source::iceberg::parquet_file_reader::create_parquet_stream_builder;
use risingwave_pb::batch_plan::file_scan_node;
use risingwave_pb::batch_plan::file_scan_node::StorageType;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::error::BatchError;
use crate::executor::{BoxedExecutor, BoxedExecutorBuilder, DataChunk, Executor, ExecutorBuilder};
use crate::task::BatchTaskContext;

#[derive(PartialEq, Debug)]
pub enum FileFormat {
    Parquet,
}

/// S3 file scan executor. Currently only support parquet file format.
pub struct S3FileScanExecutor {
    file_format: FileFormat,
    file_location: Vec<String>,
    s3_region: String,
    s3_access_key: String,
    s3_secret_key: String,
    batch_size: usize,
    schema: Schema,
    identity: String,
}

impl Executor for S3FileScanExecutor {
    fn schema(&self) -> &risingwave_common::catalog::Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> super::BoxedDataChunkStream {
        self.do_execute().boxed()
    }
}

impl S3FileScanExecutor {
    pub fn new(
        file_format: FileFormat,
        file_location: Vec<String>,
        s3_region: String,
        s3_access_key: String,
        s3_secret_key: String,
        batch_size: usize,
        schema: Schema,
        identity: String,
    ) -> Self {
        Self {
            file_format,
            file_location,
            s3_region,
            s3_access_key,
            s3_secret_key,
            batch_size,
            schema,
            identity,
        }
    }

    #[try_stream(ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        assert_eq!(self.file_format, FileFormat::Parquet);
        for file in self.file_location {
            let mut batch_stream_builder = create_parquet_stream_builder(
                self.s3_region.clone(),
                self.s3_access_key.clone(),
                self.s3_secret_key.clone(),
                file,
            )
            .await?;

            let arrow_schema = batch_stream_builder.schema();
            assert_eq!(arrow_schema.fields.len(), self.schema.fields.len());
            for (field, arrow_field) in self.schema.fields.iter().zip(arrow_schema.fields.iter()) {
                assert_eq!(*field.name, *arrow_field.name());
            }

            batch_stream_builder = batch_stream_builder.with_projection(ProjectionMask::all());

            batch_stream_builder = batch_stream_builder.with_batch_size(self.batch_size);

            let record_batch_stream = batch_stream_builder
                .build()
                .map_err(|e| anyhow!(e).context("fail to build arrow stream builder"))?;

            #[for_await]
            for record_batch in record_batch_stream {
                let record_batch = record_batch?;
                let chunk = IcebergArrowConvert.chunk_from_record_batch(&record_batch)?;
                debug_assert_eq!(chunk.data_types(), self.schema.data_types());
                yield chunk;
            }
        }
    }
}

pub struct FileScanExecutorBuilder {}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for FileScanExecutorBuilder {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<'_, C>,
        _inputs: Vec<BoxedExecutor>,
    ) -> crate::error::Result<BoxedExecutor> {
        let file_scan_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::FileScan
        )?;

        assert_eq!(file_scan_node.storage_type, StorageType::S3 as i32);

        Ok(Box::new(S3FileScanExecutor::new(
            match file_scan_node::FileFormat::try_from(file_scan_node.file_format).unwrap() {
                file_scan_node::FileFormat::Parquet => FileFormat::Parquet,
                file_scan_node::FileFormat::Unspecified => unreachable!(),
            },
            file_scan_node.file_location.clone(),
            file_scan_node.s3_region.clone(),
            file_scan_node.s3_access_key.clone(),
            file_scan_node.s3_secret_key.clone(),
            source.context.get_config().developer.chunk_size,
            Schema::from_iter(file_scan_node.columns.iter().map(Field::from)),
            source.plan_node().get_identity().clone(),
        )))
    }
}

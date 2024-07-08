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

use std::ops::Range;
use std::sync::Arc;

use anyhow::anyhow;
use bytes::Bytes;
use futures_async_stream::try_stream;
use futures_util::future::BoxFuture;
use futures_util::stream::StreamExt;
use futures_util::TryFutureExt;
use hashbrown::HashMap;
use iceberg::io::{
    FileIOBuilder, FileMetadata, FileRead, S3_ACCESS_KEY_ID, S3_REGION, S3_SECRET_ACCESS_KEY,
};
use parquet::arrow::async_reader::{AsyncFileReader, MetadataLoader};
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::file::metadata::ParquetMetaData;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::catalog::Schema;

use crate::error::BatchError;
use crate::executor::{DataChunk, Executor};

#[derive(PartialEq, Debug)]
pub enum FileFormat {
    Parquet,
}

/// S3 file scan executor. Currently only support parquet file format.
pub struct S3FileScanExecutor {
    file_format: FileFormat,
    location: String,
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
    #![expect(dead_code)]
    pub fn new(
        file_format: FileFormat,
        location: String,
        s3_region: String,
        s3_access_key: String,
        s3_secret_key: String,
        batch_size: usize,
        schema: Schema,
        identity: String,
    ) -> Self {
        Self {
            file_format,
            location,
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

        let mut props = HashMap::new();
        props.insert(S3_REGION, self.s3_region.clone());
        props.insert(S3_ACCESS_KEY_ID, self.s3_access_key.clone());
        props.insert(S3_SECRET_ACCESS_KEY, self.s3_secret_key.clone());

        let file_io_builder = FileIOBuilder::new("s3");
        let file_io = file_io_builder.with_props(props.into_iter()).build()?;
        let parquet_file = file_io.new_input(&self.location)?;

        let parquet_metadata = parquet_file.metadata().await?;
        let parquet_reader = parquet_file.reader().await?;
        let arrow_file_reader = ArrowFileReader::new(parquet_metadata, parquet_reader);

        let mut batch_stream_builder = ParquetRecordBatchStreamBuilder::new(arrow_file_reader)
            .await
            .map_err(|e| anyhow!(e))?;

        let arrow_schema = batch_stream_builder.schema();
        assert_eq!(arrow_schema.fields.len(), self.schema.fields.len());
        for (field, arrow_field) in self.schema.fields.iter().zip(arrow_schema.fields.iter()) {
            assert_eq!(*field.name, *arrow_field.name());
        }

        batch_stream_builder = batch_stream_builder.with_projection(ProjectionMask::all());

        batch_stream_builder = batch_stream_builder.with_batch_size(self.batch_size);

        let record_batch_stream = batch_stream_builder.build().map_err(|e| anyhow!(e))?;

        #[for_await]
        for record_batch in record_batch_stream {
            let record_batch = record_batch.map_err(BatchError::Parquet)?;
            let chunk = IcebergArrowConvert.chunk_from_record_batch(&record_batch)?;
            debug_assert_eq!(chunk.data_types(), self.schema.data_types());
            yield chunk;
        }
    }
}

struct ArrowFileReader<R: FileRead> {
    meta: FileMetadata,
    r: R,
}

impl<R: FileRead> ArrowFileReader<R> {
    fn new(meta: FileMetadata, r: R) -> Self {
        Self { meta, r }
    }
}

impl<R: FileRead> AsyncFileReader for ArrowFileReader<R> {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        Box::pin(
            self.r
                .read(range.start as _..range.end as _)
                .map_err(|err| parquet::errors::ParquetError::External(Box::new(err))),
        )
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        Box::pin(async move {
            let file_size = self.meta.size;
            let mut loader = MetadataLoader::load(self, file_size as usize, None).await?;
            loader.load_page_index(false, false).await?;
            Ok(Arc::new(loader.finish()))
        })
    }
}

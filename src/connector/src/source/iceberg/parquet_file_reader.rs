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

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use anyhow::anyhow;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::TryFutureExt;
use iceberg::io::{
    FileIOBuilder, FileMetadata, FileRead, S3_ACCESS_KEY_ID, S3_REGION, S3_SECRET_ACCESS_KEY,
};
use iceberg::{Error, ErrorKind};
use opendal::layers::RetryLayer;
use opendal::services::S3;
use opendal::Operator;
use parquet::arrow::async_reader::{AsyncFileReader, MetadataLoader};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::file::metadata::ParquetMetaData;
use url::Url;

pub struct ParquetFileReader<R: FileRead> {
    meta: FileMetadata,
    r: R,
}

impl<R: FileRead> ParquetFileReader<R> {
    pub fn new(meta: FileMetadata, r: R) -> Self {
        Self { meta, r }
    }
}

impl<R: FileRead> AsyncFileReader for ParquetFileReader<R> {
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

pub async fn create_parquet_stream_builder(
    s3_region: String,
    s3_access_key: String,
    s3_secret_key: String,
    location: String,
) -> Result<ParquetRecordBatchStreamBuilder<ParquetFileReader<impl FileRead>>, anyhow::Error> {
    let mut props = HashMap::new();
    props.insert(S3_REGION, s3_region.clone());
    props.insert(S3_ACCESS_KEY_ID, s3_access_key.clone());
    props.insert(S3_SECRET_ACCESS_KEY, s3_secret_key.clone());

    let file_io_builder = FileIOBuilder::new("s3");
    let file_io = file_io_builder
        .with_props(props.into_iter())
        .build()
        .map_err(|e| anyhow!(e))?;
    let parquet_file = file_io.new_input(&location).map_err(|e| anyhow!(e))?;

    let parquet_metadata = parquet_file.metadata().await.map_err(|e| anyhow!(e))?;
    let parquet_reader = parquet_file.reader().await.map_err(|e| anyhow!(e))?;
    let parquet_file_reader = ParquetFileReader::new(parquet_metadata, parquet_reader);

    ParquetRecordBatchStreamBuilder::new(parquet_file_reader)
        .await
        .map_err(|e| anyhow!(e))
}

pub async fn list_s3_directory(
    s3_region: String,
    s3_access_key: String,
    s3_secret_key: String,
    dir: String,
) -> Result<Vec<String>, anyhow::Error> {
    let url = Url::parse(&dir)?;
    let bucket = url.host_str().ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid s3 url: {}, missing bucket", dir),
        )
    })?;

    let prefix = format!("s3://{}/", bucket);
    if dir.starts_with(&prefix) {
        let mut builder = S3::default();
        builder = builder
            .region(&s3_region)
            .access_key_id(&s3_access_key)
            .secret_access_key(&s3_secret_key)
            .bucket(bucket);
        let op = Operator::new(builder)?
            .layer(RetryLayer::default())
            .finish();

        op.list(&dir[prefix.len()..])
            .await
            .map_err(|e| anyhow!(e))
            .map(|list| {
                list.into_iter()
                    .map(|entry| prefix.to_string() + entry.path())
                    .collect()
            })
    } else {
        Err(Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid s3 url: {}, should start with {}", dir, prefix),
        ))?
    }
}

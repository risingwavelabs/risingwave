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
use std::time::Duration;

use bytes::Bytes;
use fail::fail_point;
use futures::{stream, StreamExt, TryStreamExt};
use opendal::layers::{RetryLayer, TimeoutLayer};
use opendal::services::Memory;
use opendal::{Metakey, Operator, Writer};
use risingwave_common::config::ObjectStoreConfig;
use risingwave_common::range::RangeBoundsExt;
use thiserror_ext::AsReport;

use crate::object::{
    prefix, ObjectDataStream, ObjectError, ObjectMetadata, ObjectMetadataIter, ObjectRangeBounds,
    ObjectResult, ObjectStore, StreamingUploader,
};

/// Opendal object storage.
#[derive(Clone)]
pub struct OpendalObjectStore {
    pub(crate) op: Operator,
    pub(crate) engine_type: EngineType,

    pub(crate) config: Arc<ObjectStoreConfig>,
}

#[derive(Clone)]
pub enum EngineType {
    Memory,
    Hdfs,
    Gcs,
    Minio,
    S3,
    Obs,
    Oss,
    Webhdfs,
    Azblob,
    Fs,
}

impl OpendalObjectStore {
    /// create opendal memory engine, used for unit tests.
    pub fn test_new_memory_engine() -> ObjectResult<Self> {
        // Create memory backend builder.
        let builder = Memory::default();
        let op: Operator = Operator::new(builder)?.finish();
        Ok(Self {
            op,
            engine_type: EngineType::Memory,
            config: Arc::new(ObjectStoreConfig::default()),
        })
    }
}

#[async_trait::async_trait]
impl ObjectStore for OpendalObjectStore {
    type StreamingUploader = OpendalStreamingUploader;

    fn get_object_prefix(&self, obj_id: u64) -> String {
        match self.engine_type {
            EngineType::S3 => prefix::s3::get_object_prefix(obj_id),
            EngineType::Minio => prefix::s3::get_object_prefix(obj_id),
            EngineType::Memory => String::default(),
            EngineType::Hdfs => String::default(),
            EngineType::Gcs => String::default(),
            EngineType::Obs => String::default(),
            EngineType::Oss => String::default(),
            EngineType::Webhdfs => String::default(),
            EngineType::Azblob => String::default(),
            EngineType::Fs => String::default(),
        }
    }

    async fn upload(&self, path: &str, obj: Bytes) -> ObjectResult<()> {
        if obj.is_empty() {
            Err(ObjectError::internal("upload empty object"))
        } else {
            self.op.write(path, obj).await?;
            Ok(())
        }
    }

    async fn streaming_upload(&self, path: &str) -> ObjectResult<Self::StreamingUploader> {
        Ok(
            OpendalStreamingUploader::new(self.op.clone(), path.to_string(), self.config.clone())
                .await?,
        )
    }

    async fn read(&self, path: &str, range: impl ObjectRangeBounds) -> ObjectResult<Bytes> {
        let data = if range.is_full() {
            self.op.read(path).await?
        } else {
            self.op
                .read_with(path)
                .range(range.map(|v| *v as u64))
                .await?
        };

        if let Some(len) = range.len()
            && len != data.len()
        {
            return Err(ObjectError::internal(format!(
                "mismatched size: expected {}, found {} when reading {} at {:?}",
                len,
                data.len(),
                path,
                range,
            )));
        }

        Ok(Bytes::from(data))
    }

    /// Returns a stream reading the object specified in `path`. If given, the stream starts at the
    /// byte with index `start_pos` (0-based). As far as possible, the stream only loads the amount
    /// of data into memory that is read from the stream.
    async fn streaming_read(
        &self,
        path: &str,
        range: Range<usize>,
    ) -> ObjectResult<ObjectDataStream> {
        fail_point!("opendal_streaming_read_err", |_| Err(
            ObjectError::internal("opendal streaming read error")
        ));
        let range: Range<u64> = (range.start as u64)..(range.end as u64);
        let reader = self
            .op
            .clone()
            .layer(
                RetryLayer::new()
                    .with_min_delay(Duration::from_millis(
                        self.config.retry.req_backoff_interval_ms,
                    ))
                    .with_max_delay(Duration::from_millis(
                        self.config.retry.req_backoff_max_delay_ms,
                    ))
                    .with_max_times(self.config.retry.streaming_read_retry_attempts)
                    .with_factor(self.config.retry.req_backoff_factor as f32)
                    .with_jitter(),
            )
            .layer(TimeoutLayer::new().with_io_timeout(Duration::from_millis(
                self.config.retry.streaming_read_attempt_timeout_ms,
            )))
            .reader_with(path)
            .range(range)
            .await?;
        let stream = reader.into_stream().map(|item| {
            item.map_err(|e| {
                ObjectError::internal(format!("reader into_stream fail {}", e.as_report()))
            })
        });

        Ok(Box::pin(stream))
    }

    async fn metadata(&self, path: &str) -> ObjectResult<ObjectMetadata> {
        let opendal_metadata = self.op.stat(path).await?;
        let key = path.to_string();
        let last_modified = match opendal_metadata.last_modified() {
            Some(t) => t.timestamp() as f64,
            None => 0_f64,
        };

        let total_size = opendal_metadata.content_length() as usize;
        let metadata = ObjectMetadata {
            key,
            last_modified,
            total_size,
        };
        Ok(metadata)
    }

    async fn delete(&self, path: &str) -> ObjectResult<()> {
        self.op.delete(path).await?;
        Ok(())
    }

    /// Deletes the objects with the given paths permanently from the storage. If an object
    /// specified in the request is not found, it will be considered as successfully deleted.
    async fn delete_objects(&self, paths: &[String]) -> ObjectResult<()> {
        self.op.remove(paths.to_vec()).await?;
        Ok(())
    }

    async fn list(&self, prefix: &str) -> ObjectResult<ObjectMetadataIter> {
        let object_lister = self
            .op
            .lister_with(prefix)
            .recursive(true)
            .metakey(Metakey::ContentLength)
            .await?;

        let stream = stream::unfold(object_lister, |mut object_lister| async move {
            match object_lister.next().await {
                Some(Ok(object)) => {
                    let key = object.path().to_string();
                    let om = object.metadata();
                    let last_modified = match om.last_modified() {
                        Some(t) => t.timestamp() as f64,
                        None => 0_f64,
                    };
                    let total_size = om.content_length() as usize;
                    let metadata = ObjectMetadata {
                        key,
                        last_modified,
                        total_size,
                    };
                    Some((Ok(metadata), object_lister))
                }
                Some(Err(err)) => Some((Err(err.into()), object_lister)),
                None => None,
            }
        });

        Ok(stream.boxed())
    }

    fn store_media_type(&self) -> &'static str {
        match self.engine_type {
            EngineType::Memory => "Memory",
            EngineType::Hdfs => "Hdfs",
            EngineType::Minio => "Minio",
            EngineType::S3 => "S3",
            EngineType::Gcs => "Gcs",
            EngineType::Obs => "Obs",
            EngineType::Oss => "Oss",
            EngineType::Webhdfs => "Webhdfs",
            EngineType::Azblob => "Azblob",
            EngineType::Fs => "Fs",
        }
    }

    fn support_streaming_upload(&self) -> bool {
        self.op.info().native_capability().write_can_multi
    }
}

/// Store multiple parts in a map, and concatenate them on finish.
pub struct OpendalStreamingUploader {
    writer: Writer,
}

impl OpendalStreamingUploader {
    pub async fn new(
        op: Operator,
        path: String,
        config: Arc<ObjectStoreConfig>,
    ) -> ObjectResult<Self> {
        let writer = op
            .clone()
            .layer(
                RetryLayer::new()
                    .with_min_delay(Duration::from_millis(config.retry.req_backoff_interval_ms))
                    .with_max_delay(Duration::from_millis(config.retry.req_backoff_max_delay_ms))
                    .with_max_times(config.retry.streaming_upload_retry_attempts)
                    .with_factor(config.retry.req_backoff_factor as f32)
                    .with_jitter(),
            )
            .layer(TimeoutLayer::new().with_io_timeout(Duration::from_millis(
                config.retry.streaming_upload_attempt_timeout_ms,
            )))
            .writer_with(&path)
            .concurrent(8)
            .buffer(OPENDAL_BUFFER_SIZE)
            .await?;
        Ok(Self { writer })
    }
}

const OPENDAL_BUFFER_SIZE: usize = 16 * 1024 * 1024;

impl StreamingUploader for OpendalStreamingUploader {
    async fn write_bytes(&mut self, data: Bytes) -> ObjectResult<()> {
        self.writer.write(data).await?;

        Ok(())
    }

    async fn finish(mut self) -> ObjectResult<()> {
        match self.writer.close().await {
            Ok(_) => (),
            Err(err) => {
                self.writer.abort().await?;
                return Err(err.into());
            }
        };

        Ok(())
    }

    fn get_memory_usage(&self) -> u64 {
        OPENDAL_BUFFER_SIZE as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn list_all(prefix: &str, store: &OpendalObjectStore) -> Vec<ObjectMetadata> {
        store
            .list(prefix)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_memory_upload() {
        let block = Bytes::from("123456");
        let store = OpendalObjectStore::test_new_memory_engine().unwrap();
        store.upload("/abc", block).await.unwrap();

        // No such object.
        store.read("/ab", 0..3).await.unwrap_err();

        let bytes = store.read("/abc", 4..6).await.unwrap();
        assert_eq!(String::from_utf8(bytes.to_vec()).unwrap(), "56".to_string());

        // Overflow.
        store.read("/abc", 4..44).await.unwrap_err();

        store.delete("/abc").await.unwrap();

        // No such object.
        store.read("/abc", 0..3).await.unwrap_err();
    }

    #[tokio::test]
    async fn test_memory_metadata() {
        let block = Bytes::from("123456");
        let path = "/abc".to_string();
        let obj_store = OpendalObjectStore::test_new_memory_engine().unwrap();
        obj_store.upload("/abc", block).await.unwrap();

        let err = obj_store.metadata("/not_exist").await.unwrap_err();
        assert!(err.is_object_not_found_error());
        let metadata = obj_store.metadata("/abc").await.unwrap();
        assert_eq!(metadata.total_size, 6);
        obj_store.delete(&path).await.unwrap();
    }

    #[tokio::test]
    async fn test_memory_delete_objects_and_list_object() {
        let block1 = Bytes::from("123456");
        let block2 = Bytes::from("987654");
        let store = OpendalObjectStore::test_new_memory_engine().unwrap();
        store.upload("abc", Bytes::from("123456")).await.unwrap();
        store.upload("prefix/abc", block1).await.unwrap();

        store.upload("prefix/xyz", block2).await.unwrap();

        assert_eq!(list_all("", &store).await.len(), 3);
        assert_eq!(list_all("prefix/", &store).await.len(), 2);
        let str_list = [String::from("prefix/abc"), String::from("prefix/xyz")];

        store.delete_objects(&str_list).await.unwrap();

        assert!(store.read("prefix/abc/", ..).await.is_err());
        assert!(store.read("prefix/xyz/", ..).await.is_err());
        assert_eq!(list_all("", &store).await.len(), 1);
        assert_eq!(list_all("prefix/", &store).await.len(), 0);
    }
}

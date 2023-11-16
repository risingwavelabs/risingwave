// Copyright 2023 RisingWave Labs
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

use bytes::Bytes;
use fail::fail_point;
use futures::{stream, StreamExt, TryStreamExt};
use opendal::services::Memory;
use opendal::{Metakey, Operator, Writer};
use risingwave_common::range::RangeBoundsExt;

use crate::object::{
    BoxedStreamingUploader, ObjectDataStream, ObjectError, ObjectMetadata, ObjectMetadataIter,
    ObjectRangeBounds, ObjectResult, ObjectStore, StreamingUploader,
};

/// Opendal object storage.
#[derive(Clone)]
pub struct OpendalObjectStore {
    pub(crate) op: Operator,
    pub(crate) engine_type: EngineType,
}
#[derive(Clone)]
pub enum EngineType {
    Memory,
    Hdfs,
    Gcs,
    Oss,
    Webhdfs,
    Azblob,
    Fs,
}

impl OpendalObjectStore {
    /// create opendal memory engine, used for unit tests.
    pub fn new_memory_engine() -> ObjectResult<Self> {
        // Create memory backend builder.
        let builder = Memory::default();
        let op: Operator = Operator::new(builder)?.finish();
        Ok(Self {
            op,
            engine_type: EngineType::Memory,
        })
    }
}

#[async_trait::async_trait]
impl ObjectStore for OpendalObjectStore {
    fn get_object_prefix(&self, _obj_id: u64) -> String {
        String::default()
    }

    async fn upload(&self, path: &str, obj: Bytes) -> ObjectResult<()> {
        if obj.is_empty() {
            Err(ObjectError::internal("upload empty object"))
        } else {
            self.op.write(path, obj).await?;
            Ok(())
        }
    }

    async fn streaming_upload(&self, path: &str) -> ObjectResult<BoxedStreamingUploader> {
        Ok(Box::new(
            OpenDalStreamingUploader::new(self.op.clone(), path.to_string()).await?,
        ))
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
        let reader = self.op.reader_with(path).range(range).await?;
        let stream = reader
            .into_stream()
            .map(|item| item.map_err(|e| ObjectError::internal(format!("OpenDalError: {:?}", e))));

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
            .delimiter("")
            .metakey(Metakey::ContentLength | Metakey::ContentType)
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
            EngineType::Gcs => "Gcs",
            EngineType::Oss => "Oss",
            EngineType::Webhdfs => "Webhdfs",
            EngineType::Azblob => "Azblob",
            EngineType::Fs => "Fs",
        }
    }
}

/// Store multiple parts in a map, and concatenate them on finish.
pub struct OpenDalStreamingUploader {
    writer: Writer,
}
impl OpenDalStreamingUploader {
    pub async fn new(op: Operator, path: String) -> ObjectResult<Self> {
        let writer = op.writer_with(&path).buffer(OPENDAL_BUFFER_SIZE).await?;
        Ok(Self { writer })
    }
}

const OPENDAL_BUFFER_SIZE: usize = 16 * 1024 * 1024;

#[async_trait::async_trait]
impl StreamingUploader for OpenDalStreamingUploader {
    async fn write_bytes(&mut self, data: Bytes) -> ObjectResult<()> {
        self.writer.write(data).await?;
        Ok(())
    }

    async fn finish(mut self: Box<Self>) -> ObjectResult<()> {
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
    use bytes::Bytes;

    use super::*;

    async fn list_all(prefix: &str, store: &OpendalObjectStore) -> Vec<ObjectMetadata> {
        let mut iter = store.list(prefix).await.unwrap();
        let mut result = vec![];
        while let Some(r) = iter.next().await {
            result.push(r.unwrap());
        }
        result
    }

    #[tokio::test]
    async fn test_memory_upload() {
        let block = Bytes::from("123456");
        let store = OpendalObjectStore::new_memory_engine().unwrap();
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
        let obj_store = OpendalObjectStore::new_memory_engine().unwrap();
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
        let store = OpendalObjectStore::new_memory_engine().unwrap();
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

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

use bytes::Bytes;
use fail::fail_point;
use futures::future::try_join_all;
use futures::StreamExt;
use itertools::Itertools;
use opendal::services::Memory;
use opendal::{Metakey, Operator, Writer};
use tokio::io::AsyncRead;

use crate::object::{
    BlockLocation, BoxedStreamingUploader, ObjectError, ObjectMetadata, ObjectResult, ObjectStore,
    StreamingUploader,
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

    async fn read(&self, path: &str, block: Option<BlockLocation>) -> ObjectResult<Bytes> {
        match block {
            Some(block) => {
                let range = block.offset as u64..(block.offset + block.size) as u64;
                let res = Bytes::from(self.op.range_read(path, range).await?);

                if block.size != res.len() {
                    Err(ObjectError::internal("bad block offset and size"))
                } else {
                    Ok(res)
                }
            }
            None => Ok(Bytes::from(self.op.read(path).await?)),
        }
    }

    async fn readv(&self, path: &str, block_locs: &[BlockLocation]) -> ObjectResult<Vec<Bytes>> {
        let futures = block_locs
            .iter()
            .map(|block_loc| self.read(path, Some(*block_loc)))
            .collect_vec();
        try_join_all(futures).await
    }

    /// Returns a stream reading the object specified in `path`. If given, the stream starts at the
    /// byte with index `start_pos` (0-based). As far as possible, the stream only loads the amount
    /// of data into memory that is read from the stream.
    async fn streaming_read(
        &self,
        path: &str,
        start_pos: Option<usize>,
    ) -> ObjectResult<Box<dyn AsyncRead + Unpin + Send + Sync>> {
        fail_point!("opendal_streaming_read_err", |_| Err(
            ObjectError::internal("opendal streaming read error")
        ));
        let reader = match start_pos {
            Some(start_position) => self.op.range_reader(path, start_position as u64..).await?,
            None => self.op.reader(path).await?,
        };

        Ok(Box::new(reader))
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

    async fn list(&self, prefix: &str) -> ObjectResult<Vec<ObjectMetadata>> {
        let mut object_lister = self.op.scan(prefix).await?;
        let mut metadata_list = vec![];
        while let Some(obj) = object_lister.next().await {
            let object = obj?;

            let key = object.path().to_string();

            let om = self
                .op
                .metadata(&object, Metakey::LastModified | Metakey::ContentLength)
                .await?;

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
            metadata_list.push(metadata);
        }
        Ok(metadata_list)
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
        let writer = op.writer(&path).await?;
        Ok(Self { writer })
    }
}

const OPENDAL_BUFFER_SIZE: u64 = 8 * 1024 * 1024;

#[async_trait::async_trait]
impl StreamingUploader for OpenDalStreamingUploader {
    async fn write_bytes(&mut self, data: Bytes) -> ObjectResult<()> {
        self.writer.write(data).await?;
        Ok(())
    }

    async fn finish(mut self: Box<Self>) -> ObjectResult<()> {
        match self.writer.close().await {
            Ok(_) => (),
            Err(_) => self.writer.abort().await?,
        };

        Ok(())
    }

    fn get_memory_usage(&self) -> u64 {
        OPENDAL_BUFFER_SIZE
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn gen_test_payload() -> Vec<u8> {
        let mut ret = Vec::new();
        for i in 0..100000 {
            ret.extend(format!("{:05}", i).as_bytes());
        }
        ret
    }
    #[tokio::test]
    async fn test_memory_upload() {
        let block = Bytes::from("123456");
        let store = OpendalObjectStore::new_memory_engine().unwrap();
        store.upload("/abc", block).await.unwrap();

        // No such object.
        store
            .read("/ab", Some(BlockLocation { offset: 0, size: 3 }))
            .await
            .unwrap_err();

        let bytes = store
            .read("/abc", Some(BlockLocation { offset: 4, size: 2 }))
            .await
            .unwrap();
        assert_eq!(String::from_utf8(bytes.to_vec()).unwrap(), "56".to_string());

        // Overflow.
        store
            .read(
                "/abc",
                Some(BlockLocation {
                    offset: 4,
                    size: 40,
                }),
            )
            .await
            .unwrap_err();

        store.delete("/abc").await.unwrap();

        // No such object.
        store
            .read("/abc", Some(BlockLocation { offset: 0, size: 3 }))
            .await
            .unwrap_err();
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

        assert_eq!(store.list("").await.unwrap().len(), 3);
        assert_eq!(store.list("prefix/").await.unwrap().len(), 2);
        let str_list = [String::from("prefix/abc"), String::from("prefix/xyz")];

        store.delete_objects(&str_list).await.unwrap();

        assert!(store.read("prefix/abc/", None).await.is_err());
        assert!(store.read("prefix/xyz/", None).await.is_err());
        assert_eq!(store.list("").await.unwrap().len(), 1);
        assert_eq!(store.list("prefix/").await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_memory_read_multi_block() {
        let store = OpendalObjectStore::new_memory_engine().unwrap();
        let payload = gen_test_payload();
        store
            .upload("test.obj", Bytes::from(payload.clone()))
            .await
            .unwrap();
        let metadata = store.metadata("test.obj").await.unwrap();
        assert_eq!(payload.len(), metadata.total_size);
        let test_loc = vec![(0, 1000), (10000, 1000), (20000, 1000)];
        let read_data = store
            .readv(
                "test.obj",
                &test_loc
                    .iter()
                    .map(|(offset, size)| BlockLocation {
                        offset: *offset,
                        size: *size,
                    })
                    .collect_vec(),
            )
            .await
            .unwrap();
        assert_eq!(test_loc.len(), read_data.len());
        for (i, (offset, size)) in test_loc.iter().enumerate() {
            assert_eq!(&payload[*offset..(*offset + *size)], &read_data[i][..]);
        }
        store.delete("test.obj").await.unwrap();
    }
}

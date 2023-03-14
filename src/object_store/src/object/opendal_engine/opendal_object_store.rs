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

use bytes::{BufMut, Bytes, BytesMut};
use fail::fail_point;
use futures::future::try_join_all;
use futures::StreamExt;
use itertools::Itertools;
use opendal::services::Memory;
use opendal::{Metakey, Operator};
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

    fn streaming_upload(&self, path: &str) -> ObjectResult<BoxedStreamingUploader> {
        Ok(Box::new(OpenDalStreamingUploader::new(
            self.op.clone(),
            path.to_string(),
        )))
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
            Some(t) => t.unix_timestamp() as f64,
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
        let mut object_lister = self.op.list(prefix).await?;
        let mut metadata_list = vec![];
        while let Some(obj) = object_lister.next().await {
            let object = obj?;
            let key = prefix.to_string();

            let om = self
                .op
                .metadata(&object, Metakey::LastModified | Metakey::ContentLength)
                .await?;

            let last_modified = match om.last_modified() {
                Some(t) => t.unix_timestamp() as f64,
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
        }
    }
}

/// Store multiple parts in a map, and concatenate them on finish.
pub struct OpenDalStreamingUploader {
    op: Operator,
    path: String,
    buffer: BytesMut,
}
impl OpenDalStreamingUploader {
    pub fn new(op: Operator, path: String) -> Self {
        Self {
            op,
            path,
            buffer: BytesMut::new(),
        }
    }
}
#[async_trait::async_trait]
impl StreamingUploader for OpenDalStreamingUploader {
    async fn write_bytes(&mut self, data: Bytes) -> ObjectResult<()> {
        self.buffer.put(data);
        Ok(())
    }

    async fn finish(mut self: Box<Self>) -> ObjectResult<()> {
        self.op.write(&self.path, self.buffer).await?;

        Ok(())
    }

    fn get_memory_usage(&self) -> u64 {
        self.buffer.capacity() as u64
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

        let metadata = obj_store.metadata("/abc").await.unwrap();
        assert_eq!(metadata.total_size, 6);
        obj_store.delete(&path).await.unwrap();
    }

    #[tokio::test]
    async fn test_memory_delete_objects() {
        let block1 = Bytes::from("123456");
        let block2 = Bytes::from("987654");
        let store = OpendalObjectStore::new_memory_engine().unwrap();
        store.upload("abc", block1).await.unwrap();
        store.upload("/klm", block2).await.unwrap();

        assert_eq!(store.list("").await.unwrap().len(), 2);

        let str_list = [
            String::from("abc"),
            String::from("klm"),
            String::from("xyz"),
        ];

        store.delete_objects(&str_list).await.unwrap();

        assert_eq!(store.list("").await.unwrap().len(), 0);
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

    #[tokio::test]
    async fn test_memory_streaming_upload() {
        let blocks = vec![Bytes::from("123"), Bytes::from("456"), Bytes::from("789")];
        let obj = Bytes::from("123456789");

        let store = OpendalObjectStore::new_memory_engine().unwrap();
        let mut uploader = store.streaming_upload("/temp").unwrap();

        for block in blocks {
            uploader.write_bytes(block).await.unwrap();
        }
        uploader.finish().await.unwrap();

        // Read whole object.
        let read_obj = store.read("/temp", None).await.unwrap();
        assert!(read_obj.eq(&obj));

        // Read part of the object.
        let read_obj = store
            .read("/temp", Some(BlockLocation { offset: 4, size: 2 }))
            .await
            .unwrap();
        assert_eq!(
            String::from_utf8(read_obj.to_vec()).unwrap(),
            "56".to_string()
        );
        store.delete("/temp").await.unwrap();
    }
}

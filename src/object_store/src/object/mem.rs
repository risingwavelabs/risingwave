// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{BufMut, Bytes, BytesMut};
use fail::fail_point;
use futures::future::try_join_all;
use itertools::Itertools;
use tokio::io::AsyncRead;
use tokio::sync::Mutex;

use super::{
    BlockLocation, BoxedStreamingUploader, ObjectError, ObjectMetadata, ObjectResult, ObjectStore,
    StreamingUploader,
};

/// Store multiple parts in a map, and concatenate them on finish.
pub struct InMemStreamingUploader {
    path: String,
    buf: BytesMut,
    objects: Arc<Mutex<HashMap<String, (ObjectMetadata, Bytes)>>>,
}

#[async_trait::async_trait]
impl StreamingUploader for InMemStreamingUploader {
    async fn write_bytes(&mut self, data: Bytes) -> ObjectResult<()> {
        fail_point!("mem_write_bytes_err", |_| Err(ObjectError::internal(
            "mem write bytes error"
        )));
        self.buf.put(data);
        Ok(())
    }

    async fn finish(self: Box<Self>) -> ObjectResult<()> {
        fail_point!("mem_finish_streaming_upload_err", |_| Err(
            ObjectError::internal("mem finish streaming upload error")
        ));
        let obj = self.buf.freeze();
        if obj.is_empty() {
            Err(ObjectError::internal("upload empty object"))
        } else {
            let metadata = get_obj_meta(&self.path, &obj)?;
            self.objects.lock().await.insert(self.path, (metadata, obj));
            Ok(())
        }
    }

    fn get_memory_usage(&self) -> u64 {
        self.buf.capacity() as u64
    }
}

/// In-memory object storage, useful for testing.
#[derive(Default, Clone)]
pub struct InMemObjectStore {
    objects: Arc<Mutex<HashMap<String, (ObjectMetadata, Bytes)>>>,
}

#[async_trait::async_trait]
impl ObjectStore for InMemObjectStore {
    fn get_object_prefix(&self, _obj_id: u64) -> String {
        String::default()
    }

    async fn upload(&self, path: &str, obj: Bytes) -> ObjectResult<()> {
        fail_point!("mem_upload_err", |_| Err(ObjectError::internal(
            "mem upload error"
        )));
        if obj.is_empty() {
            Err(ObjectError::internal("upload empty object"))
        } else {
            let metadata = get_obj_meta(path, &obj)?;
            self.objects
                .lock()
                .await
                .insert(path.into(), (metadata, obj));
            Ok(())
        }
    }

    fn streaming_upload(&self, path: &str) -> ObjectResult<BoxedStreamingUploader> {
        Ok(Box::new(InMemStreamingUploader {
            path: path.to_string(),
            buf: BytesMut::new(),
            objects: self.objects.clone(),
        }))
    }

    async fn read(&self, path: &str, block: Option<BlockLocation>) -> ObjectResult<Bytes> {
        fail_point!("mem_read_err", |_| Err(ObjectError::internal(
            "mem read error"
        )));
        if let Some(loc) = block {
            self.get_object(path, |obj| find_block(obj, loc)).await?
        } else {
            self.get_object(path, |obj| Ok(obj.clone())).await?
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
        fail_point!("mem_streaming_read_err", |_| Err(ObjectError::internal(
            "mem streaming read error"
        )));

        let bytes = if let Some(pos) = start_pos {
            self.get_object(path, |obj| {
                find_block(
                    obj,
                    BlockLocation {
                        offset: pos,
                        size: obj.len() - pos,
                    },
                )
            })
            .await?
        } else {
            self.get_object(path, |obj| Ok(obj.clone())).await?
        };

        Ok(Box::new(Cursor::new(bytes?)))
    }

    async fn metadata(&self, path: &str) -> ObjectResult<ObjectMetadata> {
        self.objects
            .lock()
            .await
            .get(path)
            .map(|(metadata, _)| metadata)
            .cloned()
            .ok_or_else(|| ObjectError::internal(format!("no object at path '{}'", path)))
    }

    async fn delete(&self, path: &str) -> ObjectResult<()> {
        fail_point!("mem_delete_err", |_| Err(ObjectError::internal(
            "mem delete error"
        )));
        self.objects.lock().await.remove(path);
        Ok(())
    }

    /// Deletes the objects with the given paths permanently from the storage. If an object
    /// specified in the request is not found, it will be considered as successfully deleted.
    async fn delete_objects(&self, paths: &[String]) -> ObjectResult<()> {
        let mut guard = self.objects.lock().await;

        for path in paths {
            guard.remove(path);
        }

        Ok(())
    }

    async fn list(&self, prefix: &str) -> ObjectResult<Vec<ObjectMetadata>> {
        Ok(self
            .objects
            .lock()
            .await
            .iter()
            .filter_map(|(path, (metadata, _))| {
                if path.starts_with(prefix) {
                    return Some(metadata.clone());
                }
                None
            })
            .sorted_by(|a, b| Ord::cmp(&a.key, &b.key))
            .collect_vec())
    }

    fn store_media_type(&self) -> &'static str {
        "mem"
    }
}

lazy_static::lazy_static! {
    static ref SHARED: spin::Mutex<InMemObjectStore> = spin::Mutex::new(InMemObjectStore::new());
}

impl InMemObjectStore {
    pub fn new() -> Self {
        Self {
            objects: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Create a shared reference to the in-memory object store in this process.
    ///
    /// Note: Should only be used for `risedev playground`, when there're multiple compute-nodes or
    /// compactors in the same process.
    pub(super) fn shared() -> Self {
        SHARED.lock().clone()
    }

    /// Reset the shared in-memory object store.
    pub fn reset_shared() {
        *SHARED.lock() = InMemObjectStore::new();
    }

    async fn get_object<R, F>(&self, path: &str, f: F) -> ObjectResult<R>
    where
        F: Fn(&Bytes) -> R,
    {
        self.objects
            .lock()
            .await
            .get(path)
            .map(|(_, obj)| obj)
            .ok_or_else(|| ObjectError::internal(format!("no object at path '{}'", path)))
            .map(f)
    }
}

fn find_block(obj: &Bytes, block: BlockLocation) -> ObjectResult<Bytes> {
    if block.offset + block.size > obj.len() {
        Err(ObjectError::internal("bad block offset and size"))
    } else {
        Ok(obj.slice(block.offset..(block.offset + block.size)))
    }
}

fn get_obj_meta(path: &str, obj: &Bytes) -> ObjectResult<ObjectMetadata> {
    Ok(ObjectMetadata {
        key: path.to_owned(),
        last_modified: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(ObjectError::internal)?
            .as_secs_f64(),
        total_size: obj.len(),
    })
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use itertools::enumerate;

    use super::*;

    #[tokio::test]
    async fn test_upload() {
        let block = Bytes::from("123456");

        let s3 = InMemObjectStore::new();
        s3.upload("/abc", block).await.unwrap();

        // No such object.
        s3.read("/ab", Some(BlockLocation { offset: 0, size: 3 }))
            .await
            .unwrap_err();

        let bytes = s3
            .read("/abc", Some(BlockLocation { offset: 4, size: 2 }))
            .await
            .unwrap();
        assert_eq!(String::from_utf8(bytes.to_vec()).unwrap(), "56".to_string());

        // Overflow.
        s3.read("/abc", Some(BlockLocation { offset: 4, size: 4 }))
            .await
            .unwrap_err();

        s3.delete("/abc").await.unwrap();

        // No such object.
        s3.read("/abc", Some(BlockLocation { offset: 0, size: 3 }))
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn test_streaming_upload() {
        let blocks = vec![Bytes::from("123"), Bytes::from("456"), Bytes::from("789")];
        let obj = Bytes::from("123456789");

        let store = InMemObjectStore::new();
        let mut uploader = store.streaming_upload("/abc").unwrap();

        for block in blocks {
            uploader.write_bytes(block).await.unwrap();
        }
        uploader.finish().await.unwrap();

        // Read whole object.
        let read_obj = store.read("/abc", None).await.unwrap();
        assert!(read_obj.eq(&obj));

        // Read part of the object.
        let read_obj = store
            .read("/abc", Some(BlockLocation { offset: 4, size: 2 }))
            .await
            .unwrap();
        assert_eq!(
            String::from_utf8(read_obj.to_vec()).unwrap(),
            "56".to_string()
        );
    }

    #[tokio::test]
    async fn test_metadata() {
        let block = Bytes::from("123456");

        let obj_store = InMemObjectStore::new();
        obj_store.upload("/abc", block).await.unwrap();

        let metadata = obj_store.metadata("/abc").await.unwrap();
        assert_eq!(metadata.total_size, 6);
    }

    #[tokio::test]
    async fn test_list() {
        let payload = Bytes::from("123456");
        let store = InMemObjectStore::new();
        assert!(store.list("").await.unwrap().is_empty());

        let paths = vec!["001/002/test.obj", "001/003/test.obj"];
        for (i, path) in enumerate(paths.clone()) {
            assert_eq!(store.list("").await.unwrap().len(), i);
            store.upload(path, payload.clone()).await.unwrap();
            assert_eq!(store.list("").await.unwrap().len(), i + 1);
        }

        let list_path = store
            .list("")
            .await
            .unwrap()
            .iter()
            .map(|p| p.key.clone())
            .collect_vec();
        assert_eq!(list_path, paths);

        for i in 0..=5 {
            assert_eq!(store.list(&paths[0][0..=i]).await.unwrap().len(), 2);
        }
        for i in 6..=paths[0].len() - 1 {
            assert_eq!(store.list(&paths[0][0..=i]).await.unwrap().len(), 1);
        }
        assert!(store.list("003").await.unwrap().is_empty());

        for (i, path) in enumerate(paths.clone()) {
            assert_eq!(store.list("").await.unwrap().len(), paths.len() - i);
            store.delete(path).await.unwrap();
            assert_eq!(store.list("").await.unwrap().len(), paths.len() - i - 1);
        }
    }

    #[tokio::test]
    async fn test_delete_objects() {
        let block1 = Bytes::from("123456");
        let block2 = Bytes::from("987654");

        let store = InMemObjectStore::new();
        store.upload("/abc", block1).await.unwrap();
        store.upload("/klm", block2).await.unwrap();

        assert_eq!(store.list("").await.unwrap().len(), 2);

        let str_list = [
            String::from("/abc"),
            String::from("/klm"),
            String::from("/xyz"),
        ];

        store.delete_objects(&str_list).await.unwrap();

        assert_eq!(store.list("").await.unwrap().len(), 0);
    }
}

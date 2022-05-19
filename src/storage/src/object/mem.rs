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

use bytes::Bytes;
use fail::fail_point;
use futures::future::try_join_all;
use itertools::Itertools;
use tokio::sync::Mutex;

use super::{ObjectError, ObjectResult};
use crate::define_object_store_associated_types;
use crate::object::{BlockLocation, ObjectMetadata, ObjectStore};

/// In-memory object storage, useful for testing.
#[derive(Default)]
pub struct InMemObjectStore {
    objects: Mutex<HashMap<String, Bytes>>,
}

impl ObjectStore for InMemObjectStore {
    define_object_store_associated_types!();

    fn upload<'a>(&'a self, path: &'a str, obj: Bytes) -> Self::UploadFuture<'_> {
        async move {
            fail_point!("mem_upload_err", |_| Err(ObjectError::internal(
                "mem upload error"
            )));
            if obj.is_empty() {
                Err(ObjectError::internal("upload empty object"))
            } else {
                self.objects.lock().await.insert(path.into(), obj);
                Ok(())
            }
        }
    }

    fn read<'a>(&'a self, path: &'a str, block: Option<BlockLocation>) -> Self::ReadFuture<'_> {
        async move {
            fail_point!("mem_read_err", |_| Err(ObjectError::internal(
                "mem read error"
            )));
            if let Some(loc) = block {
                self.get_object(path, |obj| find_block(obj, loc)).await?
            } else {
                self.get_object(path, |obj| Ok(obj.clone())).await?
            }
        }
    }

    fn readv<'a>(
        &'a self,
        path: &'a str,
        block_locs: &'a [BlockLocation],
    ) -> Self::ReadvFuture<'_> {
        async move {
            let futures = block_locs
                .iter()
                .map(|block_loc| self.read(path, Some(*block_loc)))
                .collect_vec();
            try_join_all(futures).await
        }
    }

    fn metadata<'a>(&'a self, path: &'a str) -> Self::MetadataFuture<'_> {
        async move {
            let total_size = self.get_object(path, |v| v.len()).await?;
            Ok(ObjectMetadata { total_size })
        }
    }

    fn delete<'a>(&'a self, path: &'a str) -> Self::DeleteFuture<'_> {
        async move {
            fail_point!("mem_delete_err", |_| Err(ObjectError::internal(
                "mem delete error"
            )));
            self.objects.lock().await.remove(path);
            Ok(())
        }
    }
}

impl InMemObjectStore {
    pub fn new() -> Self {
        Self {
            objects: Mutex::new(HashMap::new()),
        }
    }

    async fn get_object<R, F>(&self, path: &str, f: F) -> ObjectResult<R>
    where
        F: Fn(&Bytes) -> R,
    {
        self.objects
            .lock()
            .await
            .get(path)
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

#[cfg(test)]
mod tests {
    use bytes::Bytes;

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
    async fn test_metadata() {
        let block = Bytes::from("123456");

        let obj_store = InMemObjectStore::new();
        obj_store.upload("/abc", block).await.unwrap();

        let metadata = obj_store.metadata("/abc").await.unwrap();
        assert_eq!(metadata.total_size, 6);
    }
}

use std::collections::HashMap;

use bytes::Bytes;
use futures::future::try_join_all;
use itertools::Itertools;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::{ensure, gen_error};
use tokio::sync::Mutex;

use crate::object::{BlockLocation, ObjectMetadata, ObjectStore};

/// In-memory object storage, useful for testing.
#[derive(Default)]
pub struct InMemObjectStore {
    objects: Mutex<HashMap<String, Bytes>>,
}

#[async_trait::async_trait]
impl ObjectStore for InMemObjectStore {
    async fn upload(&self, path: &str, obj: Bytes) -> Result<()> {
        ensure!(!obj.is_empty());
        self.objects.lock().await.insert(path.into(), obj);
        Ok(())
    }

    async fn read(&self, path: &str, block: Option<BlockLocation>) -> Result<Bytes> {
        if let Some(loc) = block {
            self.get_object(path, |obj| find_block(obj, loc)).await?
        } else {
            self.get_object(path, |obj| Ok(obj.clone())).await?
        }
    }

    async fn readv(&self, path: &str, block_locs: Vec<BlockLocation>) -> Result<Vec<Bytes>> {
        let futures = block_locs
            .into_iter()
            .map(|block_loc| self.read(path, Some(block_loc)))
            .collect_vec();
        try_join_all(futures).await
    }

    async fn metadata(&self, path: &str) -> Result<ObjectMetadata> {
        let total_size = self.get_object(path, |v| v.len()).await?;
        Ok(ObjectMetadata { total_size })
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.objects.lock().await.remove(path);
        Ok(())
    }
}

impl InMemObjectStore {
    pub fn new() -> Self {
        Self {
            objects: Mutex::new(HashMap::new()),
        }
    }

    async fn get_object<R, F>(&self, path: &str, f: F) -> Result<R>
    where
        F: Fn(&Bytes) -> R,
    {
        self.objects
            .lock()
            .await
            .get(path)
            .ok_or_else(|| RwError::from(InternalError(format!("no object at path '{}'", path))))
            .map(f)
    }
}

fn find_block(obj: &Bytes, block: BlockLocation) -> Result<Bytes> {
    ensure!(block.offset + block.size <= obj.len());
    Ok(obj.slice(block.offset..(block.offset + block.size)))
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

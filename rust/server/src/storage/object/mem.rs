use crate::storage::object::{BlockLocation, ObjectHandle, ObjectMetadata, ObjectStore};
use bytes::Bytes;
use risingwave_common::array::RwError;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::Result;
use std::collections::HashMap;
use tokio::sync::Mutex;

/// In-memory object storage, useful for testing.
pub struct InMemObjectStore {
    objects: Mutex<HashMap<String, Bytes>>,
}

impl ObjectHandle for String {}

#[async_trait::async_trait]
impl ObjectStore<String> for InMemObjectStore {
    async fn upload(&self, path: &String, obj: Bytes) -> Result<()> {
        ensure!(!obj.is_empty());
        self.objects.lock().await.insert(path.into(), obj);
        Ok(())
    }

    async fn read(&self, path: &String, block: BlockLocation) -> Result<Vec<u8>> {
        self.get_object(path, |obj| find_block(obj, block)).await?
    }

    async fn metadata(&self, path: &String) -> Result<ObjectMetadata> {
        let total_size = self.get_object(path, |v| v.len()).await?;
        Ok(ObjectMetadata { total_size })
    }

    async fn close(&self, _path: &String) -> Result<()> {
        // `InMemObjectStore` is for testing purpose only. No need to do this.
        Ok(())
    }

    async fn delete(&self, path: &String) -> Result<()> {
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

fn find_block(obj: &Bytes, block: BlockLocation) -> Result<Vec<u8>> {
    ensure!(block.offset + block.size <= obj.len());
    Ok(obj
        .slice(block.offset..(block.offset + block.size))
        .to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[tokio::test]
    async fn test_upload() {
        let block = Bytes::from("123456");

        let s3 = InMemObjectStore::new();
        s3.upload(&"/abc".to_string(), block).await.unwrap();

        // No such object.
        s3.read(&"/ab".to_string(), BlockLocation { offset: 0, size: 3 })
            .await
            .unwrap_err();

        let bytes = s3
            .read(&"/abc".to_string(), BlockLocation { offset: 4, size: 2 })
            .await
            .unwrap();
        assert_eq!(String::from_utf8(bytes.to_vec()).unwrap(), "56".to_string());

        // Overflow.
        s3.read(&"/abc".to_string(), BlockLocation { offset: 4, size: 4 })
            .await
            .unwrap_err();

        s3.delete(&"/abc".to_string()).await.unwrap();

        // No such object.
        s3.read(&"/abc".to_string(), BlockLocation { offset: 0, size: 3 })
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn test_metadata() {
        let block = Bytes::from("123456");

        let obj_store = InMemObjectStore::new();
        obj_store.upload(&"/abc".to_string(), block).await.unwrap();

        let metadata = obj_store.metadata(&"/abc".to_string()).await.unwrap();
        assert_eq!(metadata.total_size, 6);
    }
}

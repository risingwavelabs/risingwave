use crate::storage::object::Bytes;
use crate::storage::ObjectStore;
use risingwave_common::{
    array::RwError,
    error::{ErrorCode::InternalError, Result},
};
use rusoto_core::ByteStream;
use rusoto_s3::{PutObjectRequest, S3Client, S3};
use std::collections::HashMap;
use tokio::sync::Mutex;

use super::{BlockLocation, ObjectMetadata};

/// Implement object store on S3.
pub struct S3ObjectStore {
    client: Option<S3Client>,
    bucket: String,
    object: Mutex<HashMap<String, Bytes>>,
}

#[async_trait::async_trait]
impl ObjectStore for S3ObjectStore {
    async fn upload(&self, path: &str, obj: Bytes) -> Result<()> {
        ensure!(!obj.is_empty());
        let client = self
            .ensure_connection()
            .ok_or_else(|| RwError::from(InternalError("S3 connection failure".to_string())))?;

        // TODO(xiangyhu): what if the blob existed?
        let data = ByteStream::from(obj.as_ref().to_vec());
        client
            .put_object(PutObjectRequest {
                bucket: self.bucket.clone(),
                key: path.to_string(),
                body: Some(data),
                ..Default::default()
            })
            .await
            .map_err(|err| {
                RwError::from(InternalError(format!("S3 put failure with error: {}", err)))
            })?;

        Ok(())
    }

    async fn read(&self, _path: &str, _block: BlockLocation) -> Result<Vec<u8>> {
        todo!();
    }

    async fn metadata(&self, _path: &str) -> Result<ObjectMetadata> {
        todo!();
    }

    async fn close(&self, _path: &str) -> Result<()> {
        todo!();
    }

    async fn delete(&self, _path: &str) -> Result<()> {
        todo!();
    }
}

impl S3ObjectStore {
    fn ensure_connection(&self) -> Option<&S3Client> {
        // TODO(xiangyhu): Make sure s3 client is valid and connectable with retry.
        self.client.as_ref()
    }
}

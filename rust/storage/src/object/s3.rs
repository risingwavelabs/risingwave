use crate::object::Bytes;
use crate::object::ObjectStore;
use risingwave_common::ensure;
use risingwave_common::gen_error;
use risingwave_common::{
    array::RwError,
    error::{ErrorCode::InternalError, Result},
};
use rusoto_core::{
    credential::{AwsCredentials, StaticProvider},
    ByteStream, HttpClient, Region, RusotoError,
};
use rusoto_s3::{
    DeleteObjectRequest, GetObjectError, GetObjectRequest, HeadObjectRequest, PutObjectRequest,
    S3Client, S3,
};

use std::{collections::HashMap, str::FromStr};
use tokio::io::AsyncReadExt;
use tokio::sync::Mutex;

use super::{BlockLocation, ObjectMetadata};

/// Implement object store on S3.
pub struct S3ObjectStore {
    connection_info: Option<ConnectionInfo>,
    // None is reserved for to-be-apperaed connection pool
    client: Option<S3Client>,
    bucket: String,
    object: Mutex<HashMap<String, Bytes>>,
}

#[derive(Clone)]
pub struct ConnectionInfo {
    region: String,
    access_key: String,
    secret_key: String,
    token: Option<String>,
}

impl From<ConnectionInfo> for AwsCredentials {
    fn from(connection_info: ConnectionInfo) -> AwsCredentials {
        AwsCredentials::new(
            connection_info.access_key,
            connection_info.secret_key,
            connection_info.token,
            None,
        )
    }
}

impl ConnectionInfo {
    #[cfg(test)]
    pub fn new_for_test_account() -> Self {
        Self {
            region: std::env::var("S3_TEST_REGION")
                .map_err(|err| format!("no s3 access key found: {}", err))
                .unwrap(),
            access_key: std::env::var("S3_TEST_ACCESS_KEY")
                .map_err(|err| format!("no s3 access key found: {}", err))
                .unwrap(),
            secret_key: std::env::var("S3_TEST_SECRET_KEY")
                .map_err(|err| format!("no s3 access key found: {}", err))
                .unwrap(),
            token: None,
        }
    }

    pub fn new(
        region: String,
        access_key: String,
        secret_key: String,
        token: Option<String>,
    ) -> Self {
        Self {
            region,
            access_key,
            secret_key,
            token,
        }
    }
}

#[async_trait::async_trait]
impl ObjectStore for S3ObjectStore {
    async fn upload(&self, path: &str, obj: Bytes) -> Result<()> {
        ensure!(!obj.is_empty());
        ensure!(!self.client.is_none());

        // TODO(xiangyhu): what if the blob existed?
        let data = ByteStream::from(obj.as_ref().to_vec());
        self.client
            .as_ref()
            .unwrap()
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

    /// Amazon S3 doesn't support retrieving multiple ranges of data per GET request.
    async fn read(&self, path: &str, block_loc: Option<BlockLocation>) -> Result<Vec<u8>> {
        ensure!(!self.client.is_none());

        let response = self
            .client
            .as_ref()
            .unwrap()
            .get_object(GetObjectRequest {
                bucket: self.bucket.clone(),
                key: path.to_string(),
                range: block_loc.unwrap().byte_range_specifier(),
                ..Default::default()
            })
            .await;

        match response {
            Ok(data) => {
                let mut val = Vec::new();
                data.body
                    .ok_or_else(|| {
                        RwError::from(InternalError(format!(
                            "request object {} but body is missing",
                            path
                        )))
                    })?
                    .into_async_read()
                    .read_to_end(&mut val)
                    .await
                    .map_err(|err| {
                        RwError::from(InternalError(format!(
                            "reading object {} failed with error {}",
                            path, err
                        )))
                    })?;
                Ok(val)
            }
            Err(RusotoError::Service(GetObjectError::NoSuchKey(_))) => {
                return Err(RwError::from(InternalError(format!(
                    "No object named {} was found",
                    path
                ))))
            }
            Err(err) => {
                return Err(RwError::from(InternalError(format!(
                    "S3 put failure with error: {}",
                    err
                ))))
            }
        }
    }

    async fn metadata(&self, path: &str) -> Result<ObjectMetadata> {
        ensure!(!self.client.is_none());

        let response = self
            .client
            .as_ref()
            .unwrap()
            .head_object(HeadObjectRequest {
                bucket: self.bucket.clone(),
                key: path.to_string(),
                ..Default::default()
            })
            .await
            .map_err(|err| {
                RwError::from(InternalError(format!(
                    "S3 fetch metadata failure with error: {}",
                    err
                )))
            })?;

        match response.content_length {
            Some(len) => Ok(ObjectMetadata {
                total_size: len as usize,
            }),
            None => Err(RwError::from(InternalError(
                "No matched fields".to_string(),
            ))),
        }
    }

    /// Permanently delete the whole object.
    /// According to Amazon S3, this will simply return Ok if the object does not exist.
    async fn delete(&self, path: &str) -> Result<()> {
        ensure!(!self.client.is_none());

        self.client
            .as_ref()
            .unwrap()
            .delete_object(DeleteObjectRequest {
                bucket: self.bucket.clone(),
                key: path.to_string(),
                ..Default::default()
            })
            .await
            .map_err(|err| {
                RwError::from(InternalError(format!(
                    "S3 delete failure with error: {}",
                    err
                )))
            })?;

        Ok(())
    }
}

impl S3ObjectStore {
    pub fn new(conn_info: ConnectionInfo, bucket: String) -> Self {
        Self {
            connection_info: Some(conn_info.clone()),
            client: Some(S3Client::new_with(
                HttpClient::new().expect("Failed to create HTTP client"),
                StaticProvider::from(AwsCredentials::from(conn_info.clone())),
                Region::from_str(conn_info.region.as_ref()).unwrap(),
            )),
            bucket,
            object: Mutex::new(HashMap::new()),
        }
    }
}

/// We cannot leave credentials in our code history.
/// To run this test, please read this page for references.
/// [Run S3 tests](https://singularity-data.larksuite.com/docs/docuszYRfc00x6Q0QhqidP2AxEg)
#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use uuid::Uuid;

    #[tokio::test]
    #[ignore]
    async fn test_upload() {
        let block = Bytes::from("123456");
        let conn_info = ConnectionInfo::new_for_test_account();
        let bucket = "s3-ut";
        let path = Uuid::new_v4().to_string();

        let s3 = S3ObjectStore::new(conn_info, bucket.to_string());
        s3.upload(&path, block).await.unwrap();

        // No such object.
        s3.read(
            &"/ab".to_string(),
            Some(BlockLocation { offset: 0, size: 3 }),
        )
        .await
        .unwrap_err();

        let bytes = s3
            .read(&path, Some(BlockLocation { offset: 4, size: 2 }))
            .await
            .unwrap();
        assert_eq!(String::from_utf8(bytes.to_vec()).unwrap(), "56".to_string());

        // Overflow.
        s3.read(&path, Some(BlockLocation { offset: 7, size: 7 }))
            .await
            .unwrap_err();
    }

    #[tokio::test]
    #[ignore]
    async fn test_delete() {
        let block = Bytes::from("123456");
        let conn_info = ConnectionInfo::new_for_test_account();
        let bucket = "s3-ut";
        let path = Uuid::new_v4().to_string();

        let s3 = S3ObjectStore::new(conn_info, bucket.to_string());

        // delete an inexistent object should be OK
        s3.delete(&path).await.unwrap();

        s3.upload(&path, block).await.unwrap();

        // read partial object
        let bytes = s3
            .read(&path, Some(BlockLocation { offset: 4, size: 2 }))
            .await
            .unwrap();
        assert_eq!(String::from_utf8(bytes.to_vec()).unwrap(), "56".to_string());

        // delete object
        s3.delete(&path).await.unwrap();

        // should fail to read partial objects again
        let read_res = s3
            .read(&path, Some(BlockLocation { offset: 4, size: 2 }))
            .await
            .map_err(|e| e.to_string());

        assert!(matches!(read_res, Err(_)));
    }

    #[tokio::test]
    #[ignore]
    async fn test_metadata() {
        // get S3 client
        let block = Bytes::from("123456");
        let conn_info = ConnectionInfo::new_for_test_account();
        let bucket = "s3-ut";
        let path = Uuid::new_v4().to_string();
        let s3 = S3ObjectStore::new(conn_info, bucket.to_string());

        // upload data
        s3.upload(&path, block).await.unwrap();

        // read metadata
        let data_size = s3.metadata(&path).await.unwrap().total_size;
        assert_eq!(data_size, 6);

        // should not read after delete
        s3.delete(&path).await.unwrap();
        let res = s3.metadata(&path).await.map_err(|e| e.to_string());
        assert!(matches!(res, Err(_)));
    }
}

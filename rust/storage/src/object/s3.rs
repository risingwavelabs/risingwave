use std::collections::HashMap;
use std::str::FromStr;

use risingwave_common::array::RwError;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::Result;
use risingwave_common::{ensure, gen_error};
use rusoto_core::credential::{AwsCredentials, StaticProvider};
use rusoto_core::{ByteStream, HttpClient, Region, RusotoError};
use rusoto_s3::{
    DeleteObjectRequest, GetObjectError, GetObjectRequest, HeadObjectRequest, PutObjectRequest,
    S3Client, S3,
};
use tokio::io::AsyncReadExt;
use tokio::sync::Mutex;

use super::{BlockLocation, ObjectMetadata};
use crate::object::{Bytes, ObjectStore};

/// Implement object store on S3.
pub struct S3ObjectStore {
    connection_info: Option<ConnectionInfo>,
    // None is reserved for to-be-apperaed connection pool
    client: Option<S3Client>,
    bucket: String,
    object: Mutex<HashMap<String, Bytes>>,
}

#[derive(Clone, Default)]
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
    /// Please see [Set up Local S3 Accounts](https://singularity-data.larksuite.com/docs/docuszYRfc00x6Q0QhqidP2AxEg)
    /// to set up the credentials for test accounts.
    pub fn new() -> Self {
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
}

#[async_trait::async_trait]
impl ObjectStore for S3ObjectStore {
    async fn upload(&self, path: &str, obj: Bytes) -> Result<()> {
        ensure!(!obj.is_empty());
        ensure!(self.client.is_some());

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
        ensure!(self.client.is_some());
        let block_loc = block_loc.as_ref().unwrap();

        let response = self
            .client
            .as_ref()
            .unwrap()
            .get_object(GetObjectRequest {
                bucket: self.bucket.clone(),
                key: path.to_string(),
                range: block_loc.byte_range_specifier(),
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
                assert_eq!(
                    block_loc.size,
                    val.len(),
                    "mismatched size: expected {}, found {} when reading {} at {:?}",
                    block_loc.size,
                    val.len(),
                    path,
                    block_loc
                );
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
                    "S3 read failure with error: {}",
                    err
                ))))
            }
        }
    }

    async fn metadata(&self, path: &str) -> Result<ObjectMetadata> {
        ensure!(self.client.is_some());

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
        ensure!(self.client.is_some());

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

    pub fn new_with_s3_client(client: S3Client, bucket: String) -> Self {
        Self {
            connection_info: None,
            client: Some(client),
            bucket,
            object: Mutex::new(HashMap::new()),
        }
    }

    /// Create a minio client. The server should be like `minio://key:secret@address:port/bucket`.
    pub fn new_with_minio(server: &str) -> Self {
        // TODO: don't hard-code configurations
        let server = server.strip_prefix("minio://").unwrap();
        let (key, rest) = server.split_once(':').unwrap();
        let (secret, rest) = rest.split_once('@').unwrap();
        let (address, bucket) = rest.split_once('/').unwrap();
        Self::new_with_s3_client(
            S3Client::new_with(
                HttpClient::new().expect("Failed to create HTTP client"),
                StaticProvider::from(AwsCredentials::new(
                    key.to_string(),
                    secret.to_string(),
                    None,
                    None,
                )),
                rusoto_core::Region::Custom {
                    name: "minio".to_string(),
                    endpoint: format!("http://{}", address),
                },
            ),
            bucket.to_string(),
        )
    }
}

/// We cannot leave credentials in our code history.
/// To run this test, please read this page for references.
/// [Run S3 tests](https://singularity-data.larksuite.com/docs/docuszYRfc00x6Q0QhqidP2AxEg)
#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use uuid::Uuid;

    use super::*;

    #[tokio::test]
    #[ignore]
    async fn test_upload() {
        let block = Bytes::from("123456");
        let conn_info = ConnectionInfo::new();
        let bucket = "s3-ut";
        let path = Uuid::new_v4().to_string();

        let s3 = S3ObjectStore::new(conn_info, bucket.to_string());
        s3.upload(&path, block).await.unwrap();

        // No such object.
        s3.read("/ab", Some(BlockLocation { offset: 0, size: 3 }))
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
        let conn_info = ConnectionInfo::new();
        let bucket = "s3-ut";
        let path = Uuid::new_v4().to_string();

        let s3 = S3ObjectStore::new(conn_info, bucket.to_string());

        // delete an non-existing object should be OK
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
        let conn_info = ConnectionInfo::new();
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

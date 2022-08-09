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

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use aws_sdk_s3::model::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::output::UploadPartOutput;
use aws_sdk_s3::{Client, Endpoint, Region};
use fail::fail_point;
use futures::future::try_join_all;
use futures::stream;
use hyper::Body;
use itertools::Itertools;
use madsim::task::JoinHandle;

use crate::object::{
    BlockLocation, Bytes, ObjectError, ObjectMetadata, ObjectResult, ObjectStore,
    StreamingUploader, StreamingUploaderImpl,
};

type PartId = i32;

const MIN_PART_ID: PartId = 1;
const MIN_PART_SIZE: usize = 5 * 1024 * 1024;
const PART_SIZE: usize = 16 * 1024 * 1024;

/// S3 multipart upload handle.
/// Reference: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html>
pub struct S3StreamingUploader {
    client: Arc<Client>,
    bucket: String,
    /// The key of the object.
    key: String,
    /// The identifier of multipart upload task for S3.
    upload_id: String,
    /// Next part ID.
    next_part_id: PartId,
    /// Join handles for part uploads.
    join_handles: Vec<JoinHandle<ObjectResult<()>>>,
    /// Parts that are already uploaded to S3.
    uploaded_parts: Arc<Mutex<BTreeMap<PartId, UploadPartOutput>>>,
    /// Buffer for bytes.
    buf: Vec<Bytes>,
    /// Length of the data that have not been uploaded to S3.
    not_uploaded_len: usize,
    /// Length of the data that exceeds the current part to be uploaded in the buffer.
    next_part_len: usize,
    /// Two pointers into` buf` indicating the bytes to group into a part for the next upload.
    /// `part_end` is exclusive.
    part_begin: usize,
    part_end: usize,
}

impl S3StreamingUploader {
    pub fn new(
        client: Arc<Client>,
        bucket: String,
        key: String,
        upload_id: String,
    ) -> S3StreamingUploader {
        Self {
            client,
            bucket,
            key,
            upload_id,
            next_part_id: MIN_PART_ID,
            join_handles: Default::default(),
            uploaded_parts: Default::default(),
            buf: Default::default(),
            not_uploaded_len: 0,
            next_part_len: 0,
            part_begin: 0,
            part_end: 0,
        }
    }

    fn upload_part(&mut self, data: Vec<Bytes>, len: usize) {
        let part_id = self.next_part_id;
        self.next_part_id += 1;
        let client_cloned = self.client.clone();
        let uploaded_parts_cloned = self.uploaded_parts.clone();
        let bucket = self.bucket.clone();
        let key = self.key.clone();
        let upload_id = self.upload_id.clone();
        self.join_handles.push(tokio::spawn(async move {
            println!("{:?}: {:?}", part_id, len);
            let upload_output = client_cloned
                .upload_part()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(part_id)
                .body(get_upload_body(data))
                .content_length(len as i64)
                .send()
                .await?;
            uploaded_parts_cloned
                .lock()
                .map_err(ObjectError::internal)?
                .insert(part_id, upload_output);
            println!("{:?} done", part_id);
            Ok(())
        }));
    }

    async fn flush_and_complete(&mut self) -> ObjectResult<()> {
        self.upload_part(
            Vec::from_iter(self.buf[self.part_begin..].iter().cloned()),
            self.not_uploaded_len,
        );

        // If any part fails to upload, abort the upload.
        let join_handles = self.join_handles.drain(..).collect_vec();
        for result in try_join_all(join_handles)
            .await
            .map_err(ObjectError::internal)?
        {
            result?;
        }
        println!("all joined");

        let completed_parts = Some(
            self.uploaded_parts
                .lock()
                .map_err(ObjectError::internal)?
                .iter()
                .map(|(part_id, output)| {
                    CompletedPart::builder()
                        .set_e_tag(output.e_tag.clone())
                        .set_part_number(Some(*part_id))
                        .build()
                })
                .collect_vec(),
        );
        // If fail to complete the upload, abort the upload.
        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(&self.upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(completed_parts)
                    .build(),
            )
            .send()
            .await?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl StreamingUploader for S3StreamingUploader {
    fn write_bytes(&mut self, data: Bytes) -> ObjectResult<()> {
        fail_point!("s3_write_bytes_err", |_| Err(ObjectError::internal(
            "s3 write bytes error"
        )));
        let data_len = data.len();
        self.not_uploaded_len += data_len;
        self.buf.push(data);

        if self.not_uploaded_len > PART_SIZE {
            if self.part_begin == self.part_end {
                // Mark current slice of buffer to be the next part to be uploaded.
                self.part_end = self.buf.len();
                assert!(self.part_begin < self.part_end);
            } else {
                // `data` should be uploaded in the next part.
                self.next_part_len += data_len;
            }
        }
        if self.next_part_len >= MIN_PART_SIZE {
            // Take a 16MiB part and upload it. `Bytes` performs shallow clone.
            self.upload_part(
                Vec::from_iter(self.buf[self.part_begin..self.part_end].iter().cloned()),
                self.not_uploaded_len - self.next_part_len,
            );
            self.not_uploaded_len = self.next_part_len;
            self.next_part_len = 0;
            self.part_begin = self.part_end;
        }
        Ok(())
    }

    /// If the data in the buffer is smaller than `MIN_PART_SIZE`, abort multipart upload
    /// and use `PUT` to upload the data. Otherwise flush the remaining data of the buffer
    /// to S3 as a new part.
    async fn finish(mut self) -> ObjectResult<()> {
        fail_point!("s3_finish_streaming_upload_err", |_| Err(
            ObjectError::internal("s3 finish streaming upload error")
        ));
        if (self.part_begin == 0 && self.part_end == 0) || self.flush_and_complete().await.is_err()
        {
            self.client
                .abort_multipart_upload()
                .bucket(&self.bucket)
                .key(&self.key)
                .upload_id(&self.upload_id)
                .send()
                .await?;
            // As abort should happen very rarely, it's ok to recalculate the whole data length.
            let data_len = self.buf.iter().map(|b| b.len() as i64).sum();
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(&self.key)
                .body(get_upload_body(self.buf))
                .content_length(data_len)
                .send()
                .await?;
        }
        Ok(())
    }
}

fn get_upload_body(data: Vec<Bytes>) -> aws_sdk_s3::types::ByteStream {
    Body::wrap_stream(stream::iter(data.into_iter().map(ObjectResult::Ok))).into()
}

/// Object store with S3 backend
pub struct S3ObjectStore {
    client: Arc<Client>,
    bucket: String,
}

#[async_trait::async_trait]
impl ObjectStore for S3ObjectStore {
    type Uploader = StreamingUploaderImpl;

    async fn upload(&self, path: &str, obj: Bytes) -> ObjectResult<()> {
        fail_point!("s3_upload_err", |_| Err(ObjectError::internal(
            "s3 upload error"
        )));
        self.client
            .put_object()
            .bucket(&self.bucket)
            .body(aws_sdk_s3::types::ByteStream::from(obj))
            .key(path)
            .send()
            .await?;
        Ok(())
    }

    async fn streaming_upload(&self, path: &str) -> ObjectResult<Self::Uploader> {
        fail_point!("s3_streaming_upload_err", |_| Err(ObjectError::internal(
            "s3 streaming upload error"
        )));
        let resp = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(path)
            .send()
            .await?;
        Ok(Self::Uploader::S3(S3StreamingUploader::new(
            self.client.clone(),
            self.bucket.clone(),
            path.to_string(),
            resp.upload_id.unwrap(),
        )))
    }

    /// Amazon S3 doesn't support retrieving multiple ranges of data per GET request.
    async fn read(&self, path: &str, block_loc: Option<BlockLocation>) -> ObjectResult<Bytes> {
        fail_point!("s3_read_err", |_| Err(ObjectError::internal(
            "s3 read error"
        )));
        let req = self.client.get_object().bucket(&self.bucket).key(path);

        let range = match block_loc.as_ref() {
            None => None,
            Some(block_location) => block_location.byte_range_specifier(),
        };

        let req = if let Some(range) = range {
            req.range(range)
        } else {
            req
        };

        let resp = req.send().await?;
        let val = resp.body.collect().await?.into_bytes();

        if block_loc.is_some() && block_loc.as_ref().unwrap().size != val.len() {
            return Err(ObjectError::internal(format!(
                "mismatched size: expected {}, found {} when reading {} at {:?}",
                block_loc.as_ref().unwrap().size,
                val.len(),
                path,
                block_loc.as_ref().unwrap()
            )));
        }
        Ok(val)
    }

    async fn readv(&self, path: &str, block_locs: &[BlockLocation]) -> ObjectResult<Vec<Bytes>> {
        let futures = block_locs
            .iter()
            .map(|block_loc| self.read(path, Some(*block_loc)))
            .collect_vec();
        try_join_all(futures).await
    }

    async fn metadata(&self, path: &str) -> ObjectResult<ObjectMetadata> {
        fail_point!("s3_metadata_err", |_| Err(ObjectError::internal(
            "s3 metadata error"
        )));
        let resp = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(path)
            .send()
            .await?;
        Ok(ObjectMetadata {
            key: path.to_owned(),
            last_modified: resp
                .last_modified()
                .expect("last_modified required")
                .as_secs_f64(),
            total_size: resp.content_length as usize,
        })
    }

    /// Permanently deletes the whole object.
    /// According to Amazon S3, this will simply return Ok if the object does not exist.
    async fn delete(&self, path: &str) -> ObjectResult<()> {
        fail_point!("s3_delete_err", |_| Err(ObjectError::internal(
            "s3 delete error"
        )));
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(path)
            .send()
            .await?;
        Ok(())
    }

    async fn list(&self, prefix: &str) -> ObjectResult<Vec<ObjectMetadata>> {
        let mut ret: Vec<ObjectMetadata> = vec![];
        let mut next_continuation_token = None;
        // list_objects_v2 returns up to 1000 keys and truncated the exceeded parts.
        // Use `continuation_token` given by last response to fetch more parts of the result,
        // until result is no longer truncated.
        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);
            if let Some(continuation_token) = next_continuation_token.take() {
                request = request.continuation_token(continuation_token);
            }
            let result = request.send().await?;
            let is_truncated = result.is_truncated;
            ret.append(
                &mut result
                    .contents()
                    .unwrap_or_default()
                    .iter()
                    .map(|obj| ObjectMetadata {
                        key: obj.key().expect("key required").to_owned(),
                        last_modified: obj
                            .last_modified()
                            .expect("last_modified required")
                            .as_secs_f64(),
                        total_size: obj.size() as usize,
                    })
                    .collect_vec(),
            );
            next_continuation_token = result.next_continuation_token;
            if !is_truncated {
                break;
            }
        }
        Ok(ret)
    }
}

impl S3ObjectStore {
    /// Creates an S3 object store from environment variable.
    ///
    /// See [AWS Docs](https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credentials.html) on how to provide credentials and region from env variable. If you are running compute-node on EC2, no configuration is required.
    pub async fn new(bucket: String) -> Self {
        let shared_config = aws_config::load_from_env().await;
        let client = Arc::new(Client::new(&shared_config));

        Self { client, bucket }
    }

    /// Creates a minio client. The server should be like `minio://key:secret@address:port/bucket`.
    pub async fn with_minio(server: &str) -> Self {
        let server = server.strip_prefix("minio://").unwrap();
        let (access_key_id, rest) = server.split_once(':').unwrap();
        let (secret_access_key, rest) = rest.split_once('@').unwrap();
        let (address, bucket) = rest.split_once('/').unwrap();

        let loader = aws_config::ConfigLoader::default();
        let builder = aws_sdk_s3::config::Builder::from(&loader.load().await);
        let builder = builder.region(Region::new("custom"));
        let builder = builder.endpoint_resolver(Endpoint::immutable(
            format!("http://{}", address).try_into().unwrap(),
        ));
        let builder = builder.credentials_provider(aws_sdk_s3::Credentials::from_keys(
            access_key_id,
            secret_access_key,
            None,
        ));
        let config = builder.build();
        let client = Arc::new(Client::from_conf(config));
        Self {
            client,
            bucket: bucket.to_string(),
        }
    }
}

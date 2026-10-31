// Copyright 2026 RisingWave Labs
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

use std::collections::BTreeMap;
use std::sync::Mutex;

use http::{Method, Request, Response};
use opendal::raw::{HttpBody, HttpClient, HttpFetch};
use opendal::{Buffer, Result};

use super::*;

/// Records real S3 protocol requests without credentials or a network service.
#[derive(Clone, Default)]
struct RecordingS3 {
    parts: Arc<Mutex<BTreeMap<usize, Buffer>>>,
    slow_large_parts: bool,
}

impl HttpFetch for RecordingS3 {
    async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
        if self.slow_large_parts
            && req.method() == Method::PUT
            && req.body().len() > 5 * 1024 * 1024
        {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        let query = req.uri().query().unwrap_or_default();
        let body = match *req.method() {
            Method::POST if query.contains("uploads") => {
                "<InitiateMultipartUploadResult><UploadId>test</UploadId></InitiateMultipartUploadResult>"
            }
            Method::PUT => {
                let part = query
                    .split('&')
                    .find_map(|p| p.strip_prefix("partNumber="))
                    .map_or(0, |n| n.parse().unwrap());
                self.parts.lock().unwrap().insert(part, req.body().clone());
                ""
            }
            Method::POST => {
                "<CompleteMultipartUploadResult><ETag>test</ETag></CompleteMultipartUploadResult>"
            }
            _ => panic!("unexpected S3 request: {} {}", req.method(), req.uri()),
        };
        Ok(Response::builder()
            .header("etag", "test")
            .body(HttpBody::new(
                stream::iter([Ok(Buffer::from(body))]),
                Some(body.len() as u64),
            ))
            .unwrap())
    }
}

#[tokio::test]
async fn test_shared_compactor_bounds_large_metadata_parts() {
    check_large_metadata(false).await;
}

#[tokio::test]
async fn test_shared_compactor_avoids_oversized_part_timeout() {
    check_large_metadata(true).await;
}

async fn check_large_metadata(simulate_timeout: bool) {
    const PART_SIZE: usize = 5 * 1024 * 1024; // S3 minimum multipart size.
    for shared in [false, true] {
        let recorder = RecordingS3 {
            slow_large_parts: simulate_timeout,
            ..Default::default()
        };
        let op = Operator::new(
            opendal::services::S3::default()
                .bucket("test")
                .region("us-east-1")
                .endpoint("http://s3.test")
                .access_key_id("test")
                .secret_access_key("test"),
        )
        .unwrap()
        .finish();
        op.update_http_client(|_| HttpClient::with(recorder.clone()));

        let mut original = ObjectStoreConfig {
            upload_part_size: PART_SIZE,
            ..Default::default()
        };
        if simulate_timeout {
            original.retry.streaming_upload_attempt_timeout_ms = 50;
            original.retry.streaming_upload_retry_attempts = 0;
        }
        let config = if shared {
            original.with_shared_compactor_uploads()
        } else {
            original
        };
        let mut uploader = OpendalStreamingUploader::new(
            op,
            "test.data".to_owned(),
            Arc::new(config),
            Arc::new(ObjectStoreMetrics::unused()),
            "S3",
        )
        .await
        .unwrap();
        let mut expected = Vec::new();
        // Data parts followed by one metadata buffer much larger than a part.
        for (byte, len) in [(1, PART_SIZE), (2, PART_SIZE), (3, 2 * PART_SIZE + 17)] {
            let buffer = Bytes::from(vec![byte; len]);
            expected.extend_from_slice(&buffer);
            uploader.write_bytes(buffer).await.unwrap();
        }
        let result = uploader.finish().await;
        if simulate_timeout && !shared {
            assert!(result.is_err(), "legacy oversized part must time out");
            continue;
        }
        result.unwrap();

        let parts = recorder.parts.lock().unwrap();
        let sizes = parts.values().map(Buffer::len).collect::<Vec<_>>();
        if shared {
            assert_eq!(sizes, vec![PART_SIZE, PART_SIZE, PART_SIZE, PART_SIZE, 17]);
        } else {
            // The unchanged path preserves the previous oversized request.
            assert!(sizes.iter().any(|&size| size > PART_SIZE));
        }
        let actual = parts
            .values()
            .flat_map(|buffer| buffer.to_bytes().to_vec())
            .collect::<Vec<_>>();
        assert!(actual == expected, "multipart upload changed object bytes");
    }
}

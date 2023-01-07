// Copyright 2023 Singularity Data
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

use std::collections::HashMap;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use aws_sdk_s3::client as s3_client;
use aws_smithy_http::byte_stream::ByteStream;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use io::StreamReader;
use tokio::io::BufReader;
use tokio_util::io;
use tokio_util::io::ReaderStream;

use crate::aws_utils::{default_conn_config, s3_client, AwsConfigV2};
use crate::source::base::MAX_CHUNK_SIZE;
use crate::source::filesystem::file_common::{FsSplit, FsSplitReader};
use crate::source::filesystem::s3::S3Properties;
use crate::source::{BoxFsSourceStream, FsSourceMessage};
const MAX_CHANNEL_BUFFER_SIZE: usize = 2048;
const STREAM_READER_CAPACITY: usize = 4096;

#[derive(Debug, Clone)]
struct S3InnerMessage {
    obj_name: String,
    payload: Bytes,
    offset: usize,
    size: usize,
}

#[derive(Debug)]
pub struct S3FileReader {
    split_offset: HashMap<String, u64>,
    bucket_name: String,
    s3_client: s3_client::Client,
    splits: Vec<FsSplit>,
}

impl S3FileReader {
    #[try_stream(boxed, ok = S3InnerMessage, error = anyhow::Error)]
    async fn stream_read(
        client_for_s3: s3_client::Client,
        bucket_name: String,
        splits: Vec<FsSplit>,
    ) {
        for split in splits {
            let object_name = split.name;
            let byte_stream =
                S3FileReader::get_object(&client_for_s3, &bucket_name, &object_name, split.offset)
                    .await?;
            let stream_reader = StreamReader::new(
                byte_stream.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
            );

            let reader = Box::pin(BufReader::new(stream_reader));

            let stream = ReaderStream::with_capacity(reader, STREAM_READER_CAPACITY);

            let mut offset: usize = split.offset;
            #[for_await]
            for read in stream {
                let bytes = read?;
                let len = bytes.len();
                let msg = S3InnerMessage {
                    obj_name: object_name.clone(),
                    payload: bytes,
                    offset,
                    size: split.size,
                };
                offset += len;
                yield msg;
            }
        }
    }

    async fn get_object(
        client_for_s3: &s3_client::Client,
        bucket_name: &str,
        object_name: &str,
        start: usize,
    ) -> anyhow::Result<ByteStream> {
        let range = if start == 0 {
            None
        } else {
            Some(format!("bytes={}-", start))
        };
        // TODO. set_range
        let obj = client_for_s3
            .get_object()
            .bucket(bucket_name)
            .key(object_name)
            .set_range(range)
            .send()
            .await
            .map_err(|sdk_err| {
                anyhow!(
                    "S3 GetObject from {} error: {}",
                    bucket_name,
                    sdk_err.to_string()
                )
            })?
            .body;
        Ok(obj)
    }
}

#[async_trait]
impl FsSplitReader for S3FileReader {
    type Properties = S3Properties;

    async fn new(props: S3Properties, splits: Vec<FsSplit>) -> Result<Self> {
        let config = AwsConfigV2::from(HashMap::from(props.clone()));
        let sdk_config = config.load_config(None).await;

        let bucket_name = props.bucket_name;
        let s3_client = s3_client(&sdk_config, Some(default_conn_config()));

        let s3_file_reader = S3FileReader {
            split_offset: HashMap::new(),
            bucket_name,
            s3_client,
            splits,
        };

        Ok(s3_file_reader)
    }

    fn into_stream(self) -> BoxFsSourceStream {
        self.into_stream()
    }
}

impl S3FileReader {
    #[try_stream(boxed, ok = Vec<FsSourceMessage>, error = anyhow::Error)]
    pub async fn into_stream(self) {
        let reader_stream = Self::stream_read(self.s3_client, self.bucket_name, self.splits);
        #[for_await]
        for msgs in reader_stream.ready_chunks(MAX_CHUNK_SIZE) {
            let mut res = Vec::with_capacity(msgs.len());
            for msg in msgs {
                let msg = msg?;
                res.push(FsSourceMessage {
                    payload: Some(msg.payload),
                    offset: msg.offset,
                    split_size: msg.size,
                    split_id: msg.obj_name.into(),
                })
            }
            yield res
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::filesystem::{S3Properties, S3SplitEnumerator};
    use crate::source::SplitEnumerator;

    #[tokio::test]
    #[ignore]
    async fn test_read_obj() {
        let props = S3Properties {
            region_name: "ap-southeast-1".to_owned(),
            bucket_name: "mingchao-s3-source".to_owned(),
            match_pattern: None,
            access: None,
            secret: None,
        };
        let mut enumerator = S3SplitEnumerator::new(props.clone()).await.unwrap();
        let splits = enumerator.list_splits().await.unwrap();
        println!("splits {:?}", splits);
        let reader = S3FileReader::new(props, splits).await.unwrap();

        let mut chunk_stream = reader.into_stream();
        while let Some(msgs) = chunk_stream.next().await {
            for msg in msgs.unwrap() {
                println!("obj_name {}, offset {}", msg.split_id, msg.offset);
            }
        }
        // let reader = S3FileReader::new(props, None, None).await.unwrap();
    }
}

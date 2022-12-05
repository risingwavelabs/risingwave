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

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use aws_sdk_s3::client as s3_client;
use aws_smithy_http::byte_stream::ByteStream;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use io::StreamReader;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io;
use tokio_util::io::ReaderStream;
use tracing::{error, info};

use crate::aws_utils::{default_conn_config, s3_client, AwsConfigV2, AwsCredentialV2};
use crate::source::base::{SourceMessage, SplitReader, MAX_CHUNK_SIZE};
use crate::source::filesystem::file_common::EntryStat;
use crate::source::filesystem::s3::s3_dir::FileSystemOptError::IllegalS3FilePath;
use crate::source::filesystem::s3::s3_dir::{
    AwsCustomConfig, S3SourceBasicConfig, S3SourceConfig, SqsReceiveMsgConfig,
};
use crate::source::filesystem::s3::S3Properties;
use crate::source::{BoxSourceStream, Column, ConnectorState, SplitId, SplitMetaData};

const MAX_CHANNEL_BUFFER_SIZE: usize = 2048;
const STREAM_READER_CAPACITY: usize = 4096;

#[derive(Debug, Clone)]
struct S3InnerMessage {
    msg_id: String,
    payload: Bytes,
}

/// `S3File` contains the metadata and the start and end positions that need to be read.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct S3File {
    object: EntryStat,
    start: i64,
    end: Option<i64>,
}

impl Default for S3File {
    fn default() -> Self {
        S3File {
            object: EntryStat::default(),
            start: i64::MIN,
            end: None,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct S3FileSplit {
    bucket: String,
    s3_file: S3File,
}

impl Default for S3FileSplit {
    fn default() -> Self {
        S3FileSplit {
            bucket: "".to_string(),
            s3_file: S3File::default(),
        }
    }
}

impl S3FileSplit {
    // path s3://bucket_name/path
    fn from_path(path_string: String) -> Result<Self> {
        let mut s3_file = S3File::default();
        s3_file.object.path = path_string.clone();
        let path = std::path::Path::new(path_string.as_str());
        let parent_path = path
            .parent()
            .ok_or_else(|| IllegalS3FilePath(path_string.clone()))?;
        Ok(Self {
            bucket: parent_path
                .file_name()
                .unwrap()
                .to_os_string()
                .into_string()
                .unwrap(),
            s3_file,
        })
    }

    fn new(bucket: String, s3_files: S3File) -> Self {
        Self {
            bucket,
            s3_file: s3_files,
        }
    }
}

impl SplitMetaData for S3FileSplit {
    fn id(&self) -> SplitId {
        // TODO: should avoid constructing a string every time
        format!("{}/{}", self.bucket, self.s3_file.object.path).into()
    }

    fn encode_to_bytes(&self) -> Bytes {
        Bytes::from(serde_json::to_string(self).unwrap())
    }

    fn restore_from_bytes(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).map_err(|e| anyhow!(e))
    }
}

#[derive(Debug)]
pub struct S3FileReader {
    client_for_s3: s3_client::Client,
    s3_file_sender: mpsc::UnboundedSender<S3FileSplit>,
    // Considering that there are very many files under a bucket,
    // it is necessary to maintain multiple file offsets in a single S3FileReader .
    split_offset: HashMap<String, u64>,
    s3_receive_stream: ReceiverStream<S3InnerMessage>,
    s3_msg_sender: mpsc::Sender<S3InnerMessage>,
}

impl S3FileReader {
    fn build_from_config(s3_source_config: S3SourceConfig) -> Self {
        let (tx, rx) = mpsc::channel(MAX_CHANNEL_BUFFER_SIZE);
        let (split_s, mut split_r) = mpsc::unbounded_channel();
        let s3_file_reader = S3FileReader {
            client_for_s3: s3_client(&s3_source_config.shared_config, None),
            s3_file_sender: split_s,
            split_offset: HashMap::new(),
            s3_receive_stream: ReceiverStream::from(rx),
            s3_msg_sender: tx.clone(),
        };
        tokio::task::spawn(async move {
            let s3_client = s3_client(&s3_source_config.shared_config, Some(default_conn_config()));
            while let Some(s3_split) = split_r.recv().await {
                _ = S3FileReader::stream_read(s3_client.clone(), s3_split.clone(), tx.clone())
                    .await;
            }
        });
        s3_file_reader
    }

    async fn stream_read(
        client_for_s3: s3_client::Client,
        s3_file_split: S3FileSplit,
        s3_msg_sender: mpsc::Sender<S3InnerMessage>,
    ) -> Result<()> {
        let bucket = s3_file_split.bucket.clone();
        let s3_file = s3_file_split.s3_file.clone();
        let obj_val = S3FileReader::get_object(&client_for_s3, &s3_file, bucket.as_str()).await;
        match obj_val {
            Ok(byte_stream) => {
                let stream_reader = StreamReader::new(
                    byte_stream.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
                );

                let mut reader = Box::pin(BufReader::new(stream_reader));

                let buf_empty = match reader.fill_buf().await {
                    Ok(buffer) => {
                        if buffer.is_empty() {
                            info!(
                                "current s3_file is empty.bucket={}, file_path={}",
                                bucket, s3_file.object.path
                            );
                            Ok(true)
                        } else {
                            Ok(false)
                        }
                    }
                    Err(fill_buf_err) => Err(fill_buf_err),
                };

                match buf_empty {
                    Ok(is_empty) => {
                        if is_empty {
                            return Ok(());
                        }
                    }
                    Err(err) => {
                        return Err(anyhow::Error::from(err));
                    }
                };
                let mut stream = ReaderStream::with_capacity(reader, STREAM_READER_CAPACITY);

                while let Some(result) = stream.next().await {
                    match result {
                        Ok(read_bytes) => {
                            let msg_id = format!(
                                "s3://{}/{}",
                                s3_file_split.bucket, s3_file_split.s3_file.object.path
                            );
                            let s3_inner_msg = S3InnerMessage {
                                msg_id,
                                payload: read_bytes,
                            };
                            if s3_msg_sender.send(s3_inner_msg).await.is_err() {
                                return Err(anyhow::Error::from(crate::source::filesystem::s3::s3_dir::FileSystemOptError::GetS3ObjectError(
                                    bucket,
                                    s3_file.object.path,
                                )));
                            }
                        }
                        Err(read_bytes_err) => {
                            return Err(anyhow::Error::from(read_bytes_err));
                        }
                    }
                }
                Ok(())
            }
            Err(err) => {
                error!("S3FileReader get_object error cause by {:?}", err);
                Err(err)
            }
        }
    }

    async fn get_object(
        client_for_s3: &s3_client::Client,
        s3_file: &S3File,
        bucket: &str,
    ) -> anyhow::Result<ByteStream> {
        let path = s3_file.object.path.as_str();
        let s3_object_key = std::path::Path::new(path)
            .file_name()
            .unwrap()
            .to_os_string()
            .into_string()
            .unwrap();
        // TODO use set_range
        let get_object = client_for_s3
            .get_object()
            .bucket(bucket)
            .key(s3_object_key)
            .send()
            .await;
        match get_object {
            Ok(get_object_out) => Ok(get_object_out.body),
            Err(sdk_err) => Err(anyhow::Error::from(
                crate::source::filesystem::s3::s3_dir::FileSystemOptError::AwsSdkInnerError(
                    format!("S3 GetObject from {} error:", bucket),
                    sdk_err.to_string(),
                ),
            )),
        }
    }
}

#[async_trait]
impl SplitReader for S3FileReader {
    type Properties = S3Properties;

    /// 1. The config include all information about the connection to S3, for example:
    /// `s3.region_name, s3.bucket_name, s3-dd-storage-notify-queue` and the credential's
    /// `access_key` and secret. For now, only static credential is supported.
    /// 2. The identifier of the State is the Path of S3 - <S3://bucket_name/object_key>
    async fn new(
        props: S3Properties,
        _state: ConnectorState,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self> {
        let s3_basic_config = S3SourceBasicConfig::from(props);
        let credential = if s3_basic_config.secret.is_empty() || s3_basic_config.access.is_empty() {
            AwsCredentialV2::None
        } else {
            AwsCredentialV2::Static {
                access_key: s3_basic_config.access.clone(),
                secret_access: s3_basic_config.secret.clone(),
                session_token: None,
            }
        };
        let aws_config = AwsConfigV2 {
            region: None,
            arn: None,
            credential,
            endpoint: None,
        };
        // TODO: should be use external_id parameter.
        let shared_config = aws_config.load_config(None).await;
        let s3_source_config = S3SourceConfig {
            basic_config: s3_basic_config.clone(),
            shared_config,
            custom_config: Some(AwsCustomConfig::default()),
            sqs_config: SqsReceiveMsgConfig::default(),
        };
        let s3_file_reader = S3FileReader::build_from_config(s3_source_config);
        // TODO: new s3 reader with ConnectorState
        Ok(s3_file_reader)
    }

    fn into_stream(self) -> BoxSourceStream {
        self.into_stream()
    }
}

impl S3FileReader {
    #[try_stream(boxed, ok = Vec<SourceMessage>, error = anyhow::Error)]
    async fn into_stream(mut self) {
        #[for_await]
        for msgs in self.s3_receive_stream.ready_chunks(MAX_CHUNK_SIZE) {
            yield msgs
                .into_iter()
                .map(|msg| {
                    let msg_id = msg.msg_id;
                    let new_offset = if !self.split_offset.contains_key(msg_id.as_str()) {
                        1_u64
                    } else {
                        let curr_offset = self.split_offset.get(msg_id.as_str()).unwrap();
                        curr_offset + 1_u64
                    };
                    self.split_offset.insert(msg_id.clone(), new_offset);
                    SourceMessage {
                        payload: Some(msg.payload),
                        offset: new_offset.to_string(),
                        split_id: msg_id.into(),
                    }
                })
                .collect();
        }
    }
}

#[cfg(test)]
mod test {

    use crate::source::filesystem::s3::source::s3_file_reader::S3FileSplit;
    use crate::source::filesystem::s3::S3Properties;

    const TEST_REGION_NAME: &str = "cn-north-1";
    const BUCKET_NAME: &str = "dd-storage-s3";

    const EMPTY_JSON_FILE_NAME: &str = "EMPTY-2022-03-23-03:35:28.json";
    const SMALL_JSON_FILE_NAME: &str = "2022-02-28-09:32:34-example.json";
    const EMPTY_JSON_DATA: &str = r#""#;

    fn test_config_map() -> S3Properties {
        S3Properties {
            region_name: TEST_REGION_NAME.to_string(),
            bucket_name: BUCKET_NAME.to_string(),
            sqs_queue_name: "s3-dd-storage-notify-queue".to_string(),
            match_pattern: None,
            access: "".to_string(),
            secret: "".to_string(),
        }
    }

    fn new_test_s3_file_split(split_str: &str) -> S3FileSplit {
        let s3_file_split: S3FileSplit = serde_json::from_str(split_str).unwrap();
        s3_file_split
    }
}

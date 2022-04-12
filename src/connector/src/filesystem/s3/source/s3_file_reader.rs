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
use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use aws_sdk_s3::client as s3_client;
use aws_smithy_http::byte_stream::ByteStream;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use io::StreamReader;
use itertools::Itertools;
use log::{error, info};
use mpsc::Sender;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io;
use tokio_util::io::ReaderStream;

use crate::base::{InnerMessage, SourceReader, SourceSplit};
use crate::filesystem::file_common::{EntryStat, StatusWatch};
use crate::filesystem::s3::s3_dir::FileSystemOptError::IllegalS3FilePath;
use crate::filesystem::s3::s3_dir::{
    new_s3_client, new_share_config, AwsCredential, AwsCustomConfig, S3SourceBasicConfig,
    S3SourceConfig, SqsReceiveMsgConfig,
};
use crate::ConnectorState;

const MAX_CHANNEL_BUFFER_SIZE: usize = 2048;
const READ_CHUNK_SIZE: usize = 1024;
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


// todo(zhenzhou): better name
const S3_SPLIT_TYPE: &str = "s3";

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
        let parent = path.parent();
        if let Some(parent_path) = parent {
            Ok(Self {
                bucket: parent_path
                    .file_name()
                    .unwrap()
                    .to_os_string()
                    .into_string()
                    .unwrap(),
                s3_file,
            })
        } else {
            Err(IllegalS3FilePath(path_string).into())
        }
    }

    fn new(bucket: String, s3_files: S3File) -> Self {
        Self {
            bucket,
            s3_file: s3_files,
        }
    }
}

impl SourceSplit for S3FileSplit {
    fn id(&self) -> String {
        format!("{}/{}", self.bucket, self.s3_file.object.path)
    }

    fn to_string(&self) -> anyhow::Result<String> {
        let split_str = serde_json::to_string(self);
        if let Ok(split) = split_str {
            Ok(split)
        } else {
            Err(anyhow::Error::from(split_str.err().unwrap()))
        }
    }

    fn restore_from_bytes(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).map_err(|e| anyhow!(e))
    }

    fn get_type(&self) -> String {
        S3_SPLIT_TYPE.to_string()
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
    s3_msg_sender: Sender<S3InnerMessage>,
    stop_signal: Arc<watch::Sender<StatusWatch>>,
}

impl Drop for S3FileReader {
    fn drop(&mut self) {
        self.stop_signal.send(StatusWatch::Stopped).unwrap();
    }
}

impl S3FileReader {
    fn build_from_config(s3_source_config: S3SourceConfig) -> Self {
        let (tx, rx) = mpsc::channel(MAX_CHANNEL_BUFFER_SIZE);
        let (split_s, mut split_r) = mpsc::unbounded_channel();
        let (signal_s, mut signal_r) = watch::channel(StatusWatch::Running);
        let signal_arc = Arc::new(signal_s);
        let s3_file_reader = S3FileReader {
            client_for_s3: new_s3_client(s3_source_config.clone()),
            s3_file_sender: split_s,
            split_offset: HashMap::new(),
            s3_receive_stream: ReceiverStream::from(rx),
            s3_msg_sender: tx.clone(),
            stop_signal: signal_arc.clone(),
        };
        signal_arc.send(StatusWatch::Running).unwrap();
        tokio::task::spawn(async move {
            let s3_client = new_s3_client(s3_source_config.clone());
            loop {
                tokio::select! {
                    s3_split = split_r.recv() => {
                        if let Some(s3_split) = s3_split {
                            let _rs =S3FileReader::stream_read(s3_client.clone(), s3_split.clone(), tx.clone()).await;
                        } else {
                            continue;
                        }
                    }
                    running_status = signal_r.changed() => {
                        if running_status.is_ok() {
                            if let StatusWatch::Stopped = *signal_r.borrow() {
                                break;
                            }
                        }
                        continue;
                    }
                }
            }
        });
        s3_file_reader
    }

    fn add_s3_split(&mut self, state: ConnectorState) -> anyhow::Result<()> {
        let identifier_bytes = state.identifier.to_vec();
        let raw_path = std::str::from_utf8(&identifier_bytes).unwrap();
        let s3_file_split_rs = S3FileSplit::from_path(raw_path.to_string());
        if let Ok(s3_file_split) = s3_file_split_rs {
            self.s3_file_sender.send(s3_file_split).unwrap();
            Ok(())
        } else {
            let err = s3_file_split_rs.err().unwrap();
            error!("S3FileReader add_s3_split error. cause by {:?}", err);
            Err(err)
        }
    }

    async fn stream_read(
        client_for_s3: s3_client::Client,
        s3_file_split: S3FileSplit,
        s3_msg_sender: Sender<S3InnerMessage>,
    ) -> anyhow::Result<()> {
        let bucket = s3_file_split.bucket.clone();
        let s3_file = s3_file_split.s3_file.clone();
        let obj_val =
            S3FileReader::get_object(&client_for_s3, &s3_file.clone(), bucket.clone().as_str())
                .await;
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
                                bucket.clone(),
                                s3_file.clone().object.path
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
                                s3_file_split.clone().bucket.clone(),
                                s3_file_split.clone().s3_file.object.path
                            );
                            let s3_inner_msg = S3InnerMessage {
                                msg_id,
                                payload: read_bytes,
                            };
                            if s3_msg_sender.send(s3_inner_msg).await.is_err() {
                                return Err(anyhow::Error::from(crate::filesystem::s3::s3_dir::FileSystemOptError::GetS3ObjectError(
                                    bucket.clone(),
                                    s3_file.clone().object.path,
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
        let path = s3_file.clone().object.path;
        let s3_object_key = std::path::Path::new(path.as_str())
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
                crate::filesystem::s3::s3_dir::FileSystemOptError::AwsSdkInnerError(
                    format!("S3 GetObject from {} error:", bucket),
                    sdk_err.to_string(),
                ),
            )),
        }
    }
}

#[async_trait]
impl SourceReader for S3FileReader {
    async fn next(&mut self) -> anyhow::Result<Option<Vec<InnerMessage>>> {
        let mut read_chunk = self
            .s3_receive_stream
            .borrow_mut()
            .ready_chunks(READ_CHUNK_SIZE);
        let msg_vec = match read_chunk.next().await {
            None => return Ok(None),
            Some(inner_msg) => inner_msg
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
                    InnerMessage {
                        payload: Some(msg.payload),
                        offset: new_offset.to_string(),
                        split_id: msg_id,
                    }
                })
                .collect_vec(),
        };
        Ok(Some(msg_vec))
    }

    /// 1. The config include all information about the connection to S3, for example:
    /// `s3.region_name, s3.bucket_name, s3-dd-storage-notify-queue` and the credential's access_key
    /// and secret. For now, only static credential is supported.
    /// 2. The identifier of the State is the Path of S3 - S3://bucket_name/object_key
    async fn new(
        config: HashMap<String, String>,
        state: Option<crate::ConnectorState>,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        let s3_basic_config = S3SourceBasicConfig::from(config);
        let credential = if s3_basic_config.secret.is_empty() || s3_basic_config.access.is_empty() {
            AwsCredential::Default
        } else {
            AwsCredential::Static {
                access_key: s3_basic_config.clone().access,
                secret_access: s3_basic_config.clone().secret,
                session_token: None,
            }
        };
        let shared_config_rs = new_share_config(s3_basic_config.clone().region, credential).await;
        if let Ok(config) = shared_config_rs {
            let s3_source_config = S3SourceConfig {
                basic_config: s3_basic_config.clone(),
                shared_config: config,
                custom_config: Some(AwsCustomConfig::default()),
                sqs_config: SqsReceiveMsgConfig::default(),
            };
            let mut s3_file_reader = S3FileReader::build_from_config(s3_source_config);
            if let Some(s3_state) = state {
                if let Err(err) = s3_file_reader.add_s3_split(s3_state) {
                    Err(err)
                } else {
                    Ok(s3_file_reader)
                }
            } else {
                Ok(s3_file_reader)
            }
        } else {
            Err(shared_config_rs.err().unwrap())
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use bytes::Bytes;

    use crate::base::SourceReader;
    use crate::filesystem::s3::source::s3_file_reader::{S3FileReader, S3FileSplit};
    use crate::ConnectorState;

    const TEST_REGION_NAME: &str = "cn-north-1";
    const BUCKET_NAME: &str = "dd-storage-s3";

    const EMPTY_JSON_FILE_NAME: &str = "EMPTY-2022-03-23-03:35:28.json";
    const SMALL_JSON_FILE_NAME: &str = "2022-02-28-09:32:34-example.json";
    const EMPTY_JSON_DATA: &str = r#""#;

    fn test_config_map() -> HashMap<String, String> {
        let config: HashMap<String, String> = vec![
            ("s3.region_name".to_string(), TEST_REGION_NAME.to_string()),
            ("s3.bucket_name".to_string(), BUCKET_NAME.to_string()),
            (
                "sqs_queue_name".to_string(),
                "s3-dd-storage-notify-queue".to_string(),
            ),
        ]
        .into_iter()
        .collect();
        config
    }

    fn new_test_connect_state(file_name: String) -> ConnectorState {
        let path_string = format!("s3://{}/{}", BUCKET_NAME, file_name);
        let identifier = Bytes::copy_from_slice(path_string.as_str().as_bytes());
        ConnectorState {
            identifier,
            start_offset: "".to_string(),
            end_offset: "".to_string(),
        }
    }

    fn new_test_s3_file_split(split_str: &str) -> S3FileSplit {
        let s3_file_split: S3FileSplit = serde_json::from_str(split_str).unwrap();
        s3_file_split
    }

    async fn new_s3_file_reader(file_name: String) -> S3FileReader {
        let s3_file_reader =
            S3FileReader::new(test_config_map(), Some(new_test_connect_state(file_name)));
        s3_file_reader.await.unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[ignore]
    async fn test_s3_file_reader() {
        let mut s3_file_reader =
            new_s3_file_reader("2022-02-28-09:32:34-example.json".to_string()).await;
        let msg_rs = s3_file_reader.next().await;
        assert!(msg_rs.is_ok());
        println!("S3FileReader next() msg = {:?}", msg_rs.unwrap());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore]
    async fn test_empty_s3_file_reader() {
        println!("S3FileReader read empty json file.");
        let mut s3_file_reader =
            new_s3_file_reader("EMPTY-2022-03-23-03:35:28.json".to_string()).await;
        let task_join_handler = tokio::task::spawn(async move {
            tokio::select! {
                _=  tokio::time::sleep(tokio::time::Duration::from_secs(5))=> {
                    println!("S3FileReader wait 5s for next message");
                }
                _= s3_file_reader.next() => {
                    unreachable!()
                }
            }
        });
        let join_rs = task_join_handler.await;
        assert!(join_rs.is_ok());
    }
}

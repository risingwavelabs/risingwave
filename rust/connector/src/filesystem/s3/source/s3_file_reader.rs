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
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io;
use tokio_util::io::ReaderStream;

use crate::base::{InnerMessage, SourceReader, SourceSplit};
use crate::filesystem::file_common::EntryStat;
use crate::filesystem::s3::s3_dir::FileSystemOptError::{AwsSdkInnerError, GetS3ObjectError};
use crate::filesystem::s3::s3_dir::{new_s3_client, S3SourceConfig};

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
}

#[derive(Debug)]
pub struct S3FileReader {
    client_for_s3: s3_client::Client,
    s3_split_tx: mpsc::UnboundedSender<S3FileSplit>,
    // Considering that there are very many files under a bucket,
    // it is necessary to maintain multiple file offsets in a single S3FileReader .
    split_offset: HashMap<String, u64>,
    s3_receive_stream: ReceiverStream<S3InnerMessage>,
    s3_msg_sender: Sender<S3InnerMessage>,
}

impl S3FileReader {
    fn new(s3_source_config: S3SourceConfig) -> Self {
        let (tx, rx) = mpsc::channel(MAX_CHANNEL_BUFFER_SIZE);
        let (split_s, mut split_r) = mpsc::unbounded_channel();
        let s3_file_reader = S3FileReader {
            client_for_s3: new_s3_client(s3_source_config.clone()),
            s3_split_tx: split_s,
            split_offset: HashMap::new(),
            s3_receive_stream: ReceiverStream::from(rx),
            s3_msg_sender: tx.clone(),
        };
        tokio::task::spawn(async move {
            let s3_client = new_s3_client(s3_source_config.clone());
            while let Some(s3_split) = split_r.recv().await {
                let _rs =
                    S3FileReader::stream_read(s3_client.clone(), s3_split.clone(), tx.clone())
                        .await;
            }
        });
        s3_file_reader
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
                                return Err(anyhow::Error::from(GetS3ObjectError(
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
            .set_range(Some(format!("bytes={}-{}", 0, 10)))
            .send()
            .await;
        match get_object {
            Ok(get_object_out) => Ok(get_object_out.body),
            Err(sdk_err) => Err(anyhow::Error::from(AwsSdkInnerError(
                format!("S3 GetObject from {} error:", bucket),
                sdk_err.to_string(),
            ))),
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
                    // let split_key = msg.message_id();
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

    async fn assign_split<'a>(&'a mut self, split: &'a [u8]) -> anyhow::Result<()> {
        let split_json_str = std::str::from_utf8(split);
        match split_json_str {
            Err(convert_err) => Err(anyhow::Error::from(convert_err)),
            Ok(json_str) => {
                let s3_split_rs: Result<S3FileSplit, serde_json::Error> =
                    serde_json::from_str(json_str);
                if let Err(serde_err) = s3_split_rs {
                    Err(anyhow::Error::from(serde_err))
                } else {
                    let s3_file_split = s3_split_rs.unwrap();
                    println!("S3FileReader assign_split success. {:?}", s3_file_split);
                    self.s3_split_tx.send(s3_file_split).unwrap();
                    Ok(())
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::base::SourceReader;
    use crate::filesystem::s3::s3_dir::test::new_s3_source_config;
    use crate::filesystem::s3::s3_dir::{new_share_config, AwsCredential};
    use crate::filesystem::s3::source::s3_file_reader::{S3FileReader, S3FileSplit};

    const TEST_REGION_NAME: &str = "cn-north-1";
    const BUCKET_NAME: &str = "dd-storage-s3";

    const EMPTY_JSON_DATA: &str = r#""#;

    const S3_ARRAY_FILE_JSON_STR: &str = r#"[
	{
		color: "red",
		value: "f00"
	},
	{
		color: "green",
		value: "0f0"
	},
	{
		color: "blue",
		value: "00f"
	},
	{
		color: "cyan",
		value: "0ff"
	},
	{
		color: "magenta",
		value: "f0f"
	}
    ]"#;

    fn new_s3_file_split_form_str(path: String) -> String {
        format!(
            r#"{{
            "bucket": "dd-storage-s3",
            "s3_file": {{
               "object": {{
                 "path": "{}",
                 "atime": 0,
                 "time": 0,
                 "mtime": 0,
                 "size": 11
               }},
               "start": 0,
               "end": 1
            }}
        }}"#,
            path
        )
    }

    fn new_test_s3_file_split(split_str: &str) -> S3FileSplit {
        let s3_file_split: S3FileSplit = serde_json::from_str(split_str).unwrap();
        s3_file_split
    }

    async fn new_s3_file_reader() -> S3FileReader {
        let shared_config = new_share_config(TEST_REGION_NAME.to_string(), AwsCredential::Default)
            .await
            .unwrap();
        let s3_source_config = new_s3_source_config(shared_config);
        S3FileReader::new(s3_source_config)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore]
    async fn test_s3_file_reader() {
        let split_str = new_s3_file_split_form_str("2022-02-28-09:32:34-example.json".to_string());

        let mut s3_file_reader = create_and_assign_split(split_str.as_str()).await;
        let msg_rs = s3_file_reader.next().await;
        assert!(msg_rs.is_ok());
        println!("S3FileReader next() msg = {:?}", msg_rs.unwrap());
    }

    async fn create_and_assign_split(split_str: &str) -> S3FileReader {
        let mut s3_file_reader = new_s3_file_reader().await;
        let test_s3_file_split = new_test_s3_file_split(split_str);

        let s3_file_split_string = serde_json::to_string(&test_s3_file_split).unwrap();
        let assign_rs = s3_file_reader
            .assign_split(s3_file_split_string.as_bytes())
            .await;
        assert!(assign_rs.is_ok());
        s3_file_reader
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore]
    async fn test_empty_s3_file_reader() {
        println!("S3FileReader read empty json file.");
        let empty_split = new_s3_file_split_form_str("EMPTY-2022-03-23-03:35:28.json".to_string());
        let mut s3_file_reader = create_and_assign_split(empty_split.as_str()).await;
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

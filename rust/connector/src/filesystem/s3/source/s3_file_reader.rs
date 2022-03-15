use std::borrow::BorrowMut;
use std::collections::HashMap;

use async_trait::async_trait;
use aws_sdk_s3::client as s3_client;
use aws_smithy_http::byte_stream::ByteStream;
use bytes::{Buf, Bytes};
use futures::{StreamExt, TryStreamExt};
use io::StreamReader;
use itertools::Itertools;
use log::info;
use mpsc::Sender;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io;
use tokio_util::io::ReaderStream;

use crate::base::{SourceMessage, SourceReader, SourceSplit};
use crate::filesystem::file_common::EntryStat;
use crate::filesystem::s3::s3_dir::FileSystemOptError::{AwsSdkInnerError, GetS3ObjectError};
use crate::filesystem::s3::s3_dir::{new_s3_client, S3SourceConfig};

const MAX_CHANNEL_BUFFER_SIZE: usize = 2048;
const READ_CHUNK_SIZE: usize = 1024;
const STREAM_READER_CAPACITY: usize = 4096;

#[derive(Debug, Clone)]
struct S3InnerMessage {
    bucket: String,
    s3_file: S3File,
    payload: Bytes,
}

impl S3InnerMessage {
    fn message_id(&self) -> String {
        format!("s3://{}/{}", self.bucket, self.s3_file.object.path)
    }
}

#[derive(Debug, Clone)]
pub struct S3FileMessage {
    bucket: String,
    file_name: String,
    offset: u64,
    msg_payload: Option<Bytes>,
}

impl SourceMessage for S3FileMessage {
    fn payload(&self) -> anyhow::Result<Option<&[u8]>> {
        Ok(self.msg_payload.as_ref().map(|msg| msg.chunk()))
    }
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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
}

#[derive(Debug)]
pub struct S3FileReader {
    client_for_s3: s3_client::Client,
    s3_split_tx: mpsc::UnboundedSender<S3FileSplit>,
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
                            println!("S3FileReader read file content none empty.");
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
                            let s3_inner_msg = S3InnerMessage {
                                bucket: s3_file_split.clone().bucket.clone(),
                                s3_file: s3_file_split.clone().s3_file.clone(),
                                payload: read_bytes,
                            };
                            println!("S3FileReader s3_inner_msg={:?}", s3_inner_msg.clone());
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
            Err(err) => anyhow::private::Err(err),
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
        println!(
            "S3FileReader get_object for path={:?},object_key={:?}",
            path.clone(),
            s3_object_key
        );
        // TODO use set_range
        let get_object = client_for_s3
            .get_object()
            .bucket(bucket)
            .key(s3_object_key)
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
    type Message = S3FileMessage;
    type Split = S3FileSplit;

    async fn next(&mut self) -> anyhow::Result<Option<Vec<Self::Message>>> {
        let mut read_chunk = self
            .s3_receive_stream
            .borrow_mut()
            .ready_chunks(READ_CHUNK_SIZE);
        let msg_vec = match read_chunk.next().await {
            None => return Ok(None),
            Some(inner_msg) => inner_msg
                .into_iter()
                .map(|msg| {
                    let split_key = msg.message_id();
                    let new_offset = if !self.split_offset.contains_key(split_key.as_str()) {
                        1_u64
                    } else {
                        let curr_offset = self.split_offset.get(split_key.as_str()).unwrap();
                        curr_offset + 1_u64
                    };
                    self.split_offset.insert(split_key, new_offset);
                    S3FileMessage {
                        bucket: msg.bucket,
                        file_name: msg.s3_file.object.path,
                        offset: new_offset,
                        msg_payload: Some(msg.payload),
                    }
                })
                .collect_vec(),
        };
        Ok(Some(msg_vec))
    }

    async fn assign_split(&mut self, split: Self::Split) -> anyhow::Result<()> {
        self.s3_split_tx.send(split).unwrap();
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::base::SourceReader;
    use crate::filesystem::s3::s3_dir::test::new_s3_source_config;
    use crate::filesystem::s3::s3_dir::{new_share_config, AwsCredential};
    use crate::filesystem::s3::source::s3_file_reader::S3FileReader;

    const TEST_REGION_NAME: &str = "cn-north-1";
    const BUCKET_NAME: &str = "dd-storage-s3";

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_s3_file_reader() {
        let shared_config = new_share_config(TEST_REGION_NAME.to_string(), AwsCredential::Default)
            .await
            .unwrap();
        let s3_source_config = new_s3_source_config(shared_config);
        let s3_file_reader = S3FileReader::new(s3_source_config);
    }
}

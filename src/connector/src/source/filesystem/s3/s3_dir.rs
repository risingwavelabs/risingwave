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
use std::time::Duration;

use async_trait::async_trait;
use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_config::timeout::Http;
use aws_sdk_s3::{client as s3_client, config as s3_config};
use aws_sdk_sqs::client as sqs_client;
use aws_smithy_types::tristate::TriState;
use aws_types::credentials::SharedCredentialsProvider;
use aws_types::region::Region;
use sync::watch;
use thiserror::Error;
use tokio::sync;
use tokio::sync::mpsc::Sender;
use tracing::{debug, error, info};

use crate::source::filesystem::file_common::{
    Directory, EntryDiscover, EntryOpt, EntryOptEvent, EntryStat, StatusWatch,
};
use crate::source::filesystem::s3::s3_notification_event::{NotificationEvent, NotifyEventType};
use crate::source::filesystem::s3::S3Properties;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum AwsCredential {
    Default,
    Static {
        access_key: String,
        secret_access: String,
        session_token: Option<String>,
    },
}

#[derive(Error, Debug)]
pub enum FileSystemOptError {
    #[error("AWS {0} SDK  Error {1}.")]
    AwsSdkInnerError(String, String),
    #[error("S3 Bucket {0} download object {1} Error.")]
    GetS3ObjectError(String, String),
    #[error("S3FileSplit illegal. must not be null")]
    S3SplitIsEmpty,
    #[error("Illegal Path format {0}, S3 File path format is s3://bucket_name/object_key")]
    IllegalS3FilePath(String),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AwsCustomConfig {
    conn_time_out: Duration,
    read_time_out: Duration,
    max_retry_times: u32,
}

impl Default for AwsCustomConfig {
    fn default() -> Self {
        AwsCustomConfig {
            conn_time_out: Duration::from_secs(5),
            read_time_out: Duration::from_secs(5),
            max_retry_times: 5_u32,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SqsReceiveMsgConfig {
    max_number_of_message: i32,
    visibility_timeout: i32,
    wait_timeout: i32,
}

impl Default for SqsReceiveMsgConfig {
    fn default() -> Self {
        SqsReceiveMsgConfig {
            max_number_of_message: 10_i32,
            visibility_timeout: 500_i32,
            wait_timeout: 20_i32,
        }
    }
}

fn find_prefix(match_pattern: &str) -> String {
    let mut escaped = false;
    let mut escaped_filter = false;
    match_pattern
        .chars()
        .take_while(|c| match (c, &escaped) {
            ('*', false) => false,
            ('[', false) => false, // a character class is a form of glob
            ('{', false) => false, // a group class is a form of glob
            ('\\', false) => {
                escaped = true;
                true
            }
            (_, false) => true,
            (_, true) => {
                escaped = false;
                true
            }
        })
        .filter(|c| match (c, &escaped_filter) {
            (_, true) => {
                escaped_filter = false;
                true
            }
            (_, _) => true,
        })
        .collect()
}

pub async fn new_share_config(
    region: String,
    credential: AwsCredential,
) -> anyhow::Result<aws_types::SdkConfig> {
    let region_obj = Some(Region::new(region));
    let credential_provider = match credential {
        AwsCredential::Default => SharedCredentialsProvider::new(
            DefaultCredentialsChain::builder()
                .region(region_obj.clone())
                .build()
                .await,
        ),
        AwsCredential::Static {
            access_key,
            secret_access,
            session_token,
        } => SharedCredentialsProvider::new(aws_types::Credentials::from_keys(
            &access_key,
            &secret_access,
            session_token,
        )),
    };
    let shared_config_loader = aws_config::from_env()
        .region(region_obj)
        .credentials_provider(credential_provider);

    Ok(shared_config_loader.load().await)
}

#[derive(Debug, Clone, Default)]
pub struct S3SourceBasicConfig {
    pub(crate) bucket: String,
    pub(crate) sqs_queue_name: String,
    pub(crate) access: String,
    pub(crate) secret: String,
    pub(crate) region: String,
    pub(crate) match_pattern: Option<String>,
}

#[derive(Debug, Clone)]
pub struct S3SourceConfig {
    pub(crate) basic_config: S3SourceBasicConfig,
    pub(crate) shared_config: aws_types::SdkConfig,
    pub(crate) custom_config: Option<AwsCustomConfig>,
    pub(crate) sqs_config: SqsReceiveMsgConfig,
}

impl From<S3Properties> for S3SourceBasicConfig {
    fn from(props: S3Properties) -> Self {
        S3SourceBasicConfig {
            bucket: props.bucket_name,
            sqs_queue_name: props.sqs_queue_name,
            access: props.access,
            secret: props.secret,
            region: props.region_name,
            match_pattern: props.match_pattern,
        }
    }
}

pub(crate) fn new_s3_client(s3_source_config: S3SourceConfig) -> s3_client::Client {
    let config_for_s3 = match s3_source_config.custom_config {
        Some(conf) => {
            let retry_conf = aws_config::RetryConfig::standard().with_max_attempts(conf.max_retry_times);
            let timeout_conf = aws_config::timeout::Config::new().with_http_timeouts(
                Http::new()
                    .with_connect_timeout(TriState::Set(conf.conn_time_out))
                    .with_read_timeout(TriState::Set(conf.read_time_out)),
            );

            s3_config::Builder::from(&s3_source_config.shared_config)
                .retry_config(retry_conf)
                .timeout_config(timeout_conf)
                .build()
        }
        None => s3_config::Config::new(&s3_source_config.shared_config),
    };
    s3_client::Client::from_conf(config_for_s3)
}

#[derive(Debug, Clone)]
pub struct S3Directory {
    source_config: S3SourceConfig,
    client_for_s3: s3_client::Client,
    client_for_sqs: sqs_client::Client,
}

impl S3Directory {
    pub fn is_match(&self, s3_object_key: String) -> anyhow::Result<bool> {
        let match_string_option = self.source_config.clone().basic_config.match_pattern;
        if let Some(match_string) = match_string_option {
            let glob = globset::Glob::new(match_string.as_str());
            if let Ok(matcher) = glob {
                let glob_matcher = matcher.compile_matcher();
                Ok(glob_matcher.is_match(s3_object_key.as_str()))
            } else {
                Err(anyhow::Error::from(glob.err().unwrap()))
            }
        } else {
            Ok(true)
        }
    }

    pub fn new(s3_source_config: S3SourceConfig) -> Self {
        let client_for_s3 = new_s3_client(s3_source_config.clone());
        let client_for_sqs = aws_sdk_sqs::Client::new(&s3_source_config.shared_config);
        Self {
            source_config: s3_source_config,
            client_for_s3,
            client_for_sqs,
        }
    }

    /// Receive SQS messages and serialize. Current only support `ObjectObjectXXX` Event. (append
    /// only) [use aws sqs long running](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html)
    pub async fn sqs_recv_msg_post(
        &self,
        queue_url: &str,
        sender: &Sender<EntryOptEvent>,
        sqs_msg: aws_sdk_sqs::model::Message,
        sqs_client: &sqs_client::Client,
    ) -> anyhow::Result<()> {
        let msg_body = sqs_msg.body.as_ref();
        match msg_body {
            Some(body) => {
                let notification_event: Result<NotificationEvent, _> = serde_json::from_str(body);
                match notification_event {
                    Ok(event) => {
                        for msg in event.records {
                            println!("Receive SQS Msg={:?}", msg);
                            if matches!(
                                msg.event_type,
                                NotifyEventType::ObjectCreatedPut
                                    | NotifyEventType::ObjectCreatedPost
                                    | NotifyEventType::ObjectCreatedCompleteMultipartUpload
                            ) {
                                debug!("current event_type = {:?} match", msg.event_type);
                                let s3_object_key = msg.s3.object.key;
                                let path =
                                    format!("{}/{}", msg.s3.bucket.name, s3_object_key.clone());
                                let date_time =
                                    chrono::DateTime::parse_from_rfc3339(msg.event_time.as_str())
                                        .unwrap();
                                let entry_stat = EntryStat {
                                    path,
                                    atime: 0,
                                    mtime: date_time.timestamp_millis(),
                                    size: msg.s3.object.size as i64,
                                };
                                if self.is_match(s3_object_key).unwrap() {
                                    let send_rs = sender
                                        .send(EntryOptEvent {
                                            entry_operation: EntryOpt::Add,
                                            entry: entry_stat,
                                        })
                                        .await;
                                    if send_rs.is_err() {
                                        return Err(anyhow::Error::from(send_rs.err().unwrap()));
                                    } else {
                                        let del_msg_rs = sqs_client
                                            .delete_message()
                                            .queue_url(queue_url)
                                            .receipt_handle(
                                                sqs_msg
                                                    .receipt_handle
                                                    .as_ref()
                                                    .expect("always return"),
                                            )
                                            .send()
                                            .await;
                                        if del_msg_rs.is_err() {
                                            error!(
                                                "sqs_recv_msg_post del sqs msg error. {:?}",
                                                del_msg_rs
                                            );
                                            return Err(anyhow::Error::from(
                                                del_msg_rs.err().unwrap(),
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(err) => {
                        error!("S3Directory receive sqs message error cause by {:?}", err);
                        return Err(anyhow::Error::from(err));
                    }
                }
            }
            None => return Ok(()),
        };
        Ok(())
    }
}

#[async_trait]
impl Directory for S3Directory {
    /// Send the added S3 File to the specified channel sender,
    /// if the ```DirectoryChangeWatch::Stopped``` event method will exit.
    async fn push_entries_change(
        &self,
        sender: Sender<EntryOptEvent>,
        mut running_status: watch::Receiver<StatusWatch>,
    ) -> anyhow::Result<()> {
        let sqs_config = self.source_config.clone().sqs_config;
        let sqs_queue_url_rs = match self
            .client_for_sqs
            .get_queue_url()
            .queue_name(self.source_config.clone().basic_config.sqs_queue_name)
            .send()
            .await
        {
            Ok(rsp) => {
                if let Some(url_value) = rsp.queue_url {
                    Ok(url_value)
                } else {
                    Err(FileSystemOptError::AwsSdkInnerError(
                        "SQS".to_string(),
                        "not found error sdk".to_string(),
                    ))
                }
            }
            Err(e) => Err(FileSystemOptError::AwsSdkInnerError(
                "SQSGetUrl".to_string(),
                e.to_string(),
            )),
        };

        let sqs_queue_url = match sqs_queue_url_rs {
            Ok(queue) => queue,
            Err(err) => {
                return Err(err.into());
            }
        };
        loop {
            let rsp_future = self
                .client_for_sqs
                .receive_message()
                .max_number_of_messages(sqs_config.max_number_of_message)
                .queue_url(&sqs_queue_url)
                .visibility_timeout(sqs_config.visibility_timeout)
                .wait_time_seconds(sqs_config.wait_timeout)
                .send();

            let response = tokio::select! {
                response = rsp_future => response,
                status = running_status.changed() => {
                    if status.is_ok() {
                        if let StatusWatch::Stopped = *running_status.borrow() {
                            println!("sqs_recv_msg_post() send change complete");
                            // info!("");
                            return Ok(());
                        }
                    }
                    continue;
                }
            };
            // use long poll running receive message
            match response {
                Ok(rsp_msg) => {
                    if let Some(msg_vec) = rsp_msg.messages {
                        if sender.is_closed() {
                            info!("Sender is Closed. loop break ");
                            return Ok(());
                        } else {
                            let msg_iter = msg_vec.into_iter();
                            for msg in msg_iter {
                                let process_msg_rs = self
                                    .sqs_recv_msg_post(
                                        sqs_queue_url.as_str(),
                                        &sender,
                                        msg,
                                        &self.client_for_sqs,
                                    )
                                    .await;
                                if process_msg_rs.is_err() {
                                    error!("S3Directory process_msg error");
                                    return Err(process_msg_rs.err().unwrap());
                                }
                            }
                        }
                    } else {
                        info!("not receive sqs message. next around");
                        continue;
                    }
                }
                Err(e) => {
                    error!("sqs response message error");
                    return Err(e.into());
                }
            }
        }
    }

    async fn list_entries(&self) -> anyhow::Result<Vec<EntryStat>> {
        let prefix_string =
            if let Some(match_string) = self.source_config.clone().basic_config.match_pattern {
                let glob = globset::Glob::new(match_string.as_str());
                if let Ok(glob_obj) = glob {
                    Ok(find_prefix(glob_obj.compile_matcher().glob().glob()))
                } else {
                    Err(glob.err().unwrap())
                }
            } else {
                Ok("".to_string())
            };

        match prefix_string {
            Ok(prefix) => {
                let obj_keys_rsp = self
                    .client_for_s3
                    .list_objects_v2()
                    .bucket(self.source_config.clone().basic_config.bucket)
                    .prefix(prefix)
                    .send()
                    .await;
                if let Ok(list_obj_output) = obj_keys_rsp {
                    let key_obj_vec = list_obj_output.contents;
                    match key_obj_vec {
                        Some(vec) => {
                            let mut entry_stat_vec = Vec::new();
                            for v in vec {
                                let mut entry_stat = EntryStat::default();
                                let path = format!(
                                    "s3://{}/{}",
                                    self.source_config.basic_config.bucket,
                                    v.key.unwrap()
                                );
                                entry_stat.path = path;
                                entry_stat.size = v.size;
                                if let Some(mtime) = v.last_modified {
                                    entry_stat.mtime = mtime.secs();
                                }
                                entry_stat_vec.push(entry_stat);
                            }
                            Ok(entry_stat_vec)
                        }
                        None => Ok(Vec::default()),
                    }
                } else {
                    Err(anyhow::Error::from(obj_keys_rsp.err().unwrap()))
                }
            }
            Err(err) => Err(anyhow::Error::from(err)),
        }
    }

    fn entry_discover(&self) -> EntryDiscover {
        EntryDiscover::Auto
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::sync::Arc;

    use aws_smithy_http::byte_stream::ByteStream;
    use chrono::Utc;

    use crate::source::filesystem::file_common::{Directory, StatusWatch};
    use crate::source::filesystem::s3::s3_dir::{
        new_share_config, AwsCredential, S3Directory, S3SourceBasicConfig, S3SourceConfig,
        SqsReceiveMsgConfig,
    };

    pub const TEST_REGION_NAME: &str = "cn-north-1";
    pub const BUCKET_NAME: &str = "dd-storage-s3";
    const JSON_DATA: &str = r#"
        {
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#;

    async fn new_aws_s3_client() -> aws_sdk_s3::client::Client {
        let shared_config = new_share_config(TEST_REGION_NAME.to_string(), AwsCredential::Default)
            .await
            .unwrap();
        let s3_config = aws_sdk_s3::config::Config::new(&shared_config);
        aws_sdk_s3::client::Client::from_conf(s3_config)
    }

    pub async fn upload_json_file_test(data: &str) {
        let input_stream = ByteStream::from(data.as_bytes().to_vec());
        let bucket_key = Utc::now().format("%Y-%m-%d-%H:%M:%S").to_string();
        let s3_client = new_aws_s3_client().await;
        let rs = s3_client
            .put_object()
            .bucket(BUCKET_NAME.to_string())
            .key(format!("{}-example.json", bucket_key))
            .body(input_stream)
            .send()
            .await;
        assert!(rs.is_ok());
        println!("put_object complete");
    }

    pub fn new_s3_source_config(shared_config: aws_types::SdkConfig) -> S3SourceConfig {
        let basic_config = S3SourceBasicConfig {
            sqs_queue_name: "s3-dd-storage-notify-queue".to_string(),
            bucket: BUCKET_NAME.to_string(),
            region: TEST_REGION_NAME.to_string(),
            ..Default::default()
        };
        S3SourceConfig {
            basic_config,
            shared_config,
            custom_config: None,
            sqs_config: SqsReceiveMsgConfig::default(),
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_list_curr_entries() {
        let shared_config = new_share_config(TEST_REGION_NAME.to_string(), AwsCredential::Default)
            .await
            .unwrap();
        let s3_source_config = new_s3_source_config(shared_config);
        let s3_directory = S3Directory::new(s3_source_config);
        let curr_entries = s3_directory.list_entries().await.unwrap();
        println!("list_curr_entries = {:?}", curr_entries);
    }

    #[tokio::test]
    #[ignore]
    async fn test_s3_directory_entries_change() {
        let shared_config = new_share_config(TEST_REGION_NAME.to_string(), AwsCredential::Default)
            .await
            .unwrap();
        let s3_source_config = new_s3_source_config(shared_config);
        let s3_directory = S3Directory::new(s3_source_config);
        let s3_dir_for_task = Arc::new(s3_directory);
        let (s_tx, s_rx) = tokio::sync::watch::channel(StatusWatch::Stopped);
        let s_send_rs = s_tx.send(StatusWatch::Running);
        assert!(s_send_rs.is_ok());
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        let change_join = tokio::task::spawn(async move {
            let rs = s3_dir_for_task.push_entries_change(tx, s_rx).await;
            assert!(rs.is_ok());
        });
        let status_control_join = tokio::task::spawn(async move {
            upload_json_file_test(JSON_DATA).await;
            let entry_event = rx.recv().await;
            println!(
                "receive S3 DirectoryEntryOptEvent={:?}",
                entry_event.unwrap()
            );
            let send_rs = s_tx.send(StatusWatch::Stopped);
            assert!(send_rs.is_ok());
            println!("send dir watch stopped complete");
        });
        let rs1 = change_join.await;
        let rs2 = status_control_join.await;
        assert!(rs1.is_ok());
        assert!(rs2.is_ok());
    }
}

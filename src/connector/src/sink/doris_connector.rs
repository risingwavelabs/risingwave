// Copyright 2023 RisingWave Labs
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

use core::mem;
use core::time::Duration;
use std::collections::HashMap;

use base64::engine::general_purpose;
use base64::Engine;
use bytes::{BufMut, Bytes, BytesMut};
use http::request::Builder;
use hyper::body::{Body, Sender};
use hyper::client::HttpConnector;
use hyper::{body, Client, Request, StatusCode};
use hyper_tls::HttpsConnector;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::task::JoinHandle;

use super::{Result, SinkError};

const BUFFER_SIZE: usize = 64 * 1024;
const MIN_CHUNK_SIZE: usize = BUFFER_SIZE - 1024;
const DORIS_SUCCESS_STATUS: [&str; 2] = ["Success", "Publish Timeout"];
pub(crate) const DORIS_DELETE_SIGN: &str = "__DORIS_DELETE_SIGN__";
const SEND_CHUNK_TIMEOUT: Duration = Duration::from_secs(10);
const WAIT_HANDDLE_TIMEOUT: Duration = Duration::from_secs(10);
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
pub struct DorisInsertClient {
    url: String,
    header: HashMap<String, String>,
    sender: Option<Sender>,
}
impl DorisInsertClient {
    pub fn new(url: String, db: String, table: String) -> Self {
        let url = format!("{}/api/{}/{}/_stream_load", url, db, table);
        Self {
            url,
            header: HashMap::default(),
            sender: None,
        }
    }

    pub fn set_url(mut self, url: String) -> Self {
        self.url = url;
        self
    }

    pub fn add_common_header(mut self) -> Self {
        self.header
            .insert("expect".to_string(), "100-continue".to_string());
        self
    }

    /// The method is temporarily not in use, reserved for later use in 2PC.
    /// Doris will generate a default, non-repeating label.
    pub fn set_label(mut self, label: String) -> Self {
        self.header.insert("label".to_string(), label);
        self
    }

    /// This method is only called during upsert operations.
    pub fn add_hidden_column(mut self) -> Self {
        self.header
            .insert("hidden_columns".to_string(), DORIS_DELETE_SIGN.to_string());
        self
    }

    /// The method is temporarily not in use, reserved for later use in 2PC.
    pub fn enable_2_pc(mut self) -> Self {
        self.header
            .insert("two_phase_commit".to_string(), "true".to_string());
        self
    }

    pub fn set_user_password(mut self, user: String, password: String) -> Self {
        let auth = format!(
            "Basic {}",
            general_purpose::STANDARD.encode(format!("{}:{}", user, password))
        );
        self.header.insert("Authorization".to_string(), auth);
        self
    }

    /// The method is temporarily not in use, reserved for later use in 2PC.
    pub fn set_txn_id(mut self, txn_id: i64) -> Self {
        self.header
            .insert("txn_operation".to_string(), txn_id.to_string());
        self
    }

    /// The method is temporarily not in use, reserved for later use in 2PC.
    pub fn add_commit(mut self) -> Self {
        self.header
            .insert("txn_operation".to_string(), "commit".to_string());
        self
    }

    /// The method is temporarily not in use, reserved for later use in 2PC.
    pub fn add_abort(mut self) -> Self {
        self.header
            .insert("txn_operation".to_string(), "abort".to_string());
        self
    }

    /// This method is used to add custom message headers, such as the data import format.
    pub fn set_properties(mut self, properties: HashMap<String, String>) -> Self {
        self.header.extend(properties);
        self
    }

    fn build_request_and_client(
        &self,
        uri: String,
    ) -> (Builder, Client<HttpsConnector<HttpConnector>>) {
        let mut builder = Request::put(uri);
        for (k, v) in &self.header {
            builder = builder.header(k, v);
        }

        let connector = HttpsConnector::new();
        let client = Client::builder()
            .pool_idle_timeout(POOL_IDLE_TIMEOUT)
            .build(connector);

        (builder, client)
    }

    pub async fn build(&mut self) -> Result<DorisInsert> {
        let (builder, client) = self.build_request_and_client(self.url.clone());

        let request_get_url = builder
            .body(Body::empty())
            .map_err(|err| SinkError::Http(err.into()))?;
        let resp = client
            .request(request_get_url)
            .await
            .map_err(|err| SinkError::Http(err.into()))?;
        let be_url = if resp.status() == StatusCode::TEMPORARY_REDIRECT {
            resp.headers()
                .get("location")
                .ok_or_else(|| {
                    SinkError::Http(anyhow::anyhow!("Can't get doris BE url in header",))
                })?
                .to_str()
                .map_err(|err| {
                    SinkError::Http(anyhow::anyhow!(
                        "Can't get doris BE url in header {:?}",
                        err
                    ))
                })?
        } else {
            return Err(SinkError::Http(anyhow::anyhow!("Can't get doris BE url",)));
        };

        let (builder, client) = self.build_request_and_client(be_url.to_string());
        let (sender, body) = Body::channel();
        let request = builder
            .body(body)
            .map_err(|err| SinkError::Http(err.into()))?;
        let feature = client.request(request);

        let handle: JoinHandle<Result<DorisInsertResultResponse>> = tokio::spawn(async move {
            let response = feature.await.map_err(|err| SinkError::Http(err.into()))?;
            let status = response.status();
            let raw_string = String::from_utf8(
                body::to_bytes(response.into_body())
                    .await
                    .map_err(|err| SinkError::Http(err.into()))?
                    .to_vec(),
            )
            .map_err(|err| SinkError::Http(err.into()))?;

            if status == StatusCode::OK && !raw_string.is_empty() {
                let response: DorisInsertResultResponse =
                    serde_json::from_str(&raw_string).map_err(|err| SinkError::Http(err.into()))?;
                Ok(response)
            } else {
                Err(SinkError::Http(anyhow::anyhow!(
                    "Failed connection {:?},{:?}",
                    status,
                    raw_string
                )))
            }
        });

        Ok(DorisInsert::new(sender, handle))
    }
}

pub struct DorisInsert {
    sender: Option<Sender>,
    join_handle: Option<JoinHandle<Result<DorisInsertResultResponse>>>,
    buffer: BytesMut,
    is_first_record: bool,
}
impl DorisInsert {
    pub fn new(sender: Sender, join_handle: JoinHandle<Result<DorisInsertResultResponse>>) -> Self {
        Self {
            sender: Some(sender),
            join_handle: Some(join_handle),
            buffer: BytesMut::with_capacity(BUFFER_SIZE),
            is_first_record: true,
        }
    }

    async fn send_chunk(&mut self) -> Result<()> {
        if self.sender.is_none() {
            return Ok(());
        }

        let chunk = mem::replace(&mut self.buffer, BytesMut::with_capacity(BUFFER_SIZE));

        let is_timed_out = match tokio::time::timeout(
            SEND_CHUNK_TIMEOUT,
            self.sender.as_mut().unwrap().send_data(chunk.into()),
        )
        .await
        {
            Ok(Ok(_)) => return Ok(()),
            Ok(Err(_)) => false,
            Err(_) => true,
        };
        self.abort()?;

        let res = self.wait_handle().await;

        if is_timed_out {
            Err(SinkError::Http(anyhow::anyhow!("timeout")))
        } else {
            res?;
            Err(SinkError::Http(anyhow::anyhow!("channel closed")))
        }
    }

    fn abort(&mut self) -> Result<()> {
        if let Some(sender) = self.sender.take() {
            sender.abort();
        }
        Ok(())
    }

    pub async fn write(&mut self, data: Bytes) -> Result<()> {
        if self.is_first_record {
            self.is_first_record = false;
        } else {
            self.buffer.put_slice("\n".as_bytes());
        }
        self.buffer.put_slice(&data);
        if self.buffer.len() >= MIN_CHUNK_SIZE {
            self.send_chunk().await?;
        }
        Ok(())
    }

    async fn wait_handle(&mut self) -> Result<DorisInsertResultResponse> {
        let res =
            match tokio::time::timeout(WAIT_HANDDLE_TIMEOUT, self.join_handle.as_mut().unwrap())
                .await
            {
                Ok(res) => res.map_err(|err| SinkError::Http(err.into()))??,
                Err(err) => return Err(SinkError::Http(err.into())),
            };
        if !DORIS_SUCCESS_STATUS.contains(&res.status.as_str()) {
            return Err(SinkError::Http(anyhow::anyhow!(
                "Insert error: {:?}, error url: {:?}",
                res.message,
                res.err_url
            )));
        };
        Ok(res)
    }

    pub async fn finish(mut self) -> Result<DorisInsertResultResponse> {
        if !self.buffer.is_empty() {
            self.send_chunk().await?;
        }
        self.sender = None;
        self.wait_handle().await
    }
}

pub struct DorisGet {
    url: String,
    table: String,
    db: String,
    user: String,
    password: String,
}
impl DorisGet {
    pub fn new(url: String, table: String, db: String, user: String, password: String) -> Self {
        Self {
            url,
            table,
            db,
            user,
            password,
        }
    }

    pub async fn get_schema_from_doris(&self) -> Result<DorisSchema> {
        let uri = format!("{}/api/{}/{}/_schema", self.url, self.db, self.table);
        let builder = Request::get(uri);

        let connector = HttpsConnector::new();
        let client = Client::builder()
            .pool_idle_timeout(POOL_IDLE_TIMEOUT)
            .build(connector);

        let request = builder
            .header(
                "Authorization",
                format!(
                    "Basic {}",
                    general_purpose::STANDARD.encode(format!("{}:{}", self.user, self.password))
                ),
            )
            .body(Body::empty())
            .map_err(|err| SinkError::Http(err.into()))?;

        let response = client
            .request(request)
            .await
            .map_err(|err| SinkError::Http(err.into()))?;

        let raw_bytes = String::from_utf8(match body::to_bytes(response.into_body()).await {
            Ok(bytes) => bytes.to_vec(),
            Err(err) => return Err(SinkError::Http(err.into())),
        })
        .map_err(|err| SinkError::Http(err.into()))?;

        let json_map: HashMap<String, Value> =
            serde_json::from_str(&raw_bytes).map_err(|err| SinkError::Http(err.into()))?;
        let json_data = if json_map.contains_key("code") && json_map.contains_key("msg") {
            let data = json_map
                .get("data")
                .ok_or_else(|| SinkError::Http(anyhow::anyhow!("Can't find data")))?;
            data.to_string()
        } else {
            raw_bytes
        };
        let schema: DorisSchema = serde_json::from_str(&json_data).map_err(|err| {
            SinkError::Http(anyhow::anyhow!("Can't get schema from json {:?}", err))
        })?;
        Ok(schema)
    }
}
#[derive(Debug, Serialize, Deserialize)]
pub struct DorisSchema {
    status: i32,
    #[serde(rename = "keysType")]
    pub keys_type: String,
    pub properties: Vec<DorisField>,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct DorisField {
    pub name: String,
    pub r#type: String,
    comment: String,
    pub precision: Option<String>,
    pub scale: Option<String>,
    aggregation_type: String,
}
impl DorisField {
    pub fn get_decimal_pre_scale(&self) -> Option<(u8, u8)> {
        if self.r#type.contains("DECIMAL") {
            let a = self.precision.clone().unwrap().parse::<u8>().unwrap();
            let b = self.scale.clone().unwrap().parse::<u8>().unwrap();
            Some((a, b))
        } else {
            None
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DorisInsertResultResponse {
    #[serde(rename = "TxnId")]
    txn_id: i64,
    #[serde(rename = "Label")]
    label: String,
    #[serde(rename = "Status")]
    status: String,
    #[serde(rename = "TwoPhaseCommit")]
    two_phase_commit: String,
    #[serde(rename = "Message")]
    message: String,
    #[serde(rename = "NumberTotalRows")]
    number_total_rows: i64,
    #[serde(rename = "NumberLoadedRows")]
    number_loaded_rows: i64,
    #[serde(rename = "NumberFilteredRows")]
    number_filtered_rows: i32,
    #[serde(rename = "NumberUnselectedRows")]
    number_unselected_rows: i32,
    #[serde(rename = "LoadBytes")]
    load_bytes: i64,
    #[serde(rename = "LoadTimeMs")]
    load_time_ms: i32,
    #[serde(rename = "BeginTxnTimeMs")]
    begin_txn_time_ms: i32,
    #[serde(rename = "StreamLoadPutTimeMs")]
    stream_load_put_time_ms: i32,
    #[serde(rename = "ReadDataTimeMs")]
    read_data_time_ms: i32,
    #[serde(rename = "WriteDataTimeMs")]
    write_data_time_ms: i32,
    #[serde(rename = "CommitAndPublishTimeMs")]
    commit_and_publish_time_ms: i32,
    #[serde(rename = "ErrorURL")]
    err_url: Option<String>,
}

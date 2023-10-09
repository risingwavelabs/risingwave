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
use mysql_async::prelude::Queryable;
use mysql_async::Opts;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::task::JoinHandle;

use super::{Result, SinkError};

const BUFFER_SIZE: usize = 64 * 1024;
const MIN_CHUNK_SIZE: usize = BUFFER_SIZE - 1024;
const DORIS_SUCCESS_STATUS: [&str; 2] = ["Success", "Publish Timeout"];
pub(crate) const DORIS_DELETE_SIGN: &str = "__DORIS_DELETE_SIGN__";
pub(crate) const STARROCKS_DELETE_SIGN: &str = "__op";
const STARROCK_MYSQL_PREFER_SOCKET: &str = "false";
const STARROCK_MYSQL_MAX_ALLOWED_PACKET: usize = 1024;
const STARROCK_MYSQL_WAIT_TIMEOUT: usize = 31536000;
const SEND_CHUNK_TIMEOUT: Duration = Duration::from_secs(10);
const WAIT_HANDDLE_TIMEOUT: Duration = Duration::from_secs(10);
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const DORIS: &str = "doris";
const STARROCKS: &str = "starrocks";
pub struct HeaderBuilder {
    header: HashMap<String, String>,
}
impl Default for HeaderBuilder {
    fn default() -> Self {
        Self::new()
    }
}
impl HeaderBuilder {
    pub fn new() -> Self {
        Self {
            header: HashMap::default(),
        }
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

    pub fn set_columns_name(mut self, columns_name: Vec<&str>) -> Self {
        let columns_name_str = columns_name.join(",");
        self.header.insert("columns".to_string(), columns_name_str);
        self
    }

    /// This method is only called during upsert operations.
    pub fn add_hidden_column(mut self) -> Self {
        self.header
            .insert("hidden_columns".to_string(), DORIS_DELETE_SIGN.to_string());
        self
    }

    /// The method is temporarily not in use, reserved for later use in 2PC.
    /// Only use in Doris
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
    /// Only use in Doris
    pub fn set_txn_id(mut self, txn_id: i64) -> Self {
        self.header
            .insert("txn_operation".to_string(), txn_id.to_string());
        self
    }

    /// The method is temporarily not in use, reserved for later use in 2PC.
    /// Only use in Doris
    pub fn add_commit(mut self) -> Self {
        self.header
            .insert("txn_operation".to_string(), "commit".to_string());
        self
    }

    /// The method is temporarily not in use, reserved for later use in 2PC.
    /// Only use in Doris
    pub fn add_abort(mut self) -> Self {
        self.header
            .insert("txn_operation".to_string(), "abort".to_string());
        self
    }

    pub fn add_json_format(mut self) -> Self {
        self.header.insert("format".to_string(), "json".to_string());
        self
    }

    /// Only use in Doris
    pub fn add_read_json_by_line(mut self) -> Self {
        self.header
            .insert("read_json_by_line".to_string(), "true".to_string());
        self
    }

    /// Only use in Starrocks
    pub fn add_strip_outer_array(mut self) -> Self {
        self.header
            .insert("strip_outer_array".to_string(), "true".to_string());
        self
    }

    pub fn build(self) -> HashMap<String, String> {
        self.header
    }
}

pub struct InserterBuilder {
    url: String,
    header: HashMap<String, String>,
    sender: Option<Sender>,
}
impl InserterBuilder {
    pub fn new(url: String, db: String, table: String, header: HashMap<String, String>) -> Self {
        let url = format!("{}/api/{}/{}/_stream_load", url, db, table);

        Self {
            url,
            sender: None,
            header,
        }
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

    async fn build_http_send_feature(
        &self,
        send_type: &'static str,
    ) -> Result<(JoinHandle<Result<InsertResultResponse>>, Sender)> {
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

        let handle: JoinHandle<Result<InsertResultResponse>> = tokio::spawn(async move {
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
                let response = if send_type.eq(DORIS) {
                    let response: DorisInsertResultResponse = serde_json::from_str(&raw_string)
                        .map_err(|err| SinkError::Http(err.into()))?;
                    InsertResultResponse::Doris(response)
                } else if send_type.eq(STARROCKS) {
                    let response: StarrocksInsertResultResponse = serde_json::from_str(&raw_string)
                        .map_err(|err| SinkError::Http(err.into()))?;
                    InsertResultResponse::Starrocks(response)
                } else {
                    return Err(SinkError::Http(anyhow::anyhow!(
                        "Can't convert {:?}'s http response to struct",
                        send_type
                    )));
                };
                Ok(response)
            } else {
                Err(SinkError::Http(anyhow::anyhow!(
                    "Failed connection {:?},{:?}",
                    status,
                    raw_string
                )))
            }
        });
        Ok((handle, sender))
    }

    pub async fn build_doris(&mut self) -> Result<DorisInsert> {
        let (handle, sender) = self.build_http_send_feature(DORIS).await?;

        Ok(DorisInsert::new(sender, handle))
    }

    pub async fn build_starrocks(&mut self) -> Result<StarrocksInsert> {
        let (handle, sender) = self.build_http_send_feature(STARROCKS).await?;

        Ok(StarrocksInsert::new(sender, handle))
    }
}

pub struct DorisInsert {
    insert: InserterInner,
    is_first_record: bool,
}
impl DorisInsert {
    pub fn new(sender: Sender, join_handle: JoinHandle<Result<InsertResultResponse>>) -> Self {
        Self {
            insert: InserterInner::new(sender, join_handle),
            is_first_record: true,
        }
    }

    pub async fn write(&mut self, data: Bytes) -> Result<()> {
        let mut data_build = BytesMut::new();
        if self.is_first_record {
            self.is_first_record = false;
        } else {
            data_build.put_slice("\n".as_bytes());
        }
        data_build.put_slice(&data);
        self.insert.write(data_build.into()).await?;
        Ok(())
    }

    pub async fn finish(self) -> Result<DorisInsertResultResponse> {
        let res = match self.insert.finish().await? {
            InsertResultResponse::Doris(doris_res) => doris_res,
            InsertResultResponse::Starrocks(_) => {
                return Err(SinkError::Http(anyhow::anyhow!("Response is not doris")))
            }
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
}

pub struct StarrocksInsert {
    insert: InserterInner,
}
impl StarrocksInsert {
    pub fn new(sender: Sender, join_handle: JoinHandle<Result<InsertResultResponse>>) -> Self {
        Self {
            insert: InserterInner::new(sender, join_handle),
        }
    }

    pub async fn write(&mut self, data: Bytes) -> Result<()> {
        self.insert.write(data).await?;
        Ok(())
    }

    pub async fn finish(self) -> Result<StarrocksInsertResultResponse> {
        let res = match self.insert.finish().await? {
            InsertResultResponse::Doris(_) => {
                return Err(SinkError::Http(anyhow::anyhow!(
                    "Response is not starrocks"
                )))
            }
            InsertResultResponse::Starrocks(res) => res,
        };

        if !DORIS_SUCCESS_STATUS.contains(&res.status.as_str()) {
            return Err(SinkError::Http(anyhow::anyhow!(
                "Insert error: {:?}",
                res.message,
            )));
        };
        Ok(res)
    }
}

struct InserterInner {
    sender: Option<Sender>,
    join_handle: Option<JoinHandle<Result<InsertResultResponse>>>,
    buffer: BytesMut,
}
impl InserterInner {
    pub fn new(sender: Sender, join_handle: JoinHandle<Result<InsertResultResponse>>) -> Self {
        Self {
            sender: Some(sender),
            join_handle: Some(join_handle),
            buffer: BytesMut::with_capacity(BUFFER_SIZE),
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
        self.buffer.put_slice(&data);
        if self.buffer.len() >= MIN_CHUNK_SIZE {
            self.send_chunk().await?;
        }
        Ok(())
    }

    async fn wait_handle(&mut self) -> Result<InsertResultResponse> {
        let res =
            match tokio::time::timeout(WAIT_HANDDLE_TIMEOUT, self.join_handle.as_mut().unwrap())
                .await
            {
                Ok(res) => res.map_err(|err| SinkError::Http(err.into()))??,
                Err(err) => return Err(SinkError::Http(err.into())),
            };
        Ok(res)
    }

    pub async fn finish(mut self) -> Result<InsertResultResponse> {
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

pub enum InsertResultResponse {
    Doris(DorisInsertResultResponse),
    Starrocks(StarrocksInsertResultResponse),
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

#[derive(Debug, Serialize, Deserialize)]
pub struct StarrocksInsertResultResponse {
    #[serde(rename = "TxnId")]
    txn_id: i64,
    #[serde(rename = "Label")]
    label: String,
    #[serde(rename = "Status")]
    status: String,
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
    #[serde(rename = "ReadDataTimeMs")]
    read_data_time_ms: i32,
    #[serde(rename = "WriteDataTimeMs")]
    write_data_time_ms: i32,
    #[serde(rename = "CommitAndPublishTimeMs")]
    commit_and_publish_time_ms: i32,
    #[serde(rename = "StreamLoadPlanTimeMs")]
    stream_load_plan_time_ms: Option<i32>,
}

pub struct StarrocksMysqlQuery {
    table: String,
    db: String,
    conn: mysql_async::Conn,
}

impl StarrocksMysqlQuery {
    pub async fn new(
        host: String,
        port: String,
        table: String,
        db: String,
        user: String,
        password: String,
    ) -> Result<Self> {
        let conn_uri = format!(
            "mysql://{}:{}@{}:{}/{}?prefer_socket={}&max_allowed_packet={}&wait_timeout={}",
            user,
            password,
            host,
            port,
            db,
            STARROCK_MYSQL_PREFER_SOCKET,
            STARROCK_MYSQL_MAX_ALLOWED_PACKET,
            STARROCK_MYSQL_WAIT_TIMEOUT
        );
        let pool = mysql_async::Pool::new(
            Opts::from_url(&conn_uri).map_err(|err| SinkError::Http(err.into()))?,
        );
        let conn = pool
            .get_conn()
            .await
            .map_err(|err| SinkError::Http(err.into()))?;

        Ok(Self { table, db, conn })
    }

    pub async fn get_columns_from_starrocks(&mut self) -> Result<HashMap<String, String>> {
        let query = format!("select column_name, column_type from information_schema.columns where table_name = {:?} and table_schema = {:?};",self.table,self.db);
        let mut query_map: HashMap<String, String> = HashMap::default();
        self.conn
            .query_map(query, |(column_name, column_type)| {
                query_map.insert(column_name, column_type)
            })
            .await
            .map_err(|err| SinkError::Http(err.into()))?;
        Ok(query_map)
    }

    pub async fn get_pk_from_starrocks(&mut self) -> Result<(String, String)> {
        let query = format!("select table_model, primary_key from information_schema.tables_config where table_name = {:?} and table_schema = {:?};",self.table,self.db);
        let table_mode_pk: (String, String) = self
            .conn
            .query_map(query, |(table_model, primary_key)| {
                (table_model, primary_key)
            })
            .await
            .map_err(|err| SinkError::Http(err.into()))?
            .get(0)
            .ok_or_else(|| {
                SinkError::Starrocks(format!(
                    "Can't find schema with table {:?} and database {:?}",
                    self.table, self.db
                ))
            })?
            .clone();
        Ok(table_mode_pk)
    }
}

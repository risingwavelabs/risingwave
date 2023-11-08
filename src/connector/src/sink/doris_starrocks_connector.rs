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
use tokio::task::JoinHandle;

use super::{Result, SinkError};

const BUFFER_SIZE: usize = 64 * 1024;
const MIN_CHUNK_SIZE: usize = BUFFER_SIZE - 1024;
pub(crate) const DORIS_SUCCESS_STATUS: [&str; 2] = ["Success", "Publish Timeout"];
pub(crate) const DORIS_DELETE_SIGN: &str = "__DORIS_DELETE_SIGN__";
pub(crate) const STARROCKS_DELETE_SIGN: &str = "__op";

const SEND_CHUNK_TIMEOUT: Duration = Duration::from_secs(10);
const WAIT_HANDDLE_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
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

pub struct InserterInnerBuilder {
    url: String,
    header: HashMap<String, String>,
    sender: Option<Sender>,
}
impl InserterInnerBuilder {
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

    pub async fn build(&self) -> Result<InserterInner> {
        let (builder, client) = self.build_request_and_client(self.url.clone());
        let request_get_url = builder
            .body(Body::empty())
            .map_err(|err| SinkError::DorisStarrocksConnect(err.into()))?;
        let resp = client
            .request(request_get_url)
            .await
            .map_err(|err| SinkError::DorisStarrocksConnect(err.into()))?;
        let be_url = if resp.status() == StatusCode::TEMPORARY_REDIRECT {
            resp.headers()
                .get("location")
                .ok_or_else(|| {
                    SinkError::DorisStarrocksConnect(anyhow::anyhow!(
                        "Can't get doris BE url in header",
                    ))
                })?
                .to_str()
                .map_err(|err| {
                    SinkError::DorisStarrocksConnect(anyhow::anyhow!(
                        "Can't get doris BE url in header {:?}",
                        err
                    ))
                })?
        } else {
            return Err(SinkError::DorisStarrocksConnect(anyhow::anyhow!(
                "Can't get doris BE url",
            )));
        };

        let (builder, client) = self.build_request_and_client(be_url.to_string());
        let (sender, body) = Body::channel();
        let request = builder
            .body(body)
            .map_err(|err| SinkError::DorisStarrocksConnect(err.into()))?;
        let feature = client.request(request);

        let handle: JoinHandle<Result<Vec<u8>>> = tokio::spawn(async move {
            let response = feature
                .await
                .map_err(|err| SinkError::DorisStarrocksConnect(err.into()))?;
            let status = response.status();
            let raw = body::to_bytes(response.into_body())
                .await
                .map_err(|err| SinkError::DorisStarrocksConnect(err.into()))?
                .to_vec();
            if status == StatusCode::OK && !raw.is_empty() {
                Ok(raw)
            } else {
                Err(SinkError::DorisStarrocksConnect(anyhow::anyhow!(
                    "Failed connection {:?},{:?}",
                    status,
                    String::from_utf8(raw)
                        .map_err(|err| SinkError::DorisStarrocksConnect(err.into()))?
                )))
            }
        });
        Ok(InserterInner::new(sender, handle))
    }
}

pub struct InserterInner {
    sender: Option<Sender>,
    join_handle: Option<JoinHandle<Result<Vec<u8>>>>,
    buffer: BytesMut,
}
impl InserterInner {
    pub fn new(sender: Sender, join_handle: JoinHandle<Result<Vec<u8>>>) -> Self {
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
            Err(SinkError::DorisStarrocksConnect(anyhow::anyhow!("timeout")))
        } else {
            res?;
            Err(SinkError::DorisStarrocksConnect(anyhow::anyhow!(
                "channel closed"
            )))
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

    async fn wait_handle(&mut self) -> Result<Vec<u8>> {
        let res =
            match tokio::time::timeout(WAIT_HANDDLE_TIMEOUT, self.join_handle.as_mut().unwrap())
                .await
            {
                Ok(res) => res.map_err(|err| SinkError::DorisStarrocksConnect(err.into()))??,
                Err(err) => return Err(SinkError::DorisStarrocksConnect(err.into())),
            };
        Ok(res)
    }

    pub async fn finish(mut self) -> Result<Vec<u8>> {
        if !self.buffer.is_empty() {
            self.send_chunk().await?;
        }
        self.sender = None;
        self.wait_handle().await
    }
}

// Copyright 2024 RisingWave Labs
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
use std::convert::Infallible;

use anyhow::Context;
use base64::engine::general_purpose;
use base64::Engine;
use bytes::{BufMut, Bytes, BytesMut};
use futures::StreamExt;
use reqwest::header::{HeaderName, HeaderValue};
use reqwest::{redirect, Body, Client, Method, Request, RequestBuilder, Response, StatusCode};
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use url::Url;

use super::{Result, SinkError};

const BUFFER_SIZE: usize = 64 * 1024;
const MIN_CHUNK_SIZE: usize = BUFFER_SIZE - 1024;
pub(crate) const DORIS_SUCCESS_STATUS: [&str; 2] = ["Success", "Publish Timeout"];
pub(crate) const STARROCKS_SUCCESS_STATUS: [&str; 1] = ["OK"];
pub(crate) const DORIS_DELETE_SIGN: &str = "__DORIS_DELETE_SIGN__";
pub(crate) const STARROCKS_DELETE_SIGN: &str = "__op";

const SEND_CHUNK_TIMEOUT: Duration = Duration::from_secs(10);
const WAIT_HANDDLE_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const DORIS: &str = "doris";
const STARROCKS: &str = "starrocks";
const LOCALHOST: &str = "localhost";
const LOCALHOST_IP: &str = "127.0.0.1";
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

    pub fn set_partial_update(mut self, partial_update: Option<String>) -> Self {
        self.header.insert(
            "partial_update".to_string(),
            partial_update.unwrap_or_else(|| "false".to_string()),
        );
        self
    }

    /// Only used in Starrocks Transaction API
    pub fn set_db(mut self, db: String) -> Self {
        self.header.insert("db".to_string(), db);
        self
    }

    /// Only used in Starrocks Transaction API
    pub fn set_table(mut self, table: String) -> Self {
        self.header.insert("table".to_string(), table);
        self
    }

    pub fn build(self) -> HashMap<String, String> {
        self.header
    }
}

/// Try getting BE url from a redirected response, returning `Ok(None)` indicates this request does
/// not redirect.
///
/// The reason we handle the redirection manually is that if we let `reqwest` handle the redirection
/// automatically, it will remove sensitive headers (such as Authorization) during the redirection,
/// and there's no way to prevent this behavior.
fn try_get_be_url(resp: &Response, fe_host: String) -> Result<Option<Url>> {
    match resp.status() {
        StatusCode::TEMPORARY_REDIRECT => {
            let be_url = resp
                .headers()
                .get("location")
                .ok_or_else(|| {
                    SinkError::DorisStarrocksConnect(anyhow::anyhow!(
                        "Can't get doris BE url in header",
                    ))
                })?
                .to_str()
                .context("Can't get doris BE url in header")
                .map_err(SinkError::DorisStarrocksConnect)?
                .to_string();

            let mut parsed_be_url =
                Url::parse(&be_url).map_err(|err| SinkError::DorisStarrocksConnect(err.into()))?;

            if fe_host != LOCALHOST && fe_host != LOCALHOST_IP {
                let be_host = parsed_be_url.host_str().ok_or_else(|| {
                    SinkError::DorisStarrocksConnect(anyhow::anyhow!("Can't get be host from url"))
                })?;

                if be_host == LOCALHOST || be_host == LOCALHOST_IP {
                    // if be host is 127.0.0.1, we may can't connect to it directly,
                    // so replace it with fe host
                    parsed_be_url
                        .set_host(Some(fe_host.as_str()))
                        .map_err(|err| SinkError::DorisStarrocksConnect(err.into()))?;
                }
            }
            Ok(Some(parsed_be_url))
        }
        StatusCode::OK => {
            // Some of the `StarRocks` transactional APIs will respond directly from FE. For example,
            // the request to `/api/transaction/commit` endpoint does not seem to redirect to BE.
            // In this case, the request should be treated as finished.
            Ok(None)
        }
        _ => Err(SinkError::DorisStarrocksConnect(anyhow::anyhow!(
            "Can't get doris BE url",
        ))),
    }
}

pub struct InserterInnerBuilder {
    url: String,
    header: HashMap<String, String>,
    sender: Option<Sender>,
    fe_host: String,
}
impl InserterInnerBuilder {
    pub fn new(
        url: String,
        db: String,
        table: String,
        header: HashMap<String, String>,
    ) -> Result<Self> {
        let fe_host = Url::parse(&url)
            .map_err(|err| SinkError::DorisStarrocksConnect(err.into()))?
            .host_str()
            .ok_or_else(|| {
                SinkError::DorisStarrocksConnect(anyhow::anyhow!("Can't get fe host from url"))
            })?
            .to_string();
        let url = format!("{}/api/{}/{}/_stream_load", url, db, table);

        Ok(Self {
            url,
            sender: None,
            header,
            fe_host,
        })
    }

    fn build_request(&self, uri: String) -> RequestBuilder {
        let client = Client::builder()
            .pool_idle_timeout(POOL_IDLE_TIMEOUT)
            .redirect(redirect::Policy::none()) // we handle redirect by ourselves
            .build()
            .unwrap();

        let mut builder = client.put(uri);
        for (k, v) in &self.header {
            builder = builder.header(k, v);
        }
        builder
    }

    pub async fn build(&self) -> Result<InserterInner> {
        let builder = self.build_request(self.url.clone());
        let resp = builder
            .send()
            .await
            .map_err(|err| SinkError::DorisStarrocksConnect(err.into()))?;

        let be_url = try_get_be_url(&resp, self.fe_host.clone())?.ok_or_else(|| {
            SinkError::DorisStarrocksConnect(anyhow::anyhow!("Can't get doris BE url",))
        })?;

        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let body = Body::wrap_stream(
            tokio_stream::wrappers::UnboundedReceiverStream::new(receiver).map(Ok::<_, Infallible>),
        );
        let builder = self.build_request(be_url.into()).body(body);

        let handle: JoinHandle<Result<Vec<u8>>> = tokio::spawn(async move {
            let response = builder
                .send()
                .await
                .map_err(|err| SinkError::DorisStarrocksConnect(err.into()))?;
            let status = response.status();
            let raw = response
                .bytes()
                .await
                .map_err(|err| SinkError::DorisStarrocksConnect(err.into()))?
                .into();

            if status == StatusCode::OK {
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
        Ok(InserterInner::new(sender, handle, WAIT_HANDDLE_TIMEOUT))
    }
}

type Sender = UnboundedSender<Bytes>;

pub struct InserterInner {
    sender: Option<Sender>,
    join_handle: Option<JoinHandle<Result<Vec<u8>>>>,
    buffer: BytesMut,
    stream_load_http_timeout: Duration,
}
impl InserterInner {
    pub fn new(
        sender: Sender,
        join_handle: JoinHandle<Result<Vec<u8>>>,
        stream_load_http_timeout: Duration,
    ) -> Self {
        Self {
            sender: Some(sender),
            join_handle: Some(join_handle),
            buffer: BytesMut::with_capacity(BUFFER_SIZE),
            stream_load_http_timeout,
        }
    }

    async fn send_chunk(&mut self) -> Result<()> {
        if self.sender.is_none() {
            return Ok(());
        }

        let chunk = mem::replace(&mut self.buffer, BytesMut::with_capacity(BUFFER_SIZE));

        if let Err(_e) = self.sender.as_mut().unwrap().send(chunk.freeze()) {
            self.sender.take();
            self.wait_handle().await?;

            Err(SinkError::DorisStarrocksConnect(anyhow::anyhow!(
                "channel closed"
            )))
        } else {
            Ok(())
        }
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

pub struct MetaRequestSender {
    client: Client,
    request: Request,
    fe_host: String,
}

impl MetaRequestSender {
    pub fn new(client: Client, request: Request, fe_host: String) -> Self {
        Self {
            client,
            request,
            fe_host,
        }
    }

    /// Send the request and handle redirection if any.
    /// The reason we handle the redirection manually is that if we let `reqwest` handle the redirection
    /// automatically, it will remove sensitive headers (such as Authorization) during the redirection,
    /// and there's no way to prevent this behavior.
    ///
    /// Another interesting point is that some of the `StarRocks` transactional APIs will respond directly from FE.
    /// For example, the request to `/api/transaction/commit` endpoint does not seem to redirect to BE.
    pub async fn send(self) -> Result<Bytes> {
        let request = self.request;
        let mut request_for_redirection = request.try_clone().ok_or_else(|| {
            SinkError::DorisStarrocksConnect(anyhow::anyhow!("Can't clone request"))
        })?;

        let resp = self
            .client
            .execute(request)
            .await
            .map_err(|err| SinkError::DorisStarrocksConnect(err.into()))?;

        let be_url = try_get_be_url(&resp, self.fe_host)?;

        match be_url {
            Some(be_url) => {
                *request_for_redirection.url_mut() = be_url;

                self.client
                    .execute(request_for_redirection)
                    .await
                    .map_err(|err| SinkError::DorisStarrocksConnect(err.into()))?
                    .bytes()
                    .await
                    .map_err(|err| SinkError::DorisStarrocksConnect(err.into()))
            }
            None => resp
                .bytes()
                .await
                .map_err(|err| SinkError::DorisStarrocksConnect(err.into())),
        }
    }
}

pub struct StarrocksTxnRequestBuilder {
    url_begin: String,
    url_load: String,
    url_prepare: String,
    url_commit: String,
    url_rollback: String,
    header: HashMap<String, String>,
    fe_host: String,
    stream_load_http_timeout: Duration,
    // The `reqwest` crate suggests us reuse the Client, and we don't need make it Arc, because it
    // already uses an Arc internally.
    client: Client,
}

impl StarrocksTxnRequestBuilder {
    pub fn new(
        url: String,
        header: HashMap<String, String>,
        stream_load_http_timeout_ms: u64,
    ) -> Result<Self> {
        let fe_host = Url::parse(&url)
            .map_err(|err| SinkError::DorisStarrocksConnect(err.into()))?
            .host_str()
            .ok_or_else(|| {
                SinkError::DorisStarrocksConnect(anyhow::anyhow!("Can't get fe host from url"))
            })?
            .to_string();

        let url_begin = format!("{}/api/transaction/begin", url);
        let url_load = format!("{}/api/transaction/load", url);
        let url_prepare = format!("{}/api/transaction/prepare", url);
        let url_commit = format!("{}/api/transaction/commit", url);
        let url_rollback = format!("{}/api/transaction/rollback", url);

        let stream_load_http_timeout = Duration::from_millis(stream_load_http_timeout_ms);

        let client = Client::builder()
            .pool_idle_timeout(POOL_IDLE_TIMEOUT)
            .redirect(redirect::Policy::none())
            .build()
            .unwrap();

        Ok(Self {
            url_begin,
            url_load,
            url_prepare,
            url_commit,
            url_rollback,
            header,
            fe_host,
            stream_load_http_timeout,
            client,
        })
    }

    fn build_request(&self, uri: String, method: Method, label: String) -> Result<Request> {
        let parsed_url =
            Url::parse(&uri).map_err(|err| SinkError::DorisStarrocksConnect(err.into()))?;
        let mut request = Request::new(method, parsed_url);

        if uri != self.url_load {
            // Set timeout for non-load requests; load requests' timeout is controlled by `tokio::timeout`
            *request.timeout_mut() = Some(self.stream_load_http_timeout);
        }

        let header = request.headers_mut();
        for (k, v) in &self.header {
            header.insert(
                HeaderName::try_from(k)
                    .map_err(|err| SinkError::DorisStarrocksConnect(err.into()))?,
                HeaderValue::try_from(v)
                    .map_err(|err| SinkError::DorisStarrocksConnect(err.into()))?,
            );
        }
        header.insert(
            "label",
            HeaderValue::try_from(label)
                .map_err(|err| SinkError::DorisStarrocksConnect(err.into()))?,
        );

        Ok(request)
    }

    pub fn build_begin_request_sender(&self, label: String) -> Result<MetaRequestSender> {
        let request = self.build_request(self.url_begin.clone(), Method::POST, label)?;
        Ok(MetaRequestSender::new(
            self.client.clone(),
            request,
            self.fe_host.clone(),
        ))
    }

    pub fn build_prepare_request_sender(&self, label: String) -> Result<MetaRequestSender> {
        let request = self.build_request(self.url_prepare.clone(), Method::POST, label)?;
        Ok(MetaRequestSender::new(
            self.client.clone(),
            request,
            self.fe_host.clone(),
        ))
    }

    pub fn build_commit_request_sender(&self, label: String) -> Result<MetaRequestSender> {
        let request = self.build_request(self.url_commit.clone(), Method::POST, label)?;
        Ok(MetaRequestSender::new(
            self.client.clone(),
            request,
            self.fe_host.clone(),
        ))
    }

    pub fn build_rollback_request_sender(&self, label: String) -> Result<MetaRequestSender> {
        let request = self.build_request(self.url_rollback.clone(), Method::POST, label)?;
        Ok(MetaRequestSender::new(
            self.client.clone(),
            request,
            self.fe_host.clone(),
        ))
    }

    pub async fn build_txn_inserter(&self, label: String) -> Result<InserterInner> {
        let request = self.build_request(self.url_load.clone(), Method::PUT, label)?;
        let mut request_for_redirection = request.try_clone().ok_or_else(|| {
            SinkError::DorisStarrocksConnect(anyhow::anyhow!("Can't clone request"))
        })?;

        let resp = self
            .client
            .execute(request)
            .await
            .map_err(|err| SinkError::DorisStarrocksConnect(err.into()))?;

        let be_url = try_get_be_url(&resp, self.fe_host.clone())?.ok_or_else(|| {
            SinkError::DorisStarrocksConnect(anyhow::anyhow!("Can't get doris BE url",))
        })?;
        *request_for_redirection.url_mut() = be_url;

        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let body = Body::wrap_stream(
            tokio_stream::wrappers::UnboundedReceiverStream::new(receiver).map(Ok::<_, Infallible>),
        );
        *request_for_redirection.body_mut() = Some(body);

        let client = self.client.clone();
        let handle: JoinHandle<Result<Vec<u8>>> = tokio::spawn(async move {
            let response = client
                .execute(request_for_redirection)
                .await
                .map_err(|err| SinkError::DorisStarrocksConnect(err.into()))?;

            let status = response.status();
            let raw = response
                .bytes()
                .await
                .map_err(|err| SinkError::DorisStarrocksConnect(err.into()))?
                .into();

            if status == StatusCode::OK {
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
        Ok(InserterInner::new(
            sender,
            handle,
            self.stream_load_http_timeout,
        ))
    }
}

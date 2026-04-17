// Copyright 2025 RisingWave Labs
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

use std::collections::BTreeMap;

use anyhow::anyhow;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, ScalarRefImpl};
use serde::Deserialize;
use with_options::WithOptions;

use crate::enforce_secret::EnforceSecret;
use crate::sink::log_store::DeliveryFutureManagerAddFuture;
use crate::sink::writer::{
    AsyncTruncateLogSinkerOf, AsyncTruncateSinkWriter, AsyncTruncateSinkWriterExt,
};
use crate::sink::{Result, SINK_TYPE_APPEND_ONLY, Sink, SinkError, SinkParam, SinkWriterParam};

pub const HTTP_SINK: &str = "http";

fn default_content_type() -> String {
    "application/json".to_owned()
}

#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct HttpConfig {
    /// The endpoint URL to POST data to.
    pub url: String,

    /// Content-Type header value. Defaults to "application/json".
    #[serde(default = "default_content_type")]
    pub content_type: String,

    /// Sink type, must be "append-only".
    pub r#type: String,
}

impl EnforceSecret for HttpConfig {}

impl HttpConfig {
    pub fn from_btreemap(
        values: BTreeMap<String, String>,
    ) -> Result<(Self, BTreeMap<String, String>)> {
        // Extract header.* keys before serde parsing
        let mut headers = BTreeMap::new();
        let mut rest = BTreeMap::new();
        for (k, v) in &values {
            if let Some(header_name) = k.strip_prefix("header.") {
                headers.insert(header_name.to_owned(), v.clone());
            } else {
                rest.insert(k.clone(), v.clone());
            }
        }

        let config = serde_json::from_value::<HttpConfig>(serde_json::to_value(rest).unwrap())
            .map_err(|e| SinkError::Config(anyhow!(e)))?;

        if config.r#type != SINK_TYPE_APPEND_ONLY {
            return Err(SinkError::Config(anyhow!(
                "HTTP sink only supports append-only mode"
            )));
        }

        Ok((config, headers))
    }
}

/// Validates the HTTP sink parameters and returns the extracted column type and parsed URL
/// so callers can use them directly without re-parsing.
fn validate_http_sink(
    is_append_only: bool,
    schema: &Schema,
    url: &str,
) -> Result<(DataType, reqwest::Url)> {
    if !is_append_only {
        return Err(SinkError::Config(anyhow!(
            "HTTP sink only supports append-only mode"
        )));
    }

    if schema.fields().len() != 1 {
        return Err(SinkError::Config(anyhow!(
            "HTTP sink requires exactly 1 column, got {}",
            schema.fields().len()
        )));
    }

    let col_type = schema.fields()[0].data_type.clone();
    if col_type != DataType::Varchar && col_type != DataType::Jsonb {
        return Err(SinkError::Config(anyhow!(
            "HTTP sink column must be varchar or jsonb, got {:?}",
            col_type
        )));
    }

    let parsed_url = url
        .parse()
        .map_err(|e| SinkError::Config(anyhow!("invalid URL: {}", e)))?;

    Ok((col_type, parsed_url))
}

#[derive(Clone, Debug)]
pub struct HttpSink {
    pub config: HttpConfig,
    headers: BTreeMap<String, String>,
}

impl EnforceSecret for HttpSink {
    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::error::ConnectorResult<()> {
        for prop in prop_iter {
            HttpConfig::enforce_one(prop)?;
        }
        Ok(())
    }
}

impl TryFrom<SinkParam> for HttpSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let url = param
            .properties
            .get("url")
            .ok_or_else(|| SinkError::Config(anyhow!("missing 'url' in WITH")))?;
        validate_http_sink(param.sink_type.is_append_only(), &schema, url)?;

        let (config, headers) = HttpConfig::from_btreemap(param.properties)?;
        Ok(Self { config, headers })
    }
}

impl Sink for HttpSink {
    type LogSinker = AsyncTruncateLogSinkerOf<HttpSinkWriter>;

    const SINK_NAME: &'static str = HTTP_SINK;

    async fn validate(&self) -> Result<()> {
        Ok(())
    }

    async fn new_log_sinker(&self, _writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(
            HttpSinkWriter::new(self.config.clone(), self.headers.clone())?
                .into_log_sinker(usize::MAX),
        )
    }
}

pub struct HttpSinkWriter {
    client: reqwest::Client,
    endpoint: String,
}

impl HttpSinkWriter {
    pub fn new(config: HttpConfig, headers: BTreeMap<String, String>) -> Result<Self> {
        let mut header_map = reqwest::header::HeaderMap::new();
        header_map.insert(
            reqwest::header::CONTENT_TYPE,
            config
                .content_type
                .parse()
                .map_err(|e| SinkError::Http(anyhow!("invalid content_type: {}", e)))?,
        );
        for (k, v) in &headers {
            let name: reqwest::header::HeaderName = k
                .parse()
                .map_err(|e| SinkError::Http(anyhow!("invalid header name '{}': {}", k, e)))?;
            let value: reqwest::header::HeaderValue = v
                .parse()
                .map_err(|e| SinkError::Http(anyhow!("invalid header value for '{}': {}", k, e)))?;
            header_map.insert(name, value);
        }

        let client = reqwest::Client::builder()
            .default_headers(header_map)
            .build()
            .map_err(|e| SinkError::Http(anyhow!("failed to build HTTP client: {}", e)))?;

        Ok(Self {
            client,
            endpoint: config.url,
        })
    }
}

impl AsyncTruncateSinkWriter for HttpSinkWriter {
    async fn write_chunk<'a>(
        &'a mut self,
        chunk: StreamChunk,
        _add_future: DeliveryFutureManagerAddFuture<'a, Self::DeliveryFuture>,
    ) -> Result<()> {
        for (op, row) in chunk.rows() {
            if op != Op::Insert {
                continue;
            }

            let payload = match row.datum_at(0) {
                Some(ScalarRefImpl::Utf8(s)) => s.to_owned(),
                Some(ScalarRefImpl::Jsonb(j)) => j.to_string(),
                Some(_) => {
                    return Err(SinkError::Http(anyhow!(
                        "unexpected column type, expected varchar or jsonb"
                    )));
                }
                None => continue, // skip NULL rows
            };

            let resp = self
                .client
                .post(&self.endpoint)
                .body(payload)
                .send()
                .await
                .map_err(|e| SinkError::Http(anyhow!("HTTP request failed: {}", e)))?;

            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(SinkError::Http(anyhow!(
                    "HTTP sink received non-success response: {} {}",
                    status,
                    body
                )));
            }
        }

        Ok(())
    }
}

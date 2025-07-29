use std::collections::BTreeMap;
use std::fmt::Debug;
use std::time::Duration;

use anyhow::{anyhow, bail};
use async_trait::async_trait;
use reqwest::Client;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use serde_derive::Deserialize;
use serde_json::Value;
use serde_with::serde_as;

use super::{Sink, SinkError, SinkParam, SinkWriterMetrics};
use crate::enforce_secret::EnforceSecret;
use crate::sink::encoder::{JsonEncoder, RowEncoder};
use crate::sink::writer::{LogSinkerOf, SinkWriter, SinkWriterExt};
use crate::sink::{DummySinkCommitCoordinator, Result, SinkWriterParam};

pub const WEBHOOK_SINK: &str = "webhook";

use std::collections::HashMap;

use phf::{Set, phf_set};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use with_options::WithOptions;

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct WebhookConfig {
    pub endpoint: String,
    pub headers: Option<String>,
}

impl WebhookConfig {
    pub fn from_btreemap(values: BTreeMap<String, String>) -> Result<Self> {
        let config = serde_json::from_value::<WebhookConfig>(serde_json::to_value(values).unwrap())
            .map_err(|e| SinkError::Config(anyhow!(e)))?;

        Ok(config)
    }
}

impl EnforceSecret for WebhookConfig {
    const ENFORCE_SECRET_PROPERTIES: Set<&'static str> = phf_set! {};
}

#[derive(Debug)]
pub struct WebhookSink {
    pub config: WebhookConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
}

impl EnforceSecret for WebhookSink {}

impl WebhookSink {
    pub fn new(
        config: WebhookConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        Ok(Self {
            config,
            schema,
            pk_indices,
            is_append_only,
        })
    }
}

impl TryFrom<SinkParam> for WebhookSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let config = WebhookConfig::from_btreemap(param.properties)
            .map_err(|e| SinkError::Config(anyhow!(e)))?;
        WebhookSink::new(
            config,
            schema,
            param.downstream_pk,
            param.sink_type.is_append_only(),
        )
    }
}

impl Sink for WebhookSink {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = LogSinkerOf<WebhookSinkWriter>;

    const SINK_NAME: &'static str = WEBHOOK_SINK;

    async fn validate(&self) -> Result<()> {
        if self.config.endpoint.is_empty() {
            return Err(SinkError::Config(anyhow!(
                "Webhook endpoint cannot be empty"
            )));
        }
        if !self.is_append_only {
            return Err(SinkError::Config(anyhow!(
                "Webhook sink only supports append-only mode"
            )));
        }
        Ok(())
    }

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(
            WebhookSinkWriter::new(self.config.clone(), self.schema.clone())
                .await?
                .into_log_sinker(SinkWriterMetrics::new(&writer_param)),
        )
    }
}

pub struct WebhookSinkWriter {
    config: WebhookConfig,
    client: Client,
    row_encoder: JsonEncoder,
}

impl WebhookSinkWriter {
    async fn new(config: WebhookConfig, schema: Schema) -> anyhow::Result<Self> {
        let client = construct_http_client(&config.endpoint, config.headers.clone())?;
        Ok(Self {
            config,
            client,
            row_encoder: JsonEncoder::new_with_webhook(schema, None),
        })
    }

    async fn write(&self, payload: String) -> Result<()> {
        let request = self
            .client
            .post(&self.config.endpoint)
            .body(payload)
            .build()
            .map_err(|e| SinkError::Webhook(anyhow!(e)))?;

        self.client
            .execute(request)
            .await
            .map_err(|e| SinkError::Webhook(anyhow!(e)))?;

        Ok(())
    }
}

#[async_trait]
impl SinkWriter for WebhookSinkWriter {
    /// Begin a new epoch
    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        Ok(())
    }

    /// Write a stream chunk to sink
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        for (op, row) in chunk.rows() {
            match op {
                Op::Insert => {
                    let row_json_string = Value::Object(
                        self.row_encoder
                            .encode(row)
                            .map_err(|e| SinkError::Webhook(anyhow!(e)))?,
                    )
                    .to_string();
                    self.write(row_json_string).await?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Receive a barrier and mark the end of current epoch. When `is_checkpoint` is true, the sink
    /// writer should commit the current epoch.
    async fn barrier(&mut self, is_checkpoint: bool) -> Result<Self::CommitMetadata> {
        Ok(())
    }
}

pub fn string_to_map(s: &str, pair_delimeter: char) -> Option<HashMap<String, String>> {
    if s.trim().is_empty() {
        return Some(HashMap::new());
    }

    s.split(',')
        .map(|s| {
            let mut kv = s.trim().split(pair_delimeter);
            Some((kv.next()?.trim().to_string(), kv.next()?.trim().to_string()))
        })
        .collect()
}

/// Construct the http client for the webhook sink.
fn construct_http_client(endpoint: &str, headers: Option<String>) -> anyhow::Result<Client> {
    if let Err(e) = reqwest::Url::parse(endpoint) {
        bail!("invalid endpoint '{}': {:?}", endpoint, e)
    };

    let headers: anyhow::Result<HeaderMap> = string_to_map(headers.as_deref().unwrap_or(""), ':')
        .expect("Invalid header map")
        .into_iter()
        .map(|(k, v)| {
            Ok((
                TryInto::<HeaderName>::try_into(&k)
                    .map_err(|_| anyhow!("invalid header name {}", k))?,
                TryInto::<HeaderValue>::try_into(&v)
                    .map_err(|_| anyhow!("invalid header value {}", v))?,
            ))
        })
        .collect();

    let client = reqwest::ClientBuilder::new()
        .default_headers(headers?)
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(|e| anyhow!("could not construct HTTP client: {:?}", e))?;

    Ok(client)
}

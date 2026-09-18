// Copyright 2026 RisingWave Labs
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

use std::collections::{BTreeMap, HashMap};
use std::fmt;

use anyhow::{anyhow, ensure};
use phf::{Set, phf_set};
use risingwave_common::catalog::Schema;
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use url::Url;
use with_options::WithOptions;

use self::client::RabbitMqClient;
use self::encoder::RabbitMqEncoder;
use self::writer::RabbitMqSinkWriter;
use crate::enforce_secret::EnforceSecret;
use crate::sink::catalog::SinkFormatDesc;
use crate::sink::writer::{AsyncTruncateLogSinkerOf, AsyncTruncateSinkWriterExt};
use crate::sink::{Result, SINK_TYPE_APPEND_ONLY, Sink, SinkError, SinkParam, SinkWriterParam};

pub mod client;
pub mod encoder;
pub mod writer;

pub const RABBITMQ_SINK: &str = "rabbitmq";

#[serde_as]
#[derive(Clone, Deserialize, WithOptions)]
pub struct RabbitMqConfig {
    /// AMQP(S) URL with an optional percent-encoded vhost; no credentials, query or fragment.
    /// TLS uses the system trust store.
    pub url: String,
    /// Broker username.
    pub username: String,
    /// Broker password.
    pub password: String,
    /// Target exchange; empty for the default exchange.
    pub exchange: String,
    /// Routing key; queue name when using the default exchange.
    pub routing_key: String,
    /// Sink type; defaults to append-only, the only supported mode.
    #[serde(default = "default_sink_type")]
    pub r#type: String,
    /// Connection, channel and confirm setup timeout in milliseconds.
    #[serde(default = "default_connect_timeout_ms")]
    #[serde_as(as = "DisplayFromStr")]
    pub connect_timeout_ms: u64,
    /// Publish submission timeout in milliseconds.
    #[serde(default = "default_publish_timeout_ms")]
    #[serde_as(as = "DisplayFromStr")]
    pub publish_timeout_ms: u64,
    /// Publisher confirm timeout in milliseconds.
    #[serde(default = "default_confirm_timeout_ms")]
    #[serde_as(as = "DisplayFromStr")]
    pub confirm_timeout_ms: u64,
    /// Maximum messages awaiting confirmation.
    #[serde(default = "default_max_inflight_messages")]
    #[serde_as(as = "DisplayFromStr")]
    pub max_inflight_messages: usize,
    /// Maximum encoded message size in bytes.
    #[serde(default = "default_max_message_size")]
    #[serde_as(as = "DisplayFromStr")]
    pub max_message_size: usize,
    /// Heartbeat timeout in seconds; zero accepts the broker's value.
    #[serde(default = "default_heartbeat")]
    #[serde_as(as = "DisplayFromStr")]
    pub heartbeat: u16,
    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

fn default_sink_type() -> String {
    SINK_TYPE_APPEND_ONLY.to_owned()
}

fn default_connect_timeout_ms() -> u64 {
    10_000
}

fn default_publish_timeout_ms() -> u64 {
    30_000
}

fn default_confirm_timeout_ms() -> u64 {
    30_000
}

fn default_max_inflight_messages() -> usize {
    1024
}

fn default_max_message_size() -> usize {
    1_048_576
}

fn default_heartbeat() -> u16 {
    30
}

crate::impl_sink_unknown_fields!(RabbitMqConfig);

impl EnforceSecret for RabbitMqConfig {
    const ENFORCE_SECRET_PROPERTIES: Set<&'static str> = phf_set! { "password" };
}

impl RabbitMqConfig {
    pub fn from_btreemap(values: BTreeMap<String, String>) -> Result<Self> {
        let config: Self = serde_json::from_value(serde_json::to_value(values).unwrap())
            .map_err(|e| SinkError::Config(anyhow!(e)))?;
        config.validate().map_err(SinkError::Config)?;
        Ok(config)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.r#type == SINK_TYPE_APPEND_ONLY,
            "RabbitMQ sink only supports append-only mode"
        );
        // Do not include the original URL or parser error: either can contain credentials.
        let url = Url::parse(&self.url).map_err(|_| anyhow!("invalid RabbitMQ url"))?;
        ensure!(
            matches!(url.scheme(), "amqp" | "amqps") && url.host_str().is_some(),
            "RabbitMQ url must use amqp:// or amqps:// and include a host"
        );
        ensure!(
            url.username().is_empty() && url.password().is_none(),
            "RabbitMQ url must not contain credentials; use username and password"
        );
        ensure!(
            url.query().is_none() && url.fragment().is_none(),
            "RabbitMQ url must not contain query parameters or a fragment"
        );
        ensure!(url.port() != Some(0), "RabbitMQ port must be positive");
        let vhost = url.path().strip_prefix('/').unwrap_or(url.path());
        ensure!(
            !vhost.contains('/'),
            "RabbitMQ virtual host must be percent-encoded in the URL"
        );
        ensure!(
            urlencoding::decode(vhost).is_ok(),
            "RabbitMQ virtual host must be valid UTF-8"
        );
        ensure!(
            !self.username.is_empty(),
            "RabbitMQ username must not be empty"
        );
        ensure!(
            !self.username.contains('\0') && !self.password.contains('\0'),
            "RabbitMQ credentials must contain no NUL"
        );
        for (name, value) in [
            ("exchange", &self.exchange),
            ("routing_key", &self.routing_key),
        ] {
            ensure!(
                value.len() <= 255 && !value.contains('\0'),
                "RabbitMQ {name} must be at most 255 bytes and contain no NUL"
            );
        }
        for (name, value) in [
            ("connect_timeout_ms", self.connect_timeout_ms),
            ("publish_timeout_ms", self.publish_timeout_ms),
            ("confirm_timeout_ms", self.confirm_timeout_ms),
        ] {
            ensure!(
                value > 0 && value <= u32::MAX.into(),
                "RabbitMQ {name} must be between 1 and {}",
                u32::MAX
            );
        }
        ensure!(
            self.max_inflight_messages > 0,
            "RabbitMQ max_inflight_messages must be positive"
        );
        ensure!(
            self.max_message_size > 0,
            "RabbitMQ max_message_size must be positive"
        );
        Ok(())
    }
}

#[derive(Clone)]
pub struct RabbitMqSink {
    config: RabbitMqConfig,
    schema: Schema,
    format_desc: SinkFormatDesc,
    schema_subject: String,
}

impl fmt::Debug for RabbitMqSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Connection and schema registry options can contain resolved secrets.
        f.debug_struct("RabbitMqSink")
            .field("schema", &self.schema)
            .field("format", &self.format_desc.format)
            .field("encode", &self.format_desc.encode)
            .finish_non_exhaustive()
    }
}

impl EnforceSecret for RabbitMqSink {
    const ENFORCE_SECRET_PROPERTIES: Set<&'static str> = RabbitMqConfig::ENFORCE_SECRET_PROPERTIES;
}

impl TryFrom<SinkParam> for RabbitMqSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> Result<Self> {
        if !param.sink_type.is_append_only() {
            return Err(SinkError::Config(anyhow!(
                "RabbitMQ sink only supports append-only mode"
            )));
        }
        let schema = param.schema();
        let config = RabbitMqConfig::from_btreemap(param.properties)?;
        let format_desc = param
            .format_desc
            .ok_or_else(|| SinkError::Config(anyhow!("missing FORMAT ... ENCODE ...")))?;
        // Use the routing key as the topic-equivalent schema registry subject. An empty
        // routing key is valid (e.g. for fanout exchanges), so fall back to the sink name.
        let schema_subject = if config.routing_key.is_empty() {
            param.sink_name
        } else {
            config.routing_key.clone()
        };
        Ok(Self {
            config,
            schema,
            format_desc,
            schema_subject,
        })
    }
}

impl RabbitMqSink {
    async fn build_encoder(&self) -> Result<RabbitMqEncoder> {
        RabbitMqEncoder::new(self.schema.clone(), &self.format_desc, &self.schema_subject).await
    }
}

impl Sink for RabbitMqSink {
    type LogSinker = AsyncTruncateLogSinkerOf<RabbitMqSinkWriter>;

    const SINK_NAME: &'static str = RABBITMQ_SINK;

    crate::impl_validate_sink_unknown_fields!();

    async fn validate(&self) -> Result<()> {
        self.validate_unknown_fields()?;
        // Validate encoding and schema before making any broker connection.
        self.build_encoder().await?;
        let _client = RabbitMqClient::connect(&self.config).await?;
        Ok(())
    }

    async fn new_log_sinker(&self, _writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        let encoder = self.build_encoder().await?;
        Ok(RabbitMqSinkWriter::new(self.config.clone(), encoder)
            .await?
            .into_log_sinker(self.config.max_inflight_messages))
    }
}

#[cfg(test)]
mod tests;

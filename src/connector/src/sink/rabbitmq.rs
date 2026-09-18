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

use anyhow::{anyhow, ensure};

use std::collections::{BTreeMap, HashMap};

use phf::{Set, phf_set};
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use url::Url;
use with_options::WithOptions;

use crate::enforce_secret::EnforceSecret;
use crate::sink::{Result, SINK_TYPE_APPEND_ONLY, SinkError};

pub mod client;
pub mod encoder;
pub mod writer;

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
    /// Sink type; only append-only is supported.
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

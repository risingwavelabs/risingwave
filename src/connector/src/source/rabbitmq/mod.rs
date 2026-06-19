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

pub mod enumerator;
pub mod source;
pub mod split;

use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::time::Duration;

use anyhow::{Context, anyhow};
pub use enumerator::RabbitmqSplitEnumerator;
use lapin::options::QueueDeclareOptions;
use lapin::types::FieldTable;
use lapin::{Connection, ConnectionProperties};
use phf::{Set, phf_set};
use risingwave_common::{bail, ensure};
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
pub use source::*;
pub use split::RabbitmqSplit;
use thiserror::Error;
use tokio_retry::Retry;
use tokio_retry::strategy::FixedInterval;
use url::Url;
use with_options::WithOptions;

use crate::enforce_secret::EnforceSecret;
use crate::error::ConnectorResult;
use crate::source::SourceProperties;

pub const RABBITMQ_CONNECTOR: &str = "rabbitmq";

const DEFAULT_PREFETCH_COUNT: u16 = 50;
const MAX_PREFETCH_COUNT: u16 = 1000;
const DEFAULT_PREFETCH_SIZE: u32 = 0;
const DEFAULT_MAX_CONNECTIONS: usize = 5;
const DEFAULT_CONNECTION_ATTEMPTS: usize = 5;
const DEFAULT_RETRY_DELAY_SECONDS: u64 = 2;
const DEFAULT_SOCKET_TIMEOUT_SECONDS: u64 = 10;
const DEFAULT_BLOCKED_CONNECTION_TIMEOUT_SECONDS: u64 = 300;
const DEFAULT_HEARTBEAT_INTERVAL_SECONDS: u16 = 600;
const DEFAULT_FRAME_MAX: u32 = 131_072;

#[derive(Debug, Clone, Error)]
pub struct RabbitmqError(String);

impl Display for RabbitmqError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl RabbitmqError {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct RabbitmqProperties {
    /// AMQP broker URL. Both `amqp://` and `amqps://` are supported by the AMQP client.
    /// Credentials and vhost can be encoded in this URL.
    #[serde(rename = "url")]
    pub url: String,

    /// A single queue name to consume. Use either `queue` or `queues`.
    #[serde(rename = "queue")]
    pub queue: Option<String>,

    /// Comma-separated queue names. Each queue has at most one active consumer in one RW source.
    #[serde(rename = "queues")]
    pub queues: Option<String>,

    /// `RabbitMQ` prefetch count. Platform Science recommends 20-50. Valid range is
    /// 1-1000 because `RabbitMQ` treats 0 as unlimited.
    #[serde(rename = "prefetch_count")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly)]
    pub prefetch_count: Option<u16>,

    /// `RabbitMQ` prefetch size. `RabbitMQ` does not support non-zero values.
    #[serde(rename = "prefetch_size")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub prefetch_size: Option<u32>,

    /// Maximum active AMQP consumer connections opened by this source across adaptive splits.
    /// If the broker enforces a per-vhost connection limit, deploy at most one source per vhost.
    #[serde(rename = "max_connections")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub max_connections: Option<usize>,

    /// AMQP heartbeat interval in seconds.
    #[serde(rename = "heartbeat_interval")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub heartbeat_interval: Option<u16>,

    /// AMQP frame max. Defaults to 131072 (128KiB).
    #[serde(rename = "frame_max")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub frame_max: Option<u32>,

    /// Connection attempts before surfacing an error.
    #[serde(rename = "connection_attempts")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub connection_attempts: Option<usize>,

    /// Fixed retry delay in seconds between connection attempts.
    #[serde(rename = "retry_delay")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub retry_delay: Option<u64>,

    /// Socket/connect timeout in seconds.
    #[serde(rename = "socket_timeout")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub socket_timeout: Option<u64>,

    /// Close and restart the source reader after the broker keeps the connection blocked for this
    /// duration. Blocked connections can prevent checkpoint-time acks from being written.
    #[serde(rename = "blocked_connection_timeout")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub blocked_connection_timeout: Option<u64>,

    /// Check queues exist with passive declare instead of creating them.
    /// This source only supports customer-owned queues, so this must stay true.
    #[serde(rename = "queue.passive")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub queue_passive: Option<bool>,

    /// Consumer tag prefix used to make `RabbitMQ` consumers observable.
    #[serde(rename = "consumer_tag_prefix")]
    pub consumer_tag_prefix: Option<String>,

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

impl EnforceSecret for RabbitmqProperties {
    const ENFORCE_SECRET_PROPERTIES: Set<&'static str> = phf_set! {
        "url",
    };
}

impl SourceProperties for RabbitmqProperties {
    type Split = RabbitmqSplit;
    type SplitEnumerator = RabbitmqSplitEnumerator;
    type SplitReader = source::RabbitmqSplitReader;

    const SOURCE_NAME: &'static str = RABBITMQ_CONNECTOR;
}

impl crate::source::UnknownFields for RabbitmqProperties {
    fn unknown_fields(&self) -> HashMap<String, String> {
        self.unknown_fields.clone()
    }
}

impl RabbitmqProperties {
    pub fn queue_names(&self) -> ConnectorResult<Vec<String>> {
        ensure!(
            self.queue.is_some() ^ self.queues.is_some(),
            "exactly one of `queue` or `queues` must be specified for RabbitMQ source"
        );
        let raw = self.queue.clone().or_else(|| self.queues.clone()).unwrap();
        let queues = raw
            .split(',')
            .map(str::trim)
            .filter(|queue| !queue.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        ensure!(
            !queues.is_empty(),
            "RabbitMQ source requires at least one non-empty queue"
        );
        let mut seen_queues = HashSet::with_capacity(queues.len());
        for queue in &queues {
            ensure!(
                seen_queues.insert(queue),
                "RabbitMQ source queue `{queue}` is listed more than once; use at most one consumer per queue"
            );
        }
        Ok(queues)
    }

    pub fn prefetch_count(&self) -> u16 {
        self.prefetch_count.unwrap_or(DEFAULT_PREFETCH_COUNT)
    }

    pub fn prefetch_size(&self) -> u32 {
        self.prefetch_size.unwrap_or(DEFAULT_PREFETCH_SIZE)
    }

    pub fn max_connections(&self) -> usize {
        self.max_connections.unwrap_or(DEFAULT_MAX_CONNECTIONS)
    }

    pub fn connection_attempts(&self) -> usize {
        self.connection_attempts
            .unwrap_or(DEFAULT_CONNECTION_ATTEMPTS)
            .max(1)
    }

    pub fn retry_delay(&self) -> Duration {
        Duration::from_secs(self.retry_delay.unwrap_or(DEFAULT_RETRY_DELAY_SECONDS))
    }

    pub fn socket_timeout(&self) -> Duration {
        Duration::from_secs(
            self.socket_timeout
                .unwrap_or(DEFAULT_SOCKET_TIMEOUT_SECONDS),
        )
    }

    pub fn blocked_connection_timeout(&self) -> Duration {
        Duration::from_secs(
            self.blocked_connection_timeout
                .unwrap_or(DEFAULT_BLOCKED_CONNECTION_TIMEOUT_SECONDS),
        )
    }

    pub fn heartbeat_interval(&self) -> u16 {
        self.heartbeat_interval
            .unwrap_or(DEFAULT_HEARTBEAT_INTERVAL_SECONDS)
    }

    pub fn frame_max(&self) -> u32 {
        self.frame_max.unwrap_or(DEFAULT_FRAME_MAX)
    }

    pub fn queue_passive(&self) -> bool {
        self.queue_passive.unwrap_or(true)
    }

    pub fn consumer_tag_prefix(&self) -> &str {
        self.consumer_tag_prefix.as_deref().unwrap_or("rw-rabbitmq")
    }

    pub fn validate(&self) -> ConnectorResult<()> {
        self.queue_names()?;
        ensure!(
            self.prefetch_size() == 0,
            "RabbitMQ source only supports prefetch_size=0 because RabbitMQ ignores non-zero prefetch sizes"
        );
        ensure!(
            (1..=MAX_PREFETCH_COUNT).contains(&self.prefetch_count()),
            "RabbitMQ source prefetch_count must be between 1 and 1000 because RabbitMQ treats 0 as unlimited"
        );
        ensure!(
            self.max_connections() > 0,
            "RabbitMQ source max_connections must be greater than 0"
        );
        ensure!(
            self.max_connections() <= DEFAULT_MAX_CONNECTIONS,
            "RabbitMQ source max_connections must be <= 5 to honor broker vhost connection limits"
        );
        ensure!(
            self.blocked_connection_timeout() > Duration::ZERO,
            "RabbitMQ source blocked_connection_timeout must be greater than 0"
        );
        ensure!(
            self.queue_passive(),
            "RabbitMQ source only supports queue.passive=true because queue lifecycle is owned by the broker/user"
        );
        Ok(())
    }

    pub fn connection_url(&self) -> ConnectorResult<String> {
        let mut url = Url::parse(&self.url).context("invalid RabbitMQ url")?;
        match url.scheme() {
            "amqp" | "amqps" => {}
            scheme => bail!("RabbitMQ source URL must use amqp:// or amqps://, got {scheme}://"),
        }

        let query_pairs = url
            .query_pairs()
            .filter(|(key, _)| key != "heartbeat" && key != "frame_max")
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect::<Vec<_>>();
        url.set_query(None);
        {
            let mut query = url.query_pairs_mut();
            for (key, value) in query_pairs {
                query.append_pair(&key, &value);
            }
            query.append_pair("heartbeat", &self.heartbeat_interval().to_string());
            query.append_pair("frame_max", &self.frame_max().to_string());
        }
        Ok(url.to_string())
    }

    pub async fn connect(&self) -> ConnectorResult<Connection> {
        self.validate()?;
        let url = self.connection_url()?;
        let strategy = FixedInterval::new(self.retry_delay()).take(self.connection_attempts());
        let props = ConnectionProperties::default();
        Retry::spawn(strategy, || {
            let url = url.clone();
            let props = props.clone();
            async move {
                tokio::time::timeout(
                    self.socket_timeout(),
                    Connection::connect(url.as_str(), props),
                )
                .await
                .map_err(|_| {
                    anyhow!(
                        "timed out connecting to RabbitMQ after {:?}",
                        self.socket_timeout()
                    )
                })?
                .context("failed to connect to RabbitMQ")
            }
        })
        .await
        .map_err(Into::into)
    }

    pub async fn check_queues(
        &self,
        connection: &Connection,
        queues: &[String],
    ) -> ConnectorResult<()> {
        for queue in queues {
            let channel = connection.create_channel().await?;
            channel
                .queue_declare(
                    queue.as_str(),
                    QueueDeclareOptions {
                        passive: self.queue_passive(),
                        durable: false,
                        exclusive: false,
                        auto_delete: false,
                        nowait: false,
                    },
                    FieldTable::default(),
                )
                .await
                .with_context(|| format!("failed to declare/check RabbitMQ queue `{queue}`"))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::source::TryFromBTreeMap;

    fn parse(extra: &[(&str, &str)]) -> RabbitmqProperties {
        let mut props = BTreeMap::from([
            (
                "url".to_owned(),
                "amqp://guest:guest@localhost:5672/%2f".to_owned(),
            ),
            ("queue".to_owned(), "q1".to_owned()),
        ]);
        props.extend(
            extra
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned())),
        );
        RabbitmqProperties::try_from_btreemap(props, true).unwrap()
    }

    #[test]
    fn parse_defaults() {
        let props = parse(&[]);
        assert_eq!(props.queue_names().unwrap(), vec!["q1"]);
        assert_eq!(props.prefetch_count(), 50);
        assert_eq!(props.prefetch_size(), 0);
        assert_eq!(props.max_connections(), 5);
        props.validate().unwrap();
    }

    #[test]
    fn parse_multi_queue() {
        let mut props = parse(&[("queues", " q1, q2 ,,q3 ")]);
        props.queue = None;
        assert_eq!(props.queue_names().unwrap(), vec!["q1", "q2", "q3"]);
    }

    #[test]
    fn reject_duplicate_queues() {
        let mut props = parse(&[("queues", "q1,q2,q1")]);
        props.queue = None;
        assert!(
            props
                .queue_names()
                .unwrap_err()
                .to_string()
                .contains("listed more than once")
        );
    }

    #[test]
    fn reject_non_zero_prefetch_size() {
        let props = parse(&[("prefetch_size", "1")]);
        assert!(
            props
                .validate()
                .unwrap_err()
                .to_string()
                .contains("prefetch_size=0")
        );
    }

    #[test]
    fn reject_zero_prefetch_count() {
        let props = parse(&[("prefetch_count", "0")]);
        assert!(
            props
                .validate()
                .unwrap_err()
                .to_string()
                .contains("between 1 and 1000")
        );
    }

    #[test]
    fn reject_too_many_connections() {
        let props = parse(&[("max_connections", "6")]);
        assert!(
            props
                .validate()
                .unwrap_err()
                .to_string()
                .contains("max_connections")
        );
    }

    #[test]
    fn reject_queue_creation() {
        let props = parse(&[("queue.passive", "false")]);
        assert!(
            props
                .validate()
                .unwrap_err()
                .to_string()
                .contains("queue.passive=true")
        );
    }

    #[test]
    fn reject_zero_blocked_connection_timeout() {
        let props = parse(&[("blocked_connection_timeout", "0")]);
        assert!(
            props
                .validate()
                .unwrap_err()
                .to_string()
                .contains("blocked_connection_timeout")
        );
    }

    #[test]
    fn connection_url_overrides_heartbeat_and_frame_max_query() {
        let props = parse(&[
            (
                "url",
                "amqps://guest:guest@example.com:5671/vhost?heartbeat=10&frame_max=4096&locale=en_US",
            ),
            ("heartbeat_interval", "600"),
            ("frame_max", "131072"),
        ]);

        let url = props.connection_url().unwrap();
        assert!(url.contains("locale=en_US"));
        assert!(url.contains("heartbeat=600"));
        assert!(url.contains("frame_max=131072"));
        assert!(!url.contains("heartbeat=10"));
        assert!(!url.contains("frame_max=4096"));
    }
}

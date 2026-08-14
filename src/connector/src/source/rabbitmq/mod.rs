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
use std::net::{Shutdown, TcpStream};
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, anyhow};
pub use enumerator::RabbitmqSplitEnumerator;
#[cfg(not(madsim))]
use lapin::AsyncTcpStream;
use lapin::options::QueueDeclareOptions;
#[cfg(not(madsim))]
use lapin::tcp::OwnedTLSConfig;
use lapin::types::FieldTable;
#[cfg(not(madsim))]
use lapin::uri::{AMQPScheme, AMQPUri};
use lapin::{Connection, ConnectionProperties};
use phf::{Set, phf_set};
use risingwave_common::{bail, ensure};
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
pub use source::*;
pub use split::RabbitmqSplit;
use thiserror_ext::AsReport;
use tokio_retry::Retry;
use tokio_retry::strategy::FixedInterval;
use url::Url;
use with_options::WithOptions;

use crate::enforce_secret::EnforceSecret;
use crate::error::ConnectorResult;
use crate::source::SourceProperties;

pub const RABBITMQ_CONNECTOR: &str = "rabbitmq";

pub(crate) struct RabbitmqConnection {
    connection: Connection,
    shutdown_stream: Arc<Mutex<Option<TcpStream>>>,
}

impl RabbitmqConnection {
    fn abort(&self) {
        let shutdown_stream = match self.shutdown_stream.lock() {
            Ok(shutdown_stream) => shutdown_stream,
            Err(error) => {
                tracing::debug!(
                    error = %error.as_report(),
                    "RabbitMQ TCP shutdown handle lock was poisoned"
                );
                return;
            }
        };
        let Some(shutdown_stream) = shutdown_stream.as_ref() else {
            tracing::debug!("RabbitMQ TCP shutdown handle was unavailable");
            return;
        };
        if let Err(error) = shutdown_stream.shutdown(Shutdown::Both) {
            tracing::debug!(
                error = %error.as_report(),
                "RabbitMQ TCP connection was already closed"
            );
        }
    }
}

impl Drop for RabbitmqConnection {
    fn drop(&mut self) {
        // Lapin deliberately stops every socket write while a broker connection is blocked.
        // Its automatic AMQP close is therefore queued forever in exactly the failure case where
        // this connector must discard the reader and requeue its unacked deliveries. Shutting down
        // a duplicate TCP handle breaks that deadlock without making normal reader shutdowns
        // abrupt.
        if self.connection.status().blocked() {
            self.abort();
        }
    }
}

impl Deref for RabbitmqConnection {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

#[cfg(all(unix, not(madsim)))]
fn clone_tcp_shutdown_handle(stream: &tokio::net::TcpStream) -> std::io::Result<TcpStream> {
    use std::os::fd::AsFd;

    Ok(stream.as_fd().try_clone_to_owned()?.into())
}

#[cfg(all(windows, not(madsim)))]
fn clone_tcp_shutdown_handle(stream: &tokio::net::TcpStream) -> std::io::Result<TcpStream> {
    use std::os::windows::io::AsSocket;

    Ok(stream.as_socket().try_clone_to_owned()?.into())
}

const DEFAULT_PREFETCH_COUNT: u16 = 50;
const MAX_PREFETCH_COUNT: u16 = 1000;
const DEFAULT_PREFETCH_SIZE: u32 = 0;
const DEFAULT_MAX_CONNECTIONS: usize = 5;
const MAX_CONNECTIONS: usize = 5;
const DEFAULT_CONNECTION_ATTEMPTS: usize = 5;
const DEFAULT_RETRY_DELAY_SECONDS: u64 = 2;
const DEFAULT_SOCKET_TIMEOUT_SECONDS: u64 = 10;
const DEFAULT_BLOCKED_CONNECTION_TIMEOUT_SECONDS: u64 = 300;
const DEFAULT_HEARTBEAT_INTERVAL_SECONDS: u16 = 600;
const DEFAULT_FRAME_MAX: u32 = 131_072;
const MAX_CONSUMER_TAG_PREFIX_BYTES: usize = 128;

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct RabbitmqProperties {
    /// AMQP broker URL. Both `amqp://` and `amqps://` are supported by the AMQP client.
    /// Credentials and vhost can be encoded in this URL.
    #[serde(rename = "url")]
    pub url: String,

    /// A single queue name to consume. Use either `queue` or `queues`. `RabbitMQ` queues use
    /// competing-consumer semantics; independent subscribers need separate queues bound to the
    /// publishing exchange.
    #[serde(rename = "queue")]
    pub queue: Option<String>,

    /// Comma-separated queue names. Each queue has at most one active consumer in one RW source.
    /// `RabbitMQ` distributes a queue's deliveries among every consumer, including consumers outside
    /// this source; independent subscribers need separate queues.
    #[serde(rename = "queues")]
    pub queues: Option<String>,

    /// `RabbitMQ` prefetch count. Valid range is 1-1000 because `RabbitMQ` treats 0
    /// as unlimited.
    #[serde(rename = "prefetch_count")]
    #[serde_as(as = "Option<DisplayFromStr>")]
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
        let queues = if let Some(queue) = &self.queue {
            let queue = queue.trim();
            ensure!(
                !queue.is_empty(),
                "RabbitMQ source requires at least one non-empty queue"
            );
            ensure!(
                !queue.contains(','),
                "RabbitMQ source `queue` accepts exactly one queue name; use `queues` for comma-separated queue names"
            );
            vec![queue.to_owned()]
        } else {
            self.queues
                .as_ref()
                .expect("checked above")
                .split(',')
                .map(str::trim)
                .filter(|queue| !queue.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        };
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
            self.max_connections() <= MAX_CONNECTIONS,
            "RabbitMQ source max_connections must be <= {MAX_CONNECTIONS} to honor broker vhost connection limits"
        );
        ensure!(
            self.connection_attempts() > 0,
            "RabbitMQ source connection_attempts must be greater than 0"
        );
        ensure!(
            self.blocked_connection_timeout() > Duration::ZERO,
            "RabbitMQ source blocked_connection_timeout must be greater than 0"
        );
        ensure!(
            self.queue_passive(),
            "RabbitMQ source only supports queue.passive=true because queue lifecycle is owned by the broker/user"
        );
        ensure!(
            self.consumer_tag_prefix().len() <= MAX_CONSUMER_TAG_PREFIX_BYTES,
            "RabbitMQ source consumer_tag_prefix must be at most {MAX_CONSUMER_TAG_PREFIX_BYTES} bytes"
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

    pub(crate) async fn connect(&self) -> ConnectorResult<RabbitmqConnection> {
        self.validate()?;
        let url = self.connection_url()?;
        // `Retry` always performs the action once, then consumes one delay per retry.
        let strategy = FixedInterval::new(self.retry_delay()).take(self.connection_attempts() - 1);
        let props = ConnectionProperties::default();
        Retry::spawn(strategy, || {
            let url = url.clone();
            let props = props.clone();
            async move {
                tokio::time::timeout(
                    self.socket_timeout(),
                    connect_with_shutdown_handle(url.as_str(), props),
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
                    queue.as_str().into(),
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

#[cfg(not(madsim))]
async fn connect_with_shutdown_handle(
    url: &str,
    properties: ConnectionProperties,
) -> ConnectorResult<RabbitmqConnection> {
    let uri = url
        .parse::<AMQPUri>()
        .map_err(|error| anyhow!("failed to parse RabbitMQ AMQP URI: {error}"))?;
    let runtime = lapin::runtime::default_runtime()?;
    let connector_runtime = runtime.clone();
    let shutdown_stream = Arc::new(Mutex::new(None));
    let connector_shutdown_stream = Arc::clone(&shutdown_stream);
    let connection = Connection::connector(
        uri,
        runtime,
        move |uri: AMQPUri, _runtime| {
            let runtime = connector_runtime.clone();
            let connector_shutdown_stream = Arc::clone(&connector_shutdown_stream);
            async move {
                let addresses =
                    runtime.to_socket_addrs((uri.authority.host.clone(), uri.authority.port));
                let connect = AsyncTcpStream::connect(&runtime, addresses);
                let stream = if let Some(timeout_ms) = uri.query.connection_timeout {
                    tokio::time::timeout(Duration::from_millis(timeout_ms), connect)
                        .await
                        .map_err(|_| {
                            lapin::Error::from(std::io::Error::new(
                                std::io::ErrorKind::TimedOut,
                                format!("RabbitMQ TCP connect timed out after {timeout_ms}ms"),
                            ))
                        })?
                        .map_err(lapin::Error::from)?
                } else {
                    connect.await.map_err(lapin::Error::from)?
                };
                let shutdown_handle = match &stream {
                    AsyncTcpStream::Plain(stream) => {
                        clone_tcp_shutdown_handle(stream.get_ref()).map_err(lapin::Error::from)?
                    }
                    _ => {
                        return Err(lapin::Error::from(std::io::Error::other(
                            "RabbitMQ connector expected a plain TCP stream before TLS setup",
                        )));
                    }
                };
                *connector_shutdown_stream.lock().map_err(|_| {
                    lapin::Error::from(std::io::Error::other(
                        "RabbitMQ TCP shutdown handle lock was poisoned",
                    ))
                })? = Some(shutdown_handle);

                match uri.scheme {
                    AMQPScheme::AMQP => Ok(stream),
                    AMQPScheme::AMQPS => {
                        let tls_config = OwnedTLSConfig::default();
                        stream
                            .into_tls(&uri.authority.host, tls_config.as_ref())
                            .await
                            .map_err(lapin::Error::from)
                    }
                }
            }
        },
        properties,
    )
    .await?;
    let has_shutdown_stream = shutdown_stream
        .lock()
        .map_err(|_| anyhow!("RabbitMQ TCP shutdown handle lock was poisoned"))?
        .is_some();
    if !has_shutdown_stream {
        return Err(anyhow!("RabbitMQ connection did not retain a TCP shutdown handle").into());
    }
    Ok(RabbitmqConnection {
        connection,
        shutdown_stream,
    })
}

#[cfg(madsim)]
async fn connect_with_shutdown_handle(
    url: &str,
    properties: ConnectionProperties,
) -> ConnectorResult<RabbitmqConnection> {
    Ok(RabbitmqConnection {
        connection: Connection::connect(url, properties).await?,
        shutdown_stream: Arc::new(Mutex::new(None)),
    })
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
    fn reject_comma_separated_single_queue() {
        let props = parse(&[("queue", "q1,q2")]);
        assert!(
            props
                .queue_names()
                .unwrap_err()
                .to_string()
                .contains("use `queues`")
        );
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
    fn reject_zero_connection_attempts() {
        let props = parse(&[("connection_attempts", "0")]);
        assert!(
            props
                .validate()
                .unwrap_err()
                .to_string()
                .contains("connection_attempts")
        );
    }

    #[test]
    fn connection_attempts_count_initial_try() {
        for attempts in [1, 2, 5] {
            let attempts = attempts.to_string();
            let props = parse(&[("connection_attempts", &attempts)]);
            props.validate().unwrap();
            assert_eq!(
                FixedInterval::new(props.retry_delay())
                    .take(props.connection_attempts() - 1)
                    .count()
                    + 1,
                props.connection_attempts()
            );
        }
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
    fn reject_too_long_consumer_tag_prefix() {
        let prefix = "x".repeat(MAX_CONSUMER_TAG_PREFIX_BYTES + 1);
        let props = parse(&[("consumer_tag_prefix", &prefix)]);
        assert!(
            props
                .validate()
                .unwrap_err()
                .to_string()
                .contains("consumer_tag_prefix")
        );
    }

    #[test]
    fn connection_url_overrides_heartbeat_and_frame_max_query() {
        let props = parse(&[
            (
                "url",
                "amqps://guest:guest@example.com:5671/vhost?heartbeat=10&frame_max=4096&connection_timeout=4321&locale=en_US",
            ),
            ("heartbeat_interval", "600"),
            ("frame_max", "131072"),
        ]);

        let url = props.connection_url().unwrap();
        assert!(url.contains("locale=en_US"));
        assert!(url.contains("connection_timeout=4321"));
        assert!(url.contains("heartbeat=600"));
        assert!(url.contains("frame_max=131072"));
        assert!(!url.contains("heartbeat=10"));
        assert!(!url.contains("frame_max=4096"));
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn cloned_shutdown_handle_terminates_socket() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let client = tokio::net::TcpStream::connect(address);
        let (client, accepted) = tokio::join!(client, listener.accept());
        let client = client.unwrap();
        let (server, _) = accepted.unwrap();
        let shutdown_handle = clone_tcp_shutdown_handle(&client).unwrap();

        shutdown_handle.shutdown(Shutdown::Both).unwrap();
        tokio::time::timeout(Duration::from_secs(1), server.readable())
            .await
            .expect("server should observe the forced TCP shutdown")
            .unwrap();
        let mut byte = [0_u8; 1];
        assert_eq!(server.try_read(&mut byte).unwrap(), 0);
    }
}

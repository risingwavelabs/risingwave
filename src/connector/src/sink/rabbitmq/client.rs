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

use std::time::Duration;

use anyhow::{Context, anyhow};
use lapin::options::ConfirmSelectOptions;
use lapin::uri::AMQPUri;
use lapin::{Channel, Connection, ConnectionProperties};

use super::RabbitMqConfig;
use crate::sink::{Result, SinkError};

/// A dedicated connection and confirm channel for one sink writer.
pub struct RabbitMqClient {
    // Retain the connection for the writer's lifetime.
    pub connection: Connection,
    pub channel: Channel,
}

impl RabbitMqClient {
    pub async fn connect(config: &RabbitMqConfig) -> Result<Self> {
        config.validate().map_err(SinkError::Config)?;
        let uri = connection_uri(config).map_err(SinkError::Config)?;
        let connect = async {
            // Auto-recovery is disabled by default. RisingWave owns reconnect and log replay.
            let connection = Connection::connect_uri(uri, ConnectionProperties::default())
                .await
                .context("failed to connect to RabbitMQ")?;
            let channel = connection
                .create_channel()
                .await
                .context("failed to create RabbitMQ channel")?;
            // Wait for SelectOk before exposing the channel to the writer.
            channel
                .confirm_select(ConfirmSelectOptions { nowait: false })
                .await
                .context("failed to enable RabbitMQ publisher confirms")?;
            anyhow::Ok(Self {
                connection,
                channel,
            })
        };
        tokio::time::timeout(Duration::from_millis(config.connect_timeout_ms), connect)
            .await
            .context("RabbitMQ connection and confirm channel setup timed out")
            .and_then(|result| result)
            .map_err(SinkError::RabbitMq)
    }
}

fn connection_uri(config: &RabbitMqConfig) -> anyhow::Result<AMQPUri> {
    // Parse without credentials, then assign them directly to avoid escaping mistakes.
    let mut uri: AMQPUri = config
        .url
        .parse()
        .map_err(|_| anyhow!("invalid RabbitMQ AMQP URL or virtual host"))?;
    let url = url::Url::parse(&config.url).map_err(|_| anyhow!("invalid RabbitMQ url"))?;
    if let Some(url::Host::Ipv6(address)) = url.host() {
        // AMQPUri's parser uses Url::domain(), which falls back to localhost for IPv6.
        // The transport takes a (host, port) tuple, so the address must have no brackets.
        uri.authority.host = address.to_string();
    }
    uri.authority.userinfo.username.clone_from(&config.username);
    uri.authority.userinfo.password.clone_from(&config.password);
    uri.query.heartbeat = Some(config.heartbeat);
    uri.query.connection_timeout = Some(config.connect_timeout_ms);
    Ok(uri)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use lapin::uri::AMQPScheme;

    use super::*;

    fn properties() -> BTreeMap<String, String> {
        BTreeMap::from([
            ("url".into(), "amqp://localhost".into()),
            ("username".into(), "guest".into()),
            ("password".into(), "guest".into()),
            ("exchange".into(), "".into()),
            ("routing_key".into(), "test".into()),
            ("type".into(), "append-only".into()),
        ])
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn times_out_when_broker_does_not_complete_handshake() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut props = properties();
        props.insert(
            "url".into(),
            format!("amqp://{}", listener.local_addr().unwrap()),
        );
        props.insert("connect_timeout_ms".into(), "100".into());
        let config = RabbitMqConfig::from_btreemap(props).unwrap();
        // Hold the accepted socket open without answering the AMQP handshake.
        let (result, socket) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(RabbitMqClient::connect(&config), listener.accept())
        })
        .await
        .expect("test broker was not contacted");
        assert!(socket.is_ok());
        let Err(error) = result else {
            panic!("connection unexpectedly succeeded");
        };
        assert!(error.to_string().contains("setup timed out"));
    }

    /// Run with RABBITMQ_URL, RABBITMQ_USERNAME and RABBITMQ_PASSWORD set.
    #[cfg(not(madsim))]
    #[tokio::test]
    #[ignore = "requires a RabbitMQ broker"]
    async fn connects_with_publisher_confirms() {
        let mut props = properties();
        for (key, env) in [
            ("url", "RABBITMQ_URL"),
            ("username", "RABBITMQ_USERNAME"),
            ("password", "RABBITMQ_PASSWORD"),
        ] {
            props.insert(key.into(), std::env::var(env).expect(env));
        }
        let config = RabbitMqConfig::from_btreemap(props).unwrap();
        let client = RabbitMqClient::connect(&config).await.unwrap();
        assert!(client.connection.status().connected());
        assert!(client.channel.status().connected());
        assert!(client.channel.status().confirm());
        client.connection.close(200, "OK".into()).await.unwrap();
    }

    #[test]
    fn builds_uri_with_literal_credentials_and_decoded_vhost() {
        let mut props = properties();
        props.insert("url".into(), "amqps://broker/team%2fservice".into());
        props.insert("username".into(), "user@tenant".into());
        props.insert("password".into(), "p@ss:/%?#".into());
        let config = RabbitMqConfig::from_btreemap(props).unwrap();
        let uri = connection_uri(&config).unwrap();
        assert_eq!(uri.scheme, AMQPScheme::AMQPS);
        assert_eq!(uri.authority.host, "broker");
        assert_eq!(uri.authority.port, 5671);
        assert_eq!(uri.vhost, "team/service");
        assert_eq!(uri.authority.userinfo.username, "user@tenant");
        assert_eq!(uri.authority.userinfo.password, "p@ss:/%?#");
        assert_eq!(uri.query.heartbeat, Some(30));
        assert_eq!(uri.query.connection_timeout, Some(10_000));
    }

    #[test]
    fn preserves_amqp_virtual_host_semantics() {
        for (url, vhost, port) in [
            ("amqp://broker", "/", 5672),
            ("amqp://broker/%2f", "/", 5672),
            ("amqp://broker/", "", 5672),
            ("amqp://broker:5673/custom", "custom", 5673),
        ] {
            let mut props = properties();
            props.insert("url".into(), url.into());
            let config = RabbitMqConfig::from_btreemap(props).unwrap();
            let uri = connection_uri(&config).unwrap();
            assert_eq!(uri.vhost, vhost);
            assert_eq!(uri.authority.port, port);
        }
    }

    #[test]
    fn preserves_ipv6_address() {
        let mut props = properties();
        props.insert("url".into(), "amqp://[2001:db8::1]:5673/%2f".into());
        let config = RabbitMqConfig::from_btreemap(props).unwrap();
        let uri = connection_uri(&config).unwrap();
        assert_eq!(uri.authority.host, "2001:db8::1");
        assert_eq!(uri.authority.port, 5673);
    }
}

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

use anyhow::Context;
use lapin::runtime::default_runtime;
use lapin::tcp::{OwnedIdentity, OwnedTLSConfig};
use lapin::{Connection, ConnectionProperties};
use phf::{Set, phf_set};
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use url::Url;
use with_options::WithOptions;

use crate::deserialize_bool_from_string;
use crate::enforce_secret::EnforceSecret;
use crate::error::ConnectorResult;

#[serde_as]
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct RabbitMQCommon {
    /// The broker endpoint used to establish the AMQP connection.
    /// Examples: `amqp://localhost:5672` or `amqps://mq.example.com:5671`.
    pub url: String,

    /// Username for broker authentication.
    /// If credentials are already embedded in `url`, this can be omitted.
    #[serde(rename = "username")]
    pub user: Option<String>,

    /// Password for broker authentication.
    /// This should be stored as a secret in cloud environments.
    pub password: Option<String>,

    /// RabbitMQ virtual host.
    /// If unset, broker-side default vhost is used.
    #[serde(rename = "virtual_host")]
    pub virtual_host: Option<String>,

    /// Path or content of the CA certificate used to verify broker certificates.
    /// Required when using a custom CA for `amqps`.
    #[serde(rename = "tls.ca")]
    pub ca: Option<String>,

    /// Path or content of the client certificate for mTLS authentication.
    #[serde(rename = "tls.client_cert")]
    pub client_cert: Option<String>,

    /// Path or content of the client private key for mTLS authentication.
    #[serde(rename = "tls.client_key")]
    pub client_key: Option<String>,

    /// AMQP heartbeat interval in seconds.
    /// Helps detect dead TCP connections earlier.
    #[serde(rename = "heartbeat_sec")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub heartbeat_sec: Option<u16>,

    /// Timeout for establishing the initial connection, in milliseconds.
    #[serde(rename = "connect_timeout_ms")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub connect_timeout_ms: Option<u64>,

    /// Whether the publisher waits for broker confirms after publish.
    /// Enabled by default for safer delivery semantics.
    #[serde(
        rename = "publisher_confirm",
        default = "default_publisher_confirm",
        deserialize_with = "deserialize_bool_from_string"
    )]
    pub publisher_confirm: bool,
}

impl EnforceSecret for RabbitMQCommon {
    const ENFORCE_SECRET_PROPERTIES: Set<&'static str> = phf_set! {
        "password",
        "tls.client_cert",
        "tls.client_key",
    };
}

const fn default_publisher_confirm() -> bool {
    true
}

impl RabbitMQCommon {
    pub async fn build_client(&self) -> ConnectorResult<Connection> {
        let mut url = Url::parse(&self.url)?;

        let ssl = matches!(url.scheme(), "amqps");
        let has_tls_options =
            self.ca.is_some() || self.client_cert.is_some() || self.client_key.is_some();

        if let Some(user) = &self.user {
            url.set_username(user).map_err(|()| {
                anyhow::anyhow!("invalid username for RabbitMQ url: contains forbidden characters")
            })?;
        }
        if let Some(password) = &self.password {
            url.set_password(Some(password)).map_err(|()| {
                anyhow::anyhow!("invalid password for RabbitMQ url: contains forbidden characters")
            })?;
        }
        if let Some(vhost) = &self.virtual_host {
            let encoded_vhost = urlencoding::encode(vhost);
            url.set_path(&format!("/{}", encoded_vhost));
        }

        if !ssl && has_tls_options {
            return Err(
                anyhow::anyhow!("TLS options are provided but URL scheme is not amqps").into(),
            );
        }

        // Keep URL query parameters if user already provided custom values.
        let has_heartbeat = url.query_pairs().any(|(k, _)| k == "heartbeat");
        let has_connection_timeout = url.query_pairs().any(|(k, _)| k == "connection_timeout");

        {
            let mut query = url.query_pairs_mut();
            if !has_heartbeat && let Some(heartbeat_sec) = self.heartbeat_sec {
                query.append_pair("heartbeat", &heartbeat_sec.to_string());
            }
            if !has_connection_timeout && let Some(connect_timeout_ms) = self.connect_timeout_ms {
                query.append_pair("connection_timeout", &connect_timeout_ms.to_string());
            }
        }

        let tls_config = if ssl {
            self.get_tls_config()?
        } else {
            OwnedTLSConfig::default()
        };

        let runtime = default_runtime()
            .map_err(|e| anyhow::anyhow!("failed to create lapin runtime: {e}"))?;

        Connection::connect_with_config(
            url.as_str(),
            ConnectionProperties::default(),
            tls_config,
            runtime,
        )
        .await
        .context("failed to connect RabbitMQ")
        .map_err(Into::into)
    }

    fn get_tls_config(&self) -> ConnectorResult<OwnedTLSConfig> {
        let cert_chain = self.ca.as_deref().map(read_pem_string).transpose()?;

        let identity = match (&self.client_cert, &self.client_key) {
            (None, None) => None,
            (Some(_), None) | (None, Some(_)) => {
                return Err(anyhow::anyhow!(
                    "`tls.client_cert` and `tls.client_key` must be set together"
                )
                .into());
            }
            (Some(client_cert), Some(client_key)) => Some(OwnedIdentity::PKCS8 {
                pem: read_pem_bytes(client_cert)?,
                key: read_pem_bytes(client_key)?,
            }),
        };

        Ok(OwnedTLSConfig {
            identity,
            cert_chain,
        })
    }
}

/// Load PEM content as UTF-8 string.
///
/// Supported inputs:
/// - `fs://<path>`: read PEM content from local file.
/// - inline PEM content: use the value directly.
fn read_pem_string(value: &str) -> ConnectorResult<String> {
    if let Some(path) = value.strip_prefix("fs://") {
        std::fs::read_to_string(path)
            .with_context(|| format!("failed to read pem file from {path}"))
            .map_err(Into::into)
    } else {
        Ok(value.to_owned())
    }
}

/// Load PEM content as raw bytes.
///
/// Supported inputs:
/// - `fs://<path>`: read PEM content from local file.
/// - inline PEM content: convert the value to bytes directly.
///
/// This is used for PKCS#8 identity fields (`pem` and `key`) required by `lapin`.
fn read_pem_bytes(value: &str) -> ConnectorResult<Vec<u8>> {
    if let Some(path) = value.strip_prefix("fs://") {
        std::fs::read(path)
            .with_context(|| format!("failed to read pem file from {path}"))
            .map_err(Into::into)
    } else {
        Ok(value.as_bytes().to_vec())
    }
}

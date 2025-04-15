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

use phf::{Set, phf_set};
use rumqttc::tokio_rustls::rustls;
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::mqttbytes::v5::ConnectProperties;
use rumqttc::v5::{AsyncClient, EventLoop, MqttOptions};
use serde_derive::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use strum_macros::{Display, EnumString};
use with_options::WithOptions;

use super::common::{load_certs, load_private_key};
use crate::deserialize_bool_from_string;
use crate::enforce_secret_on_cloud::EnforceSecretOnCloud;
use crate::error::ConnectorResult;

#[derive(Debug, Clone, PartialEq, Display, Deserialize, EnumString)]
#[strum(serialize_all = "snake_case")]
#[allow(clippy::enum_variant_names)]
pub enum QualityOfService {
    AtLeastOnce,
    AtMostOnce,
    ExactlyOnce,
}

#[serde_as]
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct MqttCommon {
    /// The url of the broker to connect to. e.g. tcp://localhost.
    /// Must be prefixed with one of either `tcp://`, `mqtt://`, `ssl://`,`mqtts://`,
    /// to denote the protocol for establishing a connection with the broker.
    /// `mqtts://`, `ssl://` will use the native certificates if no ca is specified
    pub url: String,

    /// The quality of service to use when publishing messages. Defaults to at_most_once.
    /// Could be at_most_once, at_least_once or exactly_once
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub qos: Option<QualityOfService>,

    /// Username for the mqtt broker
    #[serde(rename = "username")]
    pub user: Option<String>,

    /// Password for the mqtt broker
    pub password: Option<String>,

    /// Prefix for the mqtt client id.
    /// The client id will be generated as `client_prefix`_`actor_id`_`(executor_id|source_id)`. Defaults to risingwave
    pub client_prefix: Option<String>,

    /// `clean_start = true` removes all the state from queues & instructs the broker
    /// to clean all the client state when client disconnects.
    ///
    /// When set `false`, broker will hold the client state and performs pending
    /// operations on the client when reconnection with same `client_id`
    /// happens. Local queue state is also held to retransmit packets after reconnection.
    #[serde(default, deserialize_with = "deserialize_bool_from_string")]
    pub clean_start: bool,

    /// The maximum number of inflight messages. Defaults to 100
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub inflight_messages: Option<usize>,

    /// The max size of messages received by the MQTT client
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub max_packet_size: Option<u32>,

    /// Path to CA certificate file for verifying the broker's key.
    #[serde(rename = "tls.ca")]
    pub ca: Option<String>,
    /// Path to client's certificate file (PEM). Required for client authentication.
    /// Can be a file path under fs:// or a string with the certificate content.
    #[serde(rename = "tls.client_cert")]
    pub client_cert: Option<String>,

    /// Path to client's private key file (PEM). Required for client authentication.
    /// Can be a file path under fs:// or a string with the private key content.
    #[serde(rename = "tls.client_key")]
    pub client_key: Option<String>,
}

impl EnforceSecretOnCloud for MqttCommon {
    const ENFORCE_SECRET_PROPERTIES_ON_CLOUD: Set<&'static str> = phf_set! {
        "tls.client_cert",
        "tls.client_key",
        "password",
    };
}

impl MqttCommon {
    pub(crate) fn build_client(
        &self,
        actor_id: u32,
        id: u64,
    ) -> ConnectorResult<(AsyncClient, EventLoop)> {
        let client_id = format!(
            "{}_{}_{}",
            self.client_prefix.as_deref().unwrap_or("risingwave"),
            actor_id,
            id
        );

        let mut url = url::Url::parse(&self.url)?;

        let ssl = matches!(url.scheme(), "mqtts" | "ssl");

        url.query_pairs_mut().append_pair("client_id", &client_id);

        tracing::debug!("connecting mqtt using url: {}", url.as_str());

        let mut options = MqttOptions::try_from(url)?;
        options.set_keep_alive(std::time::Duration::from_secs(10));

        options.set_clean_start(self.clean_start);

        let mut connect_properties = ConnectProperties::new();
        connect_properties.max_packet_size = self.max_packet_size;
        options.set_connect_properties(connect_properties);

        if ssl {
            let tls_config = self.get_tls_config()?;
            options.set_transport(rumqttc::Transport::tls_with_config(
                rumqttc::TlsConfiguration::Rustls(std::sync::Arc::new(tls_config)),
            ));
        }

        if let Some(user) = &self.user {
            options.set_credentials(user, self.password.as_deref().unwrap_or_default());
        }

        Ok(rumqttc::v5::AsyncClient::new(
            options,
            self.inflight_messages.unwrap_or(100),
        ))
    }

    pub(crate) fn qos(&self) -> QoS {
        self.qos
            .as_ref()
            .map(|qos| match qos {
                QualityOfService::AtMostOnce => QoS::AtMostOnce,
                QualityOfService::AtLeastOnce => QoS::AtLeastOnce,
                QualityOfService::ExactlyOnce => QoS::ExactlyOnce,
            })
            .unwrap_or(QoS::AtMostOnce)
    }

    fn get_tls_config(&self) -> ConnectorResult<rustls::ClientConfig> {
        let mut root_cert_store = rustls::RootCertStore::empty();
        if let Some(ca) = &self.ca {
            let certificates = load_certs(ca)?;
            for cert in certificates {
                root_cert_store.add(cert).unwrap();
            }
        } else {
            for cert in
                rustls_native_certs::load_native_certs().expect("could not load platform certs")
            {
                root_cert_store.add(cert).unwrap();
            }
        }

        let builder = rustls::ClientConfig::builder().with_root_certificates(root_cert_store);

        let tls_config = if let (Some(client_cert), Some(client_key)) =
            (self.client_cert.as_ref(), self.client_key.as_ref())
        {
            let certs = load_certs(client_cert)?;
            let key = load_private_key(client_key)?;

            builder.with_client_auth_cert(certs, key)?
        } else {
            builder.with_no_client_auth()
        };

        Ok(tls_config)
    }
}

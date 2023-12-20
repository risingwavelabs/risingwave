// Copyright 2023 RisingWave Labs
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

use std::borrow::Cow;
use std::collections::HashMap;
use std::io::Write;
use std::time::Duration;

use anyhow::{anyhow, Ok};
use async_nats::jetstream::consumer::DeliverPolicy;
use async_nats::jetstream::{self};
use aws_sdk_kinesis::Client as KinesisClient;
use pulsar::authentication::oauth2::{OAuth2Authentication, OAuth2Params};
use pulsar::{Authentication, Pulsar, TokioExecutor};
use rdkafka::ClientConfig;
use risingwave_common::error::ErrorCode::InvalidParameterValue;
use risingwave_common::error::{anyhow_error, RwError};
use serde_derive::Deserialize;
use serde_with::json::JsonString;
use serde_with::{serde_as, DisplayFromStr};
use tempfile::NamedTempFile;
use time::OffsetDateTime;
use url::Url;
use with_options::WithOptions;

use crate::aws_utils::load_file_descriptor_from_s3;
use crate::deserialize_duration_from_string;
use crate::sink::SinkError;
use crate::source::nats::source::NatsOffset;
// The file describes the common abstractions for each connector and can be used in both source and
// sink.

pub const BROKER_REWRITE_MAP_KEY: &str = "broker.rewrite.endpoints";
pub const PRIVATE_LINK_TARGETS_KEY: &str = "privatelink.targets";

#[derive(Debug, Clone, Deserialize)]
pub struct AwsPrivateLinkItem {
    pub az_id: Option<String>,
    pub port: u16,
}

use aws_config::default_provider::region::DefaultRegionChain;
use aws_config::sts::AssumeRoleProvider;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_types::region::Region;
use aws_types::SdkConfig;

/// A flatten config map for aws auth.
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct AwsAuthProps {
    pub region: Option<String>,
    #[serde(alias = "endpoint_url")]
    pub endpoint: Option<String>,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub session_token: Option<String>,
    pub arn: Option<String>,
    /// This field was added for kinesis. Not sure if it's useful for other connectors.
    /// Please ignore it in the documentation for now.
    pub external_id: Option<String>,
    pub profile: Option<String>,
}

impl AwsAuthProps {
    async fn build_region(&self) -> anyhow::Result<Region> {
        if let Some(region_name) = &self.region {
            Ok(Region::new(region_name.clone()))
        } else {
            let mut region_chain = DefaultRegionChain::builder();
            if let Some(profile_name) = &self.profile {
                region_chain = region_chain.profile_name(profile_name);
            }

            Ok(region_chain
                .build()
                .region()
                .await
                .ok_or_else(|| anyhow::format_err!("region should be provided"))?)
        }
    }

    fn build_credential_provider(&self) -> anyhow::Result<SharedCredentialsProvider> {
        if self.access_key.is_some() && self.secret_key.is_some() {
            Ok(SharedCredentialsProvider::new(
                aws_credential_types::Credentials::from_keys(
                    self.access_key.as_ref().unwrap(),
                    self.secret_key.as_ref().unwrap(),
                    self.session_token.clone(),
                ),
            ))
        } else {
            Err(anyhow!(
                "Both \"access_key\" and \"secret_access\" are required."
            ))
        }
    }

    async fn with_role_provider(
        &self,
        credential: SharedCredentialsProvider,
    ) -> anyhow::Result<SharedCredentialsProvider> {
        if let Some(role_name) = &self.arn {
            let region = self.build_region().await?;
            let mut role = AssumeRoleProvider::builder(role_name)
                .session_name("RisingWave")
                .region(region);
            if let Some(id) = &self.external_id {
                role = role.external_id(id);
            }
            let provider = role.build_from_provider(credential).await;
            Ok(SharedCredentialsProvider::new(provider))
        } else {
            Ok(credential)
        }
    }

    pub async fn build_config(&self) -> anyhow::Result<SdkConfig> {
        let region = self.build_region().await?;
        let credentials_provider = self
            .with_role_provider(self.build_credential_provider()?)
            .await?;
        let mut config_loader = aws_config::from_env()
            .region(region)
            .credentials_provider(credentials_provider);

        if let Some(endpoint) = self.endpoint.as_ref() {
            config_loader = config_loader.endpoint_url(endpoint);
        }

        Ok(config_loader.load().await)
    }
}

#[serde_as]
#[derive(Debug, Clone, Deserialize, WithOptions)]
pub struct KafkaCommon {
    #[serde(rename = "properties.bootstrap.server", alias = "kafka.brokers")]
    pub brokers: String,

    #[serde(rename = "broker.rewrite.endpoints")]
    #[serde_as(as = "Option<JsonString>")]
    pub broker_rewrite_map: Option<HashMap<String, String>>,

    #[serde(rename = "topic", alias = "kafka.topic")]
    pub topic: String,

    #[serde(
        rename = "properties.sync.call.timeout",
        deserialize_with = "deserialize_duration_from_string",
        default = "default_kafka_sync_call_timeout"
    )]
    pub sync_call_timeout: Duration,

    /// Security protocol used for RisingWave to communicate with Kafka brokers. Could be
    /// PLAINTEXT, SSL, SASL_PLAINTEXT or SASL_SSL.
    #[serde(rename = "properties.security.protocol")]
    security_protocol: Option<String>,

    #[serde(rename = "properties.ssl.endpoint.identification.algorithm")]
    ssl_endpoint_identification_algorithm: Option<String>,

    // For the properties below, please refer to [librdkafka](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md) for more information.
    /// Path to CA certificate file for verifying the broker's key.
    #[serde(rename = "properties.ssl.ca.location")]
    ssl_ca_location: Option<String>,

    /// Path to client's certificate file (PEM).
    #[serde(rename = "properties.ssl.certificate.location")]
    ssl_certificate_location: Option<String>,

    /// Path to client's private key file (PEM).
    #[serde(rename = "properties.ssl.key.location")]
    ssl_key_location: Option<String>,

    /// Passphrase of client's private key.
    #[serde(rename = "properties.ssl.key.password")]
    ssl_key_password: Option<String>,

    /// SASL mechanism if SASL is enabled. Currently support PLAIN, SCRAM and GSSAPI.
    #[serde(rename = "properties.sasl.mechanism")]
    sasl_mechanism: Option<String>,

    /// SASL username for SASL/PLAIN and SASL/SCRAM.
    #[serde(rename = "properties.sasl.username")]
    sasl_username: Option<String>,

    /// SASL password for SASL/PLAIN and SASL/SCRAM.
    #[serde(rename = "properties.sasl.password")]
    sasl_password: Option<String>,

    /// Kafka server's Kerberos principal name under SASL/GSSAPI, not including /hostname@REALM.
    #[serde(rename = "properties.sasl.kerberos.service.name")]
    sasl_kerberos_service_name: Option<String>,

    /// Path to client's Kerberos keytab file under SASL/GSSAPI.
    #[serde(rename = "properties.sasl.kerberos.keytab")]
    sasl_kerberos_keytab: Option<String>,

    /// Client's Kerberos principal name under SASL/GSSAPI.
    #[serde(rename = "properties.sasl.kerberos.principal")]
    sasl_kerberos_principal: Option<String>,

    /// Shell command to refresh or acquire the client's Kerberos ticket under SASL/GSSAPI.
    #[serde(rename = "properties.sasl.kerberos.kinit.cmd")]
    sasl_kerberos_kinit_cmd: Option<String>,

    /// Minimum time in milliseconds between key refresh attempts under SASL/GSSAPI.
    #[serde(rename = "properties.sasl.kerberos.min.time.before.relogin")]
    sasl_kerberos_min_time_before_relogin: Option<String>,

    /// Configurations for SASL/OAUTHBEARER.
    #[serde(rename = "properties.sasl.oauthbearer.config")]
    sasl_oathbearer_config: Option<String>,

    #[serde(flatten)]
    pub rdkafka_properties: RdKafkaPropertiesCommon,
}

const fn default_kafka_sync_call_timeout() -> Duration {
    Duration::from_secs(5)
}

#[serde_as]
#[derive(Debug, Clone, Deserialize, WithOptions)]
pub struct RdKafkaPropertiesCommon {
    /// Maximum Kafka protocol request message size. Due to differing framing overhead between
    /// protocol versions the producer is unable to reliably enforce a strict max message limit at
    /// produce time and may exceed the maximum size by one message in protocol ProduceRequests,
    /// the broker will enforce the the topic's max.message.bytes limit
    #[serde(rename = "properties.message.max.bytes")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub message_max_bytes: Option<usize>,

    /// Maximum Kafka protocol response message size. This serves as a safety precaution to avoid
    /// memory exhaustion in case of protocol hickups. This value must be at least fetch.max.bytes
    /// + 512 to allow for protocol overhead; the value is adjusted automatically unless the
    /// configuration property is explicitly set.
    #[serde(rename = "properties.receive.message.max.bytes")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub receive_message_max_bytes: Option<usize>,

    #[serde(rename = "properties.statistics.interval.ms")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub statistics_interval_ms: Option<usize>,

    /// Client identifier
    #[serde(rename = "properties.client.id")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub client_id: Option<String>,
}

impl RdKafkaPropertiesCommon {
    pub(crate) fn set_client(&self, c: &mut rdkafka::ClientConfig) {
        if let Some(v) = self.statistics_interval_ms {
            c.set("statistics.interval.ms", v.to_string());
        }
        if let Some(v) = self.message_max_bytes {
            c.set("message.max.bytes", v.to_string());
        }
        if let Some(v) = self.receive_message_max_bytes {
            c.set("receive.message.max.bytes", v.to_string());
        }
        if let Some(v) = self.client_id.as_ref() {
            c.set("client.id", v);
        }
    }
}

impl KafkaCommon {
    pub(crate) fn set_security_properties(&self, config: &mut ClientConfig) {
        // Security protocol
        if let Some(security_protocol) = self.security_protocol.as_ref() {
            config.set("security.protocol", security_protocol);
        }

        // SSL
        if let Some(ssl_ca_location) = self.ssl_ca_location.as_ref() {
            config.set("ssl.ca.location", ssl_ca_location);
        }
        if let Some(ssl_certificate_location) = self.ssl_certificate_location.as_ref() {
            config.set("ssl.certificate.location", ssl_certificate_location);
        }
        if let Some(ssl_key_location) = self.ssl_key_location.as_ref() {
            config.set("ssl.key.location", ssl_key_location);
        }
        if let Some(ssl_key_password) = self.ssl_key_password.as_ref() {
            config.set("ssl.key.password", ssl_key_password);
        }
        if let Some(ssl_endpoint_identification_algorithm) =
            self.ssl_endpoint_identification_algorithm.as_ref()
        {
            // accept only `none` and `http` here, let the sdk do the check
            config.set(
                "ssl.endpoint.identification.algorithm",
                ssl_endpoint_identification_algorithm,
            );
        }

        // SASL mechanism
        if let Some(sasl_mechanism) = self.sasl_mechanism.as_ref() {
            config.set("sasl.mechanism", sasl_mechanism);
        }

        // SASL/PLAIN & SASL/SCRAM
        if let Some(sasl_username) = self.sasl_username.as_ref() {
            config.set("sasl.username", sasl_username);
        }
        if let Some(sasl_password) = self.sasl_password.as_ref() {
            config.set("sasl.password", sasl_password);
        }

        // SASL/GSSAPI
        if let Some(sasl_kerberos_service_name) = self.sasl_kerberos_service_name.as_ref() {
            config.set("sasl.kerberos.service.name", sasl_kerberos_service_name);
        }
        if let Some(sasl_kerberos_keytab) = self.sasl_kerberos_keytab.as_ref() {
            config.set("sasl.kerberos.keytab", sasl_kerberos_keytab);
        }
        if let Some(sasl_kerberos_principal) = self.sasl_kerberos_principal.as_ref() {
            config.set("sasl.kerberos.principal", sasl_kerberos_principal);
        }
        if let Some(sasl_kerberos_kinit_cmd) = self.sasl_kerberos_kinit_cmd.as_ref() {
            config.set("sasl.kerberos.kinit.cmd", sasl_kerberos_kinit_cmd);
        }
        if let Some(sasl_kerberos_min_time_before_relogin) =
            self.sasl_kerberos_min_time_before_relogin.as_ref()
        {
            config.set(
                "sasl.kerberos.min.time.before.relogin",
                sasl_kerberos_min_time_before_relogin,
            );
        }

        // SASL/OAUTHBEARER
        if let Some(sasl_oathbearer_config) = self.sasl_oathbearer_config.as_ref() {
            config.set("sasl.oauthbearer.config", sasl_oathbearer_config);
        }
        // Currently, we only support unsecured OAUTH.
        config.set("enable.sasl.oauthbearer.unsecure.jwt", "true");
    }

    pub(crate) fn set_client(&self, c: &mut rdkafka::ClientConfig) {
        self.rdkafka_properties.set_client(c);
    }
}

#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct PulsarCommon {
    #[serde(rename = "topic", alias = "pulsar.topic")]
    pub topic: String,

    #[serde(rename = "service.url", alias = "pulsar.service.url")]
    pub service_url: String,

    #[serde(rename = "auth.token")]
    pub auth_token: Option<String>,

    #[serde(flatten)]
    pub oauth: Option<PulsarOauthCommon>,
}

#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct PulsarOauthCommon {
    #[serde(rename = "oauth.issuer.url")]
    pub issuer_url: String,

    #[serde(rename = "oauth.credentials.url")]
    pub credentials_url: String,

    #[serde(rename = "oauth.audience")]
    pub audience: String,

    #[serde(rename = "oauth.scope")]
    pub scope: Option<String>,

    #[serde(flatten)]
    pub aws_auth_props: AwsAuthProps,
}

impl PulsarCommon {
    pub(crate) async fn build_client(&self) -> anyhow::Result<Pulsar<TokioExecutor>> {
        let mut pulsar_builder = Pulsar::builder(&self.service_url, TokioExecutor);
        let mut temp_file = None;
        if let Some(oauth) = &self.oauth {
            let url = Url::parse(&oauth.credentials_url)?;
            match url.scheme() {
                "s3" => {
                    let credentials =
                        load_file_descriptor_from_s3(&url, &oauth.aws_auth_props).await?;
                    let mut f = NamedTempFile::new()?;
                    f.write_all(&credentials)?;
                    f.as_file().sync_all()?;
                    temp_file = Some(f);
                }
                "file" => {}
                _ => {
                    return Err(RwError::from(InvalidParameterValue(String::from(
                        "invalid credentials_url, only file url and s3 url are supported",
                    )))
                    .into());
                }
            }

            let auth_params = OAuth2Params {
                issuer_url: oauth.issuer_url.clone(),
                credentials_url: if temp_file.is_none() {
                    oauth.credentials_url.clone()
                } else {
                    let mut raw_path = temp_file
                        .as_ref()
                        .unwrap()
                        .path()
                        .to_str()
                        .unwrap()
                        .to_string();
                    raw_path.insert_str(0, "file://");
                    raw_path
                },
                audience: Some(oauth.audience.clone()),
                scope: oauth.scope.clone(),
            };

            pulsar_builder = pulsar_builder
                .with_auth_provider(OAuth2Authentication::client_credentials(auth_params));
        } else if let Some(auth_token) = &self.auth_token {
            pulsar_builder = pulsar_builder.with_auth(Authentication {
                name: "token".to_string(),
                data: Vec::from(auth_token.as_str()),
            });
        }

        let res = pulsar_builder.build().await.map_err(|e| anyhow!(e))?;
        drop(temp_file);
        Ok(res)
    }
}

#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct KinesisCommon {
    #[serde(rename = "stream", alias = "kinesis.stream.name")]
    pub stream_name: String,
    #[serde(rename = "aws.region", alias = "kinesis.stream.region")]
    pub stream_region: String,
    #[serde(rename = "endpoint", alias = "kinesis.endpoint")]
    pub endpoint: Option<String>,
    #[serde(
        rename = "aws.credentials.access_key_id",
        alias = "kinesis.credentials.access"
    )]
    pub credentials_access_key: Option<String>,
    #[serde(
        rename = "aws.credentials.secret_access_key",
        alias = "kinesis.credentials.secret"
    )]
    pub credentials_secret_access_key: Option<String>,
    #[serde(
        rename = "aws.credentials.session_token",
        alias = "kinesis.credentials.session_token"
    )]
    pub session_token: Option<String>,
    #[serde(rename = "aws.credentials.role.arn", alias = "kinesis.assumerole.arn")]
    pub assume_role_arn: Option<String>,
    #[serde(
        rename = "aws.credentials.role.external_id",
        alias = "kinesis.assumerole.external_id"
    )]
    pub assume_role_external_id: Option<String>,
}

impl KinesisCommon {
    pub(crate) async fn build_client(&self) -> anyhow::Result<KinesisClient> {
        let config = AwsAuthProps {
            region: Some(self.stream_region.clone()),
            endpoint: self.endpoint.clone(),
            access_key: self.credentials_access_key.clone(),
            secret_key: self.credentials_secret_access_key.clone(),
            session_token: self.session_token.clone(),
            arn: self.assume_role_arn.clone(),
            external_id: self.assume_role_external_id.clone(),
            profile: Default::default(),
        };
        let aws_config = config.build_config().await?;
        let mut builder = aws_sdk_kinesis::config::Builder::from(&aws_config);
        if let Some(endpoint) = &config.endpoint {
            builder = builder.endpoint_url(endpoint);
        }
        Ok(KinesisClient::from_conf(builder.build()))
    }
}
#[derive(Debug, Deserialize)]
pub struct UpsertMessage<'a> {
    #[serde(borrow)]
    pub primary_key: Cow<'a, [u8]>,
    #[serde(borrow)]
    pub record: Cow<'a, [u8]>,
}

#[serde_as]
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct NatsCommon {
    #[serde(rename = "server_url")]
    pub server_url: String,
    #[serde(rename = "subject")]
    pub subject: String,
    #[serde(rename = "connect_mode")]
    pub connect_mode: String,
    #[serde(rename = "username")]
    pub user: Option<String>,
    #[serde(rename = "password")]
    pub password: Option<String>,
    #[serde(rename = "jwt")]
    pub jwt: Option<String>,
    #[serde(rename = "nkey")]
    pub nkey: Option<String>,
    #[serde(rename = "max_bytes")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub max_bytes: Option<i64>,
    #[serde(rename = "max_messages")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub max_messages: Option<i64>,
    #[serde(rename = "max_messages_per_subject")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub max_messages_per_subject: Option<i64>,
    #[serde(rename = "max_consumers")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub max_consumers: Option<i32>,
    #[serde(rename = "max_message_size")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub max_message_size: Option<i32>,
}

impl NatsCommon {
    pub(crate) async fn build_client(&self) -> anyhow::Result<async_nats::Client> {
        let mut connect_options = async_nats::ConnectOptions::new();
        match self.connect_mode.as_str() {
            "user_and_password" => {
                if let (Some(v_user), Some(v_password)) =
                    (self.user.as_ref(), self.password.as_ref())
                {
                    connect_options =
                        connect_options.user_and_password(v_user.into(), v_password.into())
                } else {
                    return Err(anyhow_error!(
                        "nats connect mode is user_and_password, but user or password is empty"
                    ));
                }
            }

            "credential" => {
                if let (Some(v_nkey), Some(v_jwt)) = (self.nkey.as_ref(), self.jwt.as_ref()) {
                    connect_options = connect_options
                        .credentials(&self.create_credential(v_nkey, v_jwt)?)
                        .expect("failed to parse static creds")
                } else {
                    return Err(anyhow_error!(
                        "nats connect mode is credential, but nkey or jwt is empty"
                    ));
                }
            }
            "plain" => {}
            _ => {
                return Err(anyhow_error!(
                    "nats connect mode only accept user_and_password/credential/plain"
                ));
            }
        };

        let servers = self.server_url.split(',').collect::<Vec<&str>>();
        let client = connect_options
            .connect(
                servers
                    .iter()
                    .map(|url| url.parse())
                    .collect::<Result<Vec<async_nats::ServerAddr>, _>>()?,
            )
            .await
            .map_err(|e| SinkError::Nats(anyhow_error!("build nats client error: {:?}", e)))?;
        Ok(client)
    }

    pub(crate) async fn build_context(&self) -> anyhow::Result<jetstream::Context> {
        let client = self.build_client().await?;
        let jetstream = async_nats::jetstream::new(client);
        Ok(jetstream)
    }

    pub(crate) async fn build_consumer(
        &self,
        stream: String,
        split_id: String,
        start_sequence: NatsOffset,
    ) -> anyhow::Result<
        async_nats::jetstream::consumer::Consumer<async_nats::jetstream::consumer::pull::Config>,
    > {
        let context = self.build_context().await?;
        let stream = self.build_or_get_stream(context.clone(), stream).await?;
        let subject_name = self
            .subject
            .replace(',', "-")
            .replace(['.', '>', '*', ' ', '\t'], "_");
        let name = format!("risingwave-consumer-{}-{}", subject_name, split_id);
        let mut config = jetstream::consumer::pull::Config {
            ack_policy: jetstream::consumer::AckPolicy::None,
            ..Default::default()
        };

        let deliver_policy = match start_sequence {
            NatsOffset::Earliest => DeliverPolicy::All,
            NatsOffset::Latest => DeliverPolicy::Last,
            NatsOffset::SequenceNumber(v) => {
                let parsed = v.parse::<u64>()?;
                DeliverPolicy::ByStartSequence {
                    start_sequence: 1 + parsed,
                }
            }
            NatsOffset::Timestamp(v) => DeliverPolicy::ByStartTime {
                start_time: OffsetDateTime::from_unix_timestamp_nanos(v * 1_000_000)?,
            },
            NatsOffset::None => DeliverPolicy::All,
        };
        let consumer = stream
            .get_or_create_consumer(&name, {
                config.deliver_policy = deliver_policy;
                config
            })
            .await?;
        Ok(consumer)
    }

    pub(crate) async fn build_or_get_stream(
        &self,
        jetstream: jetstream::Context,
        stream: String,
    ) -> anyhow::Result<jetstream::stream::Stream> {
        let subjects: Vec<String> = self.subject.split(',').map(|s| s.to_string()).collect();
        let mut config = jetstream::stream::Config {
            name: stream,
            max_bytes: 1000000,
            subjects,
            ..Default::default()
        };
        if let Some(v) = self.max_bytes {
            config.max_bytes = v;
        }
        if let Some(v) = self.max_messages {
            config.max_messages = v;
        }
        if let Some(v) = self.max_messages_per_subject {
            config.max_messages_per_subject = v;
        }
        if let Some(v) = self.max_consumers {
            config.max_consumers = v;
        }
        if let Some(v) = self.max_message_size {
            config.max_message_size = v;
        }
        let stream = jetstream.get_or_create_stream(config).await?;
        Ok(stream)
    }

    pub(crate) fn create_credential(&self, seed: &str, jwt: &str) -> anyhow::Result<String> {
        let creds = format!(
            "-----BEGIN NATS USER JWT-----\n{}\n------END NATS USER JWT------\n\n\
                         ************************* IMPORTANT *************************\n\
                         NKEY Seed printed below can be used to sign and prove identity.\n\
                         NKEYs are sensitive and should be treated as secrets.\n\n\
                         -----BEGIN USER NKEY SEED-----\n{}\n------END USER NKEY SEED------\n\n\
                         *************************************************************",
            jwt, seed
        );
        Ok(creds)
    }
}

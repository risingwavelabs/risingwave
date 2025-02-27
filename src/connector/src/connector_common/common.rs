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
use std::hash::Hash;
use std::io::Write;
use std::time::Duration;

use anyhow::{Context, anyhow};
use async_nats::jetstream::consumer::DeliverPolicy;
use async_nats::jetstream::{self};
use aws_sdk_kinesis::Client as KinesisClient;
use pulsar::authentication::oauth2::{OAuth2Authentication, OAuth2Params};
use pulsar::{Authentication, Pulsar, TokioExecutor};
use rdkafka::ClientConfig;
use risingwave_common::bail;
use serde_derive::Deserialize;
use serde_with::json::JsonString;
use serde_with::{DisplayFromStr, serde_as};
use tempfile::NamedTempFile;
use time::OffsetDateTime;
use url::Url;
use with_options::WithOptions;

use crate::aws_utils::load_file_descriptor_from_s3;
use crate::deserialize_duration_from_string;
use crate::error::ConnectorResult;
use crate::sink::SinkError;
use crate::source::nats::source::NatsOffset;

pub const PRIVATE_LINK_BROKER_REWRITE_MAP_KEY: &str = "broker.rewrite.endpoints";
pub const PRIVATE_LINK_TARGETS_KEY: &str = "privatelink.targets";

const AWS_MSK_IAM_AUTH: &str = "AWS_MSK_IAM";

/// The environment variable to disable using default credential from environment.
/// It's recommended to set this variable to `false` in cloud hosting environment.
const DISABLE_DEFAULT_CREDENTIAL: &str = "DISABLE_DEFAULT_CREDENTIAL";

#[derive(Debug, Clone, Deserialize)]
pub struct AwsPrivateLinkItem {
    pub az_id: Option<String>,
    pub port: u16,
}

use aws_config::default_provider::region::DefaultRegionChain;
use aws_config::sts::AssumeRoleProvider;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_types::SdkConfig;
use aws_types::region::Region;
use risingwave_common::util::env_var::env_var_is_true;

/// A flatten config map for aws auth.
#[derive(Deserialize, Debug, Clone, WithOptions, PartialEq)]
pub struct AwsAuthProps {
    #[serde(rename = "aws.region", alias = "region", alias = "s3.region")]
    pub region: Option<String>,

    #[serde(
        rename = "aws.endpoint_url",
        alias = "endpoint_url",
        alias = "endpoint",
        alias = "s3.endpoint"
    )]
    pub endpoint: Option<String>,
    #[serde(
        rename = "aws.credentials.access_key_id",
        alias = "access_key",
        alias = "s3.access.key"
    )]
    pub access_key: Option<String>,
    #[serde(
        rename = "aws.credentials.secret_access_key",
        alias = "secret_key",
        alias = "s3.secret.key"
    )]
    pub secret_key: Option<String>,
    #[serde(rename = "aws.credentials.session_token", alias = "session_token")]
    pub session_token: Option<String>,
    /// IAM role
    #[serde(rename = "aws.credentials.role.arn", alias = "arn")]
    pub arn: Option<String>,
    /// external ID in IAM role trust policy
    #[serde(rename = "aws.credentials.role.external_id", alias = "external_id")]
    pub external_id: Option<String>,
    #[serde(rename = "aws.profile", alias = "profile")]
    pub profile: Option<String>,
    #[serde(rename = "aws.msk.signer_timeout_sec")]
    pub msk_signer_timeout_sec: Option<u64>,
}

impl AwsAuthProps {
    async fn build_region(&self) -> ConnectorResult<Region> {
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
                .context("region should be provided")?)
        }
    }

    async fn build_credential_provider(&self) -> ConnectorResult<SharedCredentialsProvider> {
        if self.access_key.is_some() && self.secret_key.is_some() {
            Ok(SharedCredentialsProvider::new(
                aws_credential_types::Credentials::from_keys(
                    self.access_key.as_ref().unwrap(),
                    self.secret_key.as_ref().unwrap(),
                    self.session_token.clone(),
                ),
            ))
        } else if !env_var_is_true(DISABLE_DEFAULT_CREDENTIAL) {
            Ok(SharedCredentialsProvider::new(
                aws_config::default_provider::credentials::default_provider().await,
            ))
        } else {
            bail!("Both \"access_key\" and \"secret_key\" are required.")
        }
    }

    async fn with_role_provider(
        &self,
        credential: SharedCredentialsProvider,
    ) -> ConnectorResult<SharedCredentialsProvider> {
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

    pub async fn build_config(&self) -> ConnectorResult<SdkConfig> {
        let region = self.build_region().await?;
        let credentials_provider = self
            .with_role_provider(self.build_credential_provider().await?)
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
#[derive(Debug, Clone, Deserialize, WithOptions, PartialEq, Hash, Eq)]
pub struct KafkaConnectionProps {
    #[serde(rename = "properties.bootstrap.server", alias = "kafka.brokers")]
    pub brokers: String,

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

    /// CA certificate string (PEM format) for verifying the broker's key.
    #[serde(rename = "properties.ssl.ca.pem")]
    ssl_ca_pem: Option<String>,

    /// Path to client's certificate file (PEM).
    #[serde(rename = "properties.ssl.certificate.location")]
    ssl_certificate_location: Option<String>,

    /// Client's public key string (PEM format) used for authentication.
    #[serde(rename = "properties.ssl.certificate.pem")]
    ssl_certificate_pem: Option<String>,

    /// Path to client's private key file (PEM).
    #[serde(rename = "properties.ssl.key.location")]
    ssl_key_location: Option<String>,

    /// Client's private key string (PEM format) used for authentication.
    #[serde(rename = "properties.ssl.key.pem")]
    ssl_key_pem: Option<String>,

    /// Passphrase of client's private key.
    #[serde(rename = "properties.ssl.key.password")]
    ssl_key_password: Option<String>,

    /// SASL mechanism if SASL is enabled. Currently support PLAIN, SCRAM, GSSAPI, and AWS_MSK_IAM.
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
}

#[serde_as]
#[derive(Debug, Clone, Deserialize, WithOptions)]
pub struct KafkaCommon {
    // connection related props are moved to `KafkaConnection`
    #[serde(rename = "topic", alias = "kafka.topic")]
    pub topic: String,

    #[serde(
        rename = "properties.sync.call.timeout",
        deserialize_with = "deserialize_duration_from_string",
        default = "default_kafka_sync_call_timeout"
    )]
    pub sync_call_timeout: Duration,
}

#[serde_as]
#[derive(Debug, Clone, Deserialize, WithOptions, PartialEq, Hash, Eq)]
pub struct KafkaPrivateLinkCommon {
    /// This is generated from `private_link_targets` and `private_link_endpoint` in frontend, instead of given by users.
    #[serde(rename = "broker.rewrite.endpoints")]
    #[serde_as(as = "Option<JsonString>")]
    pub broker_rewrite_map: Option<BTreeMap<String, String>>,
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
    /// the broker will enforce the topic's max.message.bytes limit
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

    #[serde(rename = "properties.enable.ssl.certificate.verification")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub enable_ssl_certificate_verification: Option<bool>,
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
        if let Some(v) = self.enable_ssl_certificate_verification {
            c.set("enable.ssl.certificate.verification", v.to_string());
        }
    }
}

impl KafkaConnectionProps {
    #[cfg(test)]
    pub fn test_default() -> Self {
        Self {
            brokers: "localhost:9092".to_owned(),
            security_protocol: None,
            ssl_ca_location: None,
            ssl_certificate_location: None,
            ssl_key_location: None,
            ssl_ca_pem: None,
            ssl_certificate_pem: None,
            ssl_key_pem: None,
            ssl_key_password: None,
            ssl_endpoint_identification_algorithm: None,
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
            sasl_kerberos_service_name: None,
            sasl_kerberos_keytab: None,
            sasl_kerberos_principal: None,
            sasl_kerberos_kinit_cmd: None,
            sasl_kerberos_min_time_before_relogin: None,
            sasl_oathbearer_config: None,
        }
    }

    pub(crate) fn set_security_properties(&self, config: &mut ClientConfig) {
        // AWS_MSK_IAM
        if self.is_aws_msk_iam() {
            config.set("security.protocol", "SASL_SSL");
            config.set("sasl.mechanism", "OAUTHBEARER");
            return;
        }

        // Security protocol
        if let Some(security_protocol) = self.security_protocol.as_ref() {
            config.set("security.protocol", security_protocol);
        }

        // SSL
        if let Some(ssl_ca_location) = self.ssl_ca_location.as_ref() {
            config.set("ssl.ca.location", ssl_ca_location);
        }
        if let Some(ssl_ca_pem) = self.ssl_ca_pem.as_ref() {
            config.set("ssl.ca.pem", ssl_ca_pem);
        }
        if let Some(ssl_certificate_location) = self.ssl_certificate_location.as_ref() {
            config.set("ssl.certificate.location", ssl_certificate_location);
        }
        if let Some(ssl_certificate_pem) = self.ssl_certificate_pem.as_ref() {
            config.set("ssl.certificate.pem", ssl_certificate_pem);
        }
        if let Some(ssl_key_location) = self.ssl_key_location.as_ref() {
            config.set("ssl.key.location", ssl_key_location);
        }
        if let Some(ssl_key_pem) = self.ssl_key_pem.as_ref() {
            config.set("ssl.key.pem", ssl_key_pem);
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

    pub(crate) fn is_aws_msk_iam(&self) -> bool {
        if let Some(sasl_mechanism) = self.sasl_mechanism.as_ref()
            && sasl_mechanism == AWS_MSK_IAM_AUTH
        {
            true
        } else {
            false
        }
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
}

fn create_credential_temp_file(credentials: &[u8]) -> std::io::Result<NamedTempFile> {
    let mut f = NamedTempFile::new()?;
    f.write_all(credentials)?;
    f.as_file().sync_all()?;
    Ok(f)
}

impl PulsarCommon {
    pub(crate) async fn build_client(
        &self,
        oauth: &Option<PulsarOauthCommon>,
        aws_auth_props: &AwsAuthProps,
    ) -> ConnectorResult<Pulsar<TokioExecutor>> {
        let mut pulsar_builder = Pulsar::builder(&self.service_url, TokioExecutor);
        let mut temp_file = None;
        if let Some(oauth) = oauth.as_ref() {
            let url = Url::parse(&oauth.credentials_url)?;
            match url.scheme() {
                "s3" => {
                    let credentials = load_file_descriptor_from_s3(&url, aws_auth_props).await?;
                    temp_file = Some(
                        create_credential_temp_file(&credentials)
                            .context("failed to create temp file for pulsar credentials")?,
                    );
                }
                "file" => {}
                _ => {
                    bail!("invalid credentials_url, only file url and s3 url are supported",);
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
                        .to_owned();
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
                name: "token".to_owned(),
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
    pub(crate) async fn build_client(&self) -> ConnectorResult<KinesisClient> {
        let config = AwsAuthProps {
            region: Some(self.stream_region.clone()),
            endpoint: self.endpoint.clone(),
            access_key: self.credentials_access_key.clone(),
            secret_key: self.credentials_secret_access_key.clone(),
            session_token: self.session_token.clone(),
            arn: self.assume_role_arn.clone(),
            external_id: self.assume_role_external_id.clone(),
            profile: Default::default(),
            msk_signer_timeout_sec: Default::default(),
        };
        let aws_config = config.build_config().await?;
        let mut builder = aws_sdk_kinesis::config::Builder::from(&aws_config);
        if let Some(endpoint) = &config.endpoint {
            builder = builder.endpoint_url(endpoint);
        }
        Ok(KinesisClient::from_conf(builder.build()))
    }
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
    pub(crate) async fn build_client(&self) -> ConnectorResult<async_nats::Client> {
        let mut connect_options = async_nats::ConnectOptions::new();
        match self.connect_mode.as_str() {
            "user_and_password" => {
                if let (Some(v_user), Some(v_password)) =
                    (self.user.as_ref(), self.password.as_ref())
                {
                    connect_options =
                        connect_options.user_and_password(v_user.into(), v_password.into())
                } else {
                    bail!("nats connect mode is user_and_password, but user or password is empty");
                }
            }

            "credential" => {
                if let (Some(v_nkey), Some(v_jwt)) = (self.nkey.as_ref(), self.jwt.as_ref()) {
                    connect_options = connect_options
                        .credentials(&self.create_credential(v_nkey, v_jwt)?)
                        .expect("failed to parse static creds")
                } else {
                    bail!("nats connect mode is credential, but nkey or jwt is empty");
                }
            }
            "plain" => {}
            _ => {
                bail!("nats connect mode only accepts user_and_password/credential/plain");
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
            .context("build nats client error")
            .map_err(SinkError::Nats)?;
        Ok(client)
    }

    pub(crate) async fn build_context(&self) -> ConnectorResult<jetstream::Context> {
        let client = self.build_client().await?;
        let jetstream = async_nats::jetstream::new(client);
        Ok(jetstream)
    }

    pub(crate) async fn build_consumer(
        &self,
        stream: String,
        durable_consumer_name: String,
        split_id: String,
        start_sequence: NatsOffset,
        mut config: jetstream::consumer::pull::Config,
    ) -> ConnectorResult<
        async_nats::jetstream::consumer::Consumer<async_nats::jetstream::consumer::pull::Config>,
    > {
        let context = self.build_context().await?;
        let stream = self.build_or_get_stream(context.clone(), stream).await?;
        let subject_name = self
            .subject
            .replace(',', "-")
            .replace(['.', '>', '*', ' ', '\t'], "_");
        let name = format!("risingwave-consumer-{}-{}", subject_name, split_id);

        let deliver_policy = match start_sequence {
            NatsOffset::Earliest => DeliverPolicy::All,
            NatsOffset::Latest => DeliverPolicy::New,
            NatsOffset::SequenceNumber(v) => {
                // for compatibility, we do not write to any state table now
                let parsed = v
                    .parse::<u64>()
                    .context("failed to parse nats offset as sequence number")?;
                DeliverPolicy::ByStartSequence {
                    start_sequence: 1 + parsed,
                }
            }
            NatsOffset::Timestamp(v) => DeliverPolicy::ByStartTime {
                start_time: OffsetDateTime::from_unix_timestamp_nanos(v as i128 * 1_000_000)
                    .context("invalid timestamp for nats offset")?,
            },
            NatsOffset::None => DeliverPolicy::All,
        };

        let consumer = if let Ok(consumer) = stream.get_consumer(&name).await {
            consumer
        } else {
            stream
                .get_or_create_consumer(&name, {
                    config.deliver_policy = deliver_policy;
                    config.durable_name = Some(durable_consumer_name);
                    config.filter_subjects =
                        self.subject.split(',').map(|s| s.to_owned()).collect();
                    config
                })
                .await?
        };
        Ok(consumer)
    }

    pub(crate) async fn build_or_get_stream(
        &self,
        jetstream: jetstream::Context,
        stream: String,
    ) -> ConnectorResult<jetstream::stream::Stream> {
        let subjects: Vec<String> = self.subject.split(',').map(|s| s.to_owned()).collect();
        if let Ok(mut stream_instance) = jetstream.get_stream(&stream).await {
            tracing::info!(
                "load existing nats stream ({:?}) with config {:?}",
                stream,
                stream_instance.info().await?
            );
            return Ok(stream_instance);
        }

        let mut config = jetstream::stream::Config {
            name: stream.clone(),
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
        tracing::info!(
            "create nats stream ({:?}) with config {:?}",
            &stream,
            config
        );
        let stream = jetstream.get_or_create_stream(config).await?;
        Ok(stream)
    }

    pub(crate) fn create_credential(&self, seed: &str, jwt: &str) -> ConnectorResult<String> {
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

pub(crate) fn load_certs(
    certificates: &str,
) -> ConnectorResult<Vec<rustls_pki_types::CertificateDer<'static>>> {
    let cert_bytes = if let Some(path) = certificates.strip_prefix("fs://") {
        std::fs::read_to_string(path).map(|cert| cert.as_bytes().to_owned())?
    } else {
        certificates.as_bytes().to_owned()
    };

    rustls_pemfile::certs(&mut cert_bytes.as_slice())
        .map(|cert| Ok(cert?))
        .collect()
}

pub(crate) fn load_private_key(
    certificate: &str,
) -> ConnectorResult<rustls_pki_types::PrivateKeyDer<'static>> {
    let cert_bytes = if let Some(path) = certificate.strip_prefix("fs://") {
        std::fs::read_to_string(path).map(|cert| cert.as_bytes().to_owned())?
    } else {
        certificate.as_bytes().to_owned()
    };

    let cert = rustls_pemfile::pkcs8_private_keys(&mut cert_bytes.as_slice())
        .next()
        .ok_or_else(|| anyhow!("No private key found"))?;
    Ok(cert?.into())
}

#[serde_as]
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct MongodbCommon {
    /// The URL of MongoDB
    #[serde(rename = "mongodb.url")]
    pub connect_uri: String,
    /// The collection name where data should be written to or read from. For sinks, the format is
    /// `db_name.collection_name`. Data can also be written to dynamic collections, see `collection.name.field`
    /// for more information.
    #[serde(rename = "collection.name")]
    pub collection_name: String,
}

impl MongodbCommon {
    pub(crate) async fn build_client(&self) -> ConnectorResult<mongodb::Client> {
        let client = mongodb::Client::with_uri_str(&self.connect_uri).await?;

        Ok(client)
    }
}

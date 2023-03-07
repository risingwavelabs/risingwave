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

use aws_sdk_kinesis::Client as KinesisClient;
use http::Uri;
use rdkafka::ClientConfig;
use serde_derive::{Deserialize, Serialize};

use crate::source::kinesis::config::AwsConfigInfo;

// The file describes the common abstractions for each connector and can be used in both source and
// sink.

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AwsPrivateLinkItem {
    pub service_name: String,
    pub availability_zones: Vec<String>,
    pub port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AwsPrivateLinks {
    pub provider: String,
    pub infos: Vec<AwsPrivateLinkItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaCommon {
    #[serde(rename = "properties.bootstrap.server", alias = "kafka.brokers")]
    pub brokers: String,

    #[serde(rename = "private.links.endpoints")]
    pub private_link_dns_names: Option<String>,

    #[serde(rename = "topic", alias = "kafka.topic")]
    pub topic: String,

    /// Security protocol used for RisingWave to communicate with Kafka brokers. Could be
    /// PLAINTEXT, SSL, SASL_PLAINTEXT or SASL_SSL.
    #[serde(rename = "properties.security.protocol")]
    security_protocol: Option<String>,

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
}

#[derive(Deserialize, Serialize, Debug, Clone)]
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
        let config = AwsConfigInfo::build(self.clone())?;
        let aws_config = config.load().await?;
        let mut builder = aws_sdk_kinesis::config::Builder::from(&aws_config);
        if let Some(endpoint) = &config.endpoint {
            let uri = endpoint.clone().parse::<Uri>().unwrap();
            builder =
                builder.endpoint_resolver(aws_smithy_http::endpoint::Endpoint::immutable(uri));
        }
        Ok(KinesisClient::from_conf(builder.build()))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpsertMessage<'a> {
    #[serde(borrow)]
    pub primary_key: Cow<'a, [u8]>,
    #[serde(borrow)]
    pub record: Cow<'a, [u8]>,
}

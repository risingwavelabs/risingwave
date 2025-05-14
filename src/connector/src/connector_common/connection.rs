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

use std::time::Duration;

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::ClientConfig;
use risingwave_common::secret::LocalSecretManager;
use risingwave_pb::catalog::PbConnection;
use serde_derive::Deserialize;
use serde_with::serde_as;
use tonic::async_trait;
use with_options::WithOptions;

use crate::connector_common::{AwsAuthProps, KafkaConnectionProps, KafkaPrivateLinkCommon};
use crate::error::ConnectorResult;
use crate::schema::schema_registry::Client as ConfluentSchemaRegistryClient;
use crate::source::kafka::{KafkaContextCommon, RwConsumerContext};
use crate::{dispatch_connection_impl, ConnectionImpl};

pub const SCHEMA_REGISTRY_CONNECTION_TYPE: &str = "schema_registry";

#[async_trait]
pub trait Connection {
    async fn test_connection(&self) -> ConnectorResult<()>;
}

#[serde_as]
#[derive(Debug, Clone, Deserialize, WithOptions, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct KafkaConnection {
    #[serde(flatten)]
    pub inner: KafkaConnectionProps,
    #[serde(flatten)]
    pub kafka_private_link_common: KafkaPrivateLinkCommon,
    #[serde(flatten)]
    pub aws_auth_props: AwsAuthProps,
}

pub async fn validate_connection(connection: &PbConnection) -> ConnectorResult<()> {
    if let Some(ref info) = connection.info {
        match info {
            risingwave_pb::catalog::connection::Info::ConnectionParams(cp) => {
                let options = cp.properties.clone().into_iter().collect();
                let secret_refs = cp.secret_refs.clone().into_iter().collect();
                let props_secret_resolved =
                    LocalSecretManager::global().fill_secrets(options, secret_refs)?;
                let connection_impl =
                    ConnectionImpl::from_proto(cp.connection_type(), props_secret_resolved)?;
                dispatch_connection_impl!(connection_impl, inner, inner.test_connection().await?)
            }
            risingwave_pb::catalog::connection::Info::PrivateLinkService(_) => unreachable!(),
        }
    }
    Ok(())
}

#[async_trait]
impl Connection for KafkaConnection {
    async fn test_connection(&self) -> ConnectorResult<()> {
        let client = self.build_client().await?;
        // describe cluster here
        client.fetch_metadata(None, Duration::from_secs(10)).await?;
        Ok(())
    }
}

use rdkafka::config::RDKafkaLogLevel;

pub fn read_kafka_log_level() -> RDKafkaLogLevel {
    let log_level =
        std::env::var("RISINGWAVE_KAFKA_LOG_LEVEL").unwrap_or_else(|_| "INFO".to_owned());
    match log_level.to_uppercase().as_str() {
        "DEBUG" => RDKafkaLogLevel::Debug,
        "INFO" => RDKafkaLogLevel::Info,
        "WARN" => RDKafkaLogLevel::Warning,
        "ERROR" => RDKafkaLogLevel::Error,
        "CRITICAL" => RDKafkaLogLevel::Critical,
        "EMERG" => RDKafkaLogLevel::Emerg,
        "ALERT" => RDKafkaLogLevel::Alert,
        "NOTICE" => RDKafkaLogLevel::Notice,
        _ => {
            tracing::info!(
                "Invalid RISINGWAVE_KAFKA_LOG_LEVEL: {}, using INFO instead",
                log_level
            );
            RDKafkaLogLevel::Info
        }
    }
}

impl KafkaConnection {
    async fn build_client(&self) -> ConnectorResult<BaseConsumer<RwConsumerContext>> {
        let mut config = ClientConfig::new();
        let bootstrap_servers = &self.inner.brokers;
        let broker_rewrite_map = self.kafka_private_link_common.broker_rewrite_map.clone();
        config.set("bootstrap.servers", bootstrap_servers);
        self.inner.set_security_properties(&mut config);

        // dup with Kafka Enumerator
        let ctx_common = KafkaContextCommon::new(
            broker_rewrite_map,
            None,
            None,
            self.aws_auth_props.clone(),
            self.inner.is_aws_msk_iam(),
        )
        .await?;
        let client_ctx = RwConsumerContext::new(ctx_common);
        let client: BaseConsumer<RwConsumerContext> = config
            .set_log_level(read_kafka_log_level())
            .create_with_context(client_ctx)
            .await?;
        if self.inner.is_aws_msk_iam() {
            #[cfg(not(madsim))]
            client.poll(Duration::from_secs(10)); // note: this is a blocking call
            #[cfg(madsim)]
            client.poll(Duration::from_secs(10)).await;
        }
        Ok(client)
    }
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, WithOptions)]
#[serde(deny_unknown_fields)]
pub struct IcebergConnection {}

#[async_trait]
impl Connection for IcebergConnection {
    async fn test_connection(&self) -> ConnectorResult<()> {
        todo!()
    }
}

#[serde_as]
#[derive(Debug, Clone, Deserialize, WithOptions, PartialEq, Hash, Eq)]
#[serde(deny_unknown_fields)]
pub struct ConfluentSchemaRegistryConnection {
    #[serde(rename = "schema.registry")]
    pub url: String,
    // ref `SchemaRegistryAuth`
    #[serde(rename = "schema.registry.username")]
    pub username: Option<String>,
    #[serde(rename = "schema.registry.password")]
    pub password: Option<String>,
}

#[async_trait]
impl Connection for ConfluentSchemaRegistryConnection {
    async fn test_connection(&self) -> ConnectorResult<()> {
        // GET /config to validate the connection
        let client = ConfluentSchemaRegistryClient::try_from(self)?;
        client.validate_connection().await?;
        Ok(())
    }
}

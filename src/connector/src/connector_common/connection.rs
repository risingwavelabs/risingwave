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

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Context;
use opendal::Operator;
use opendal::services::{Gcs, S3};
use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use risingwave_common::bail;
use risingwave_common::secret::LocalSecretManager;
use risingwave_pb::catalog::PbConnection;
use serde_derive::Deserialize;
use serde_with::serde_as;
use tonic::async_trait;
use url::Url;
use with_options::WithOptions;

use crate::connector_common::{
    AwsAuthProps, IcebergCommon, KafkaConnectionProps, KafkaPrivateLinkCommon,
};
use crate::deserialize_optional_bool_from_string;
use crate::error::ConnectorResult;
use crate::schema::schema_registry::Client as ConfluentSchemaRegistryClient;
use crate::source::build_connection;
use crate::source::kafka::{KafkaContextCommon, RwConsumerContext};

pub const SCHEMA_REGISTRY_CONNECTION_TYPE: &str = "schema_registry";

#[async_trait]
pub trait Connection: Send {
    async fn validate_connection(&self) -> ConnectorResult<()>;
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
                let connection = build_connection(cp.connection_type(), props_secret_resolved)?;
                connection.validate_connection().await?
            }
            risingwave_pb::catalog::connection::Info::PrivateLinkService(_) => unreachable!(),
        }
    }
    Ok(())
}

#[async_trait]
impl Connection for KafkaConnection {
    async fn validate_connection(&self) -> ConnectorResult<()> {
        let client = self.build_client().await?;
        // describe cluster here
        client.fetch_metadata(None, Duration::from_secs(10)).await?;
        Ok(())
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
        let client: BaseConsumer<RwConsumerContext> =
            config.create_with_context(client_ctx).await?;
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
pub struct IcebergConnection {
    #[serde(rename = "catalog.type")]
    pub catalog_type: Option<String>,
    #[serde(rename = "s3.region")]
    pub region: Option<String>,
    #[serde(rename = "s3.endpoint")]
    pub endpoint: Option<String>,
    #[serde(rename = "s3.access.key")]
    pub access_key: Option<String>,
    #[serde(rename = "s3.secret.key")]
    pub secret_key: Option<String>,

    #[serde(rename = "gcs.credential")]
    pub gcs_credential: Option<String>,

    /// Path of iceberg warehouse.
    #[serde(rename = "warehouse.path")]
    pub warehouse_path: Option<String>,
    /// Catalog id, can be omitted for storage catalog or when
    /// caller's AWS account ID matches glue id
    #[serde(rename = "glue.id")]
    pub glue_id: Option<String>,
    /// Catalog name, default value is risingwave.
    #[serde(rename = "catalog.name")]
    pub catalog_name: Option<String>,
    /// URI of iceberg catalog, only applicable in rest catalog.
    #[serde(rename = "catalog.uri")]
    pub catalog_uri: Option<String>,
    /// Credential for accessing iceberg catalog, only applicable in rest catalog.
    /// A credential to exchange for a token in the OAuth2 client credentials flow.
    #[serde(rename = "catalog.credential")]
    pub credential: Option<String>,
    /// token for accessing iceberg catalog, only applicable in rest catalog.
    /// A Bearer token which will be used for interaction with the server.
    #[serde(rename = "catalog.token")]
    pub token: Option<String>,
    /// `oauth2_server_uri` for accessing iceberg catalog, only applicable in rest catalog.
    /// Token endpoint URI to fetch token from if the Rest Catalog is not the authorization server.
    #[serde(rename = "catalog.oauth2_server_uri")]
    pub oauth2_server_uri: Option<String>,
    /// scope for accessing iceberg catalog, only applicable in rest catalog.
    /// Additional scope for OAuth2.
    #[serde(rename = "catalog.scope")]
    pub scope: Option<String>,

    /// The signing region to use when signing requests to the REST catalog.
    #[serde(rename = "catalog.rest.signing_region")]
    pub rest_signing_region: Option<String>,

    /// The signing name to use when signing requests to the REST catalog.
    #[serde(rename = "catalog.rest.signing_name")]
    pub rest_signing_name: Option<String>,

    /// Whether to use SigV4 for signing requests to the REST catalog.
    #[serde(
        rename = "catalog.rest.sigv4_enabled",
        default,
        deserialize_with = "deserialize_optional_bool_from_string"
    )]
    pub rest_sigv4_enabled: Option<bool>,

    #[serde(
        rename = "s3.path.style.access",
        default,
        deserialize_with = "deserialize_optional_bool_from_string"
    )]
    pub path_style_access: Option<bool>,

    #[serde(rename = "catalog.jdbc.user")]
    pub jdbc_user: Option<String>,

    #[serde(rename = "catalog.jdbc.password")]
    pub jdbc_password: Option<String>,
}

#[async_trait]
impl Connection for IcebergConnection {
    async fn validate_connection(&self) -> ConnectorResult<()> {
        let info = match &self.warehouse_path {
            Some(warehouse_path) => {
                let is_s3_tables = warehouse_path.starts_with("arn:aws:s3tables");
                let url = Url::parse(warehouse_path);
                if (url.is_err() || is_s3_tables)
                    && matches!(self.catalog_type.as_deref(), Some("rest" | "rest_rust"))
                {
                    // If the warehouse path is not a valid URL, it could be a warehouse name in rest catalog,
                    // Or it could be a s3tables path, which is not a valid URL but a valid warehouse path,
                    // so we allow it to pass here.
                    None
                } else {
                    let url =
                        url.with_context(|| format!("Invalid warehouse path: {}", warehouse_path))?;
                    let bucket = url
                        .host_str()
                        .with_context(|| {
                            format!("Invalid s3 path: {}, bucket is missing", warehouse_path)
                        })?
                        .to_owned();
                    let root = url.path().trim_start_matches('/').to_owned();
                    Some((url.scheme().to_owned(), bucket, root))
                }
            }
            None => {
                if matches!(self.catalog_type.as_deref(), Some("rest" | "rest_rust")) {
                    None
                } else {
                    bail!("`warehouse.path` must be set");
                }
            }
        };

        // Test warehouse
        if let Some((scheme, bucket, root)) = info {
            match scheme.as_str() {
                "s3" | "s3a" => {
                    let mut builder = S3::default();
                    if let Some(region) = &self.region {
                        builder = builder.region(region);
                    }
                    if let Some(endpoint) = &self.endpoint {
                        builder = builder.endpoint(endpoint);
                    }
                    if let Some(access_key) = &self.access_key {
                        builder = builder.access_key_id(access_key);
                    }
                    if let Some(secret_key) = &self.secret_key {
                        builder = builder.secret_access_key(secret_key);
                    }
                    builder = builder.root(root.as_str()).bucket(bucket.as_str());
                    let op = Operator::new(builder)?.finish();
                    op.check().await?;
                }
                "gs" | "gcs" => {
                    let mut builder = Gcs::default();
                    if let Some(credential) = &self.gcs_credential {
                        builder = builder.credential(credential);
                    }
                    builder = builder.root(root.as_str()).bucket(bucket.as_str());
                    let op = Operator::new(builder)?.finish();
                    op.check().await?;
                }
                _ => {
                    bail!("Unsupported scheme: {}", scheme);
                }
            }
        }

        if self.catalog_type.is_none() {
            bail!("`catalog.type` must be set");
        }

        // Test catalog
        let iceberg_common = IcebergCommon {
            catalog_type: self.catalog_type.clone(),
            region: self.region.clone(),
            endpoint: self.endpoint.clone(),
            access_key: self.access_key.clone(),
            secret_key: self.secret_key.clone(),
            gcs_credential: self.gcs_credential.clone(),
            warehouse_path: self.warehouse_path.clone(),
            glue_id: self.glue_id.clone(),
            catalog_name: self.catalog_name.clone(),
            catalog_uri: self.catalog_uri.clone(),
            credential: self.credential.clone(),
            token: self.token.clone(),
            oauth2_server_uri: self.oauth2_server_uri.clone(),
            scope: self.scope.clone(),
            rest_signing_region: self.rest_signing_region.clone(),
            rest_signing_name: self.rest_signing_name.clone(),
            rest_sigv4_enabled: self.rest_sigv4_enabled,
            path_style_access: self.path_style_access,
            database_name: Some("test_database".to_owned()),
            table_name: "test_table".to_owned(),
            enable_config_load: Some(false),
        };

        let mut java_map = HashMap::new();
        if let Some(jdbc_user) = &self.jdbc_user {
            java_map.insert("jdbc.user".to_owned(), jdbc_user.to_owned());
        }
        if let Some(jdbc_password) = &self.jdbc_password {
            java_map.insert("jdbc.password".to_owned(), jdbc_password.to_owned());
        }
        let catalog = iceberg_common.create_catalog(&java_map).await?;
        // test catalog by `table_exists` api
        catalog
            .table_exists(&iceberg_common.full_table_name()?)
            .await?;
        Ok(())
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
    async fn validate_connection(&self) -> ConnectorResult<()> {
        // GET /config to validate the connection
        let client = ConfluentSchemaRegistryClient::try_from(self)?;
        client.validate_connection().await?;
        Ok(())
    }
}

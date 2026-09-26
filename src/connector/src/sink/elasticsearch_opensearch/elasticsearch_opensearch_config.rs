// Copyright 2024 RisingWave Labs
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

use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::anyhow;
use maplit::hashset;
use risingwave_common::catalog::Schema;
use risingwave_common::types::DataType;
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use url::Url;
use with_options::WithOptions;

use super::super::SinkError;
use super::elasticsearch_opensearch_client::ElasticSearchOpenSearchClient;
use crate::connector_common::{AwsAuthProps, ElasticsearchConnection};
use crate::enforce_secret::EnforceSecret;
use crate::error::ConnectorError;
use crate::sink::Result;

pub const ES_OPTION_DELIMITER: &str = "delimiter";
pub const ES_OPTION_INDEX_COLUMN: &str = "index_column";
pub const ES_OPTION_INDEX: &str = "index";
pub const ES_OPTION_ROUTING_COLUMN: &str = "routing_column";

#[serde_as]
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct ElasticSearchConfig {
    #[serde(flatten)]
    pub inner: ElasticSearchOpenSearchConfig,

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

#[serde_as]
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct OpenSearchConfig {
    #[serde(flatten)]
    pub inner: ElasticSearchOpenSearchConfig,

    /// Authentication method for `OpenSearch`. Supported values: `basic`, `aws_sigv4`.
    #[serde(rename = "auth.method")]
    pub auth_method: Option<String>,

    /// AWS `SigV4` signing service name. Use `es` for Amazon `OpenSearch` Service and `aoss` for `OpenSearch` Serverless.
    #[serde(rename = "aws.sigv4.service_name")]
    pub aws_sigv4_service_name: Option<String>,

    #[serde(flatten)]
    pub aws_auth_props: AwsAuthProps,

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

#[serde_as]
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct ElasticSearchOpenSearchConfig {
    #[serde(rename = "url")]
    pub url: String,
    /// The index's name of elasticsearch or openserach
    #[serde(rename = "index")]
    pub index: Option<String>,
    /// If pk is set, then "pk1+delimiter+pk2+delimiter..." will be used as the key, if pk is not set, we will just use the first column as the key.
    #[serde(rename = "delimiter")]
    pub delimiter: Option<String>,
    /// The username of elasticsearch or openserach
    #[serde(rename = "username")]
    pub username: Option<String>,
    /// The username of elasticsearch or openserach
    #[serde(rename = "password")]
    pub password: Option<String>,
    /// It is used for dynamic index, if it is be set, the value of this column will be used as the index. It and `index` can only set one
    #[serde(rename = "index_column")]
    pub index_column: Option<String>,

    /// It is used for dynamic route, if it is be set, the value of this column will be used as the route
    #[serde(rename = "routing_column")]
    pub routing_column: Option<String>,

    #[serde(rename = "retry_on_conflict")]
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "default_retry_on_conflict")]
    pub retry_on_conflict: i32,

    #[serde(rename = "batch_num_messages")]
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "default_batch_num_messages")]
    #[with_option(allow_alter_on_fly)]
    pub batch_num_messages: usize,

    #[serde(rename = "batch_size_kb")]
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "default_batch_size_kb")]
    #[with_option(allow_alter_on_fly)]
    pub batch_size_kb: usize,

    #[serde(rename = "concurrent_requests")]
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "default_concurrent_requests")]
    #[with_option(allow_alter_on_fly)]
    pub concurrent_requests: usize,

    #[serde(default = "default_type")]
    pub r#type: String,
}

crate::impl_sink_unknown_fields!(ElasticSearchConfig);
crate::impl_sink_unknown_fields!(OpenSearchConfig);

impl EnforceSecret for ElasticSearchOpenSearchConfig {
    const ENFORCE_SECRET_PROPERTIES: phf::Set<&'static str> = phf::phf_set! {
        "username",
        "password",
    };
}

impl EnforceSecret for ElasticSearchConfig {
    fn enforce_one(prop: &str) -> crate::error::ConnectorResult<()> {
        ElasticSearchOpenSearchConfig::enforce_one(prop)
    }
}

impl EnforceSecret for OpenSearchConfig {
    fn enforce_one(prop: &str) -> crate::error::ConnectorResult<()> {
        ElasticSearchOpenSearchConfig::enforce_one(prop)?;
        AwsAuthProps::enforce_one(prop)?;
        Ok(())
    }
}

fn default_type() -> String {
    "upsert".to_owned()
}

fn default_retry_on_conflict() -> i32 {
    3
}

fn default_batch_num_messages() -> usize {
    512
}

fn default_batch_size_kb() -> usize {
    5 * 1024
}

fn default_concurrent_requests() -> usize {
    1024
}

impl TryFrom<&ElasticsearchConnection> for ElasticSearchOpenSearchConfig {
    type Error = ConnectorError;

    fn try_from(value: &ElasticsearchConnection) -> std::result::Result<Self, Self::Error> {
        let allowed_fields: HashSet<&str> = hashset!["url", "username", "password"]; // from ElasticsearchOpenSearchConfig

        for k in value.0.keys() {
            if !allowed_fields.contains(k.as_str()) {
                return Err(ConnectorError::from(anyhow!(
                    "Invalid field: {}, allowed fields: {:?}",
                    k,
                    allowed_fields
                )));
            }
        }

        let config = serde_json::from_value::<ElasticSearchOpenSearchConfig>(
            serde_json::to_value(value.0.clone()).unwrap(),
        )
        .map_err(|e| SinkError::Config(anyhow!(e)))?;
        Ok(config)
    }
}

impl ElasticSearchConfig {
    pub fn from_btreemap(properties: BTreeMap<String, String>) -> Result<Self> {
        let config = serde_json::from_value::<ElasticSearchConfig>(
            serde_json::to_value(properties).unwrap(),
        )
        .map_err(|e| SinkError::Config(anyhow!(e)))?;
        Ok(config)
    }

    pub fn build_client(&self) -> Result<ElasticSearchOpenSearchClient> {
        self.inner.build_elasticsearch_client()
    }
}

impl OpenSearchConfig {
    pub fn from_btreemap(properties: BTreeMap<String, String>) -> Result<Self> {
        let config =
            serde_json::from_value::<OpenSearchConfig>(serde_json::to_value(properties).unwrap())
                .map_err(|e| SinkError::Config(anyhow!(e)))?;
        Ok(config)
    }

    pub async fn build_client(&self) -> Result<ElasticSearchOpenSearchClient> {
        let mut transport_builder = opensearch::http::transport::TransportBuilder::new(
            opensearch::http::transport::SingleNodeConnectionPool::new(self.inner.url()?),
        );

        match self.validate_auth_config()? {
            OpenSearchAuthMethod::None => {
                if let Some(username) = &self.inner.username
                    && let Some(password) = &self.inner.password
                {
                    transport_builder = transport_builder.auth(
                        opensearch::auth::Credentials::Basic(username.clone(), password.clone()),
                    );
                }
            }
            OpenSearchAuthMethod::Basic => {
                let username = self.inner.username.as_ref().unwrap();
                let password = self.inner.password.as_ref().unwrap();
                transport_builder = transport_builder.auth(opensearch::auth::Credentials::Basic(
                    username.clone(),
                    password.clone(),
                ));
            }
            OpenSearchAuthMethod::AwsSigV4 => {
                let aws_config = self.aws_auth_props.build_config().await?;
                let credentials = opensearch::auth::Credentials::try_from(&aws_config)
                    .map_err(|e| SinkError::ElasticSearchOpenSearch(anyhow!(e)))?;
                transport_builder = transport_builder
                    .service_name(self.aws_sigv4_service_name.as_deref().unwrap_or("es"))
                    .auth(credentials);
            }
        }

        let transport = transport_builder
            .build()
            .map_err(|e| SinkError::ElasticSearchOpenSearch(anyhow!(e)))?;
        let client = opensearch::OpenSearch::new(transport);
        Ok(ElasticSearchOpenSearchClient::OpenSearch(client))
    }

    pub fn validate_auth_config(&self) -> Result<OpenSearchAuthMethod> {
        let auth_method = self.auth_method()?;
        match auth_method {
            OpenSearchAuthMethod::None => {
                if self.has_aws_auth_config() {
                    return Err(SinkError::Config(anyhow!(
                        "please set `auth.method` to `aws_sigv4` when AWS authentication properties are set."
                    )));
                }
                self.inner.check_username_password()?;
            }
            OpenSearchAuthMethod::Basic => {
                if self.has_aws_auth_config() {
                    return Err(SinkError::Config(anyhow!(
                        "AWS authentication properties cannot be used when `auth.method` is `basic`."
                    )));
                }
                self.inner.check_username_password()?;
                if self.inner.username.is_none() {
                    return Err(SinkError::Config(anyhow!(
                        "please set the username when `auth.method` is `basic`."
                    )));
                }
                if self.inner.password.is_none() {
                    return Err(SinkError::Config(anyhow!(
                        "please set the password when `auth.method` is `basic`."
                    )));
                }
            }
            OpenSearchAuthMethod::AwsSigV4 => {
                if self.inner.username.is_some() || self.inner.password.is_some() {
                    return Err(SinkError::Config(anyhow!(
                        "`username` and `password` cannot be used when `auth.method` is `aws_sigv4`."
                    )));
                }
            }
        }
        Ok(auth_method)
    }

    fn auth_method(&self) -> Result<OpenSearchAuthMethod> {
        match self.auth_method.as_deref() {
            Some("basic") => Ok(OpenSearchAuthMethod::Basic),
            Some("aws_sigv4") => Ok(OpenSearchAuthMethod::AwsSigV4),
            Some(auth_method) => Err(SinkError::Config(anyhow!(
                "unsupported `auth.method` for OpenSearch sink: {auth_method}. Supported values are `basic` and `aws_sigv4`."
            ))),
            None => Ok(OpenSearchAuthMethod::None),
        }
    }

    fn has_aws_auth_config(&self) -> bool {
        self.aws_auth_props.region.is_some()
            || self.aws_auth_props.endpoint.is_some()
            || self.aws_auth_props.access_key.is_some()
            || self.aws_auth_props.secret_key.is_some()
            || self.aws_auth_props.session_token.is_some()
            || self.aws_auth_props.arn.is_some()
            || self.aws_auth_props.external_id.is_some()
            || self.aws_auth_props.profile.is_some()
            || self.aws_auth_props.msk_signer_timeout_sec.is_some()
            || self.aws_sigv4_service_name.is_some()
    }
}

#[derive(Debug)]
pub enum OpenSearchAuthMethod {
    None,
    Basic,
    AwsSigV4,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn base_opensearch_props() -> BTreeMap<String, String> {
        BTreeMap::from([
            (
                "url".to_owned(),
                "https://example.us-east-1.es.amazonaws.com".to_owned(),
            ),
            ("index".to_owned(), "rw_test".to_owned()),
        ])
    }

    #[test]
    fn test_parse_opensearch_basic_auth_config() {
        let mut props = base_opensearch_props();
        props.insert("auth.method".to_owned(), "basic".to_owned());
        props.insert("username".to_owned(), "user".to_owned());
        props.insert("password".to_owned(), "pass".to_owned());

        let config = OpenSearchConfig::from_btreemap(props).unwrap();
        assert!(matches!(
            config.auth_method().unwrap(),
            OpenSearchAuthMethod::Basic
        ));
        assert_eq!(config.inner.username.as_deref(), Some("user"));
        assert_eq!(config.inner.password.as_deref(), Some("pass"));
        assert!(config.unknown_fields.is_empty());
    }

    #[test]
    fn test_parse_opensearch_sigv4_config() {
        let mut props = base_opensearch_props();
        props.insert("auth.method".to_owned(), "aws_sigv4".to_owned());
        props.insert("aws.region".to_owned(), "us-east-1".to_owned());
        props.insert(
            "aws.credentials.access_key_id".to_owned(),
            "test-access-key".to_owned(),
        );
        props.insert(
            "aws.credentials.secret_access_key".to_owned(),
            "test-secret-key".to_owned(),
        );
        props.insert("aws.sigv4.service_name".to_owned(), "aoss".to_owned());

        let config = OpenSearchConfig::from_btreemap(props).unwrap();
        assert!(matches!(
            config.auth_method().unwrap(),
            OpenSearchAuthMethod::AwsSigV4
        ));
        assert_eq!(config.aws_auth_props.region.as_deref(), Some("us-east-1"));
        assert_eq!(config.aws_sigv4_service_name.as_deref(), Some("aoss"));
        assert!(config.inner.username.is_none());
        assert!(config.inner.password.is_none());
        assert!(config.unknown_fields.is_empty());
    }

    #[test]
    fn test_reject_invalid_opensearch_auth_method() {
        let mut props = base_opensearch_props();
        props.insert("auth.method".to_owned(), "bearer".to_owned());

        let config = OpenSearchConfig::from_btreemap(props).unwrap();
        let err = config.auth_method().unwrap_err();
        assert!(err.to_string().contains("unsupported `auth.method`"));
    }
}

impl ElasticSearchOpenSearchConfig {
    pub fn from_btreemap(properties: BTreeMap<String, String>) -> Result<Self> {
        let config = serde_json::from_value::<ElasticSearchOpenSearchConfig>(
            serde_json::to_value(properties).unwrap(),
        )
        .map_err(|e| SinkError::Config(anyhow!(e)))?;
        Ok(config)
    }

    fn url(&self) -> Result<Url> {
        Url::parse(&self.url).map_err(|e| SinkError::ElasticSearchOpenSearch(anyhow!(e)))
    }

    fn check_username_password(&self) -> Result<()> {
        if self.username.is_some() && self.password.is_none() {
            return Err(SinkError::Config(anyhow!(
                "please set the password when the username is set."
            )));
        }
        if self.username.is_none() && self.password.is_some() {
            return Err(SinkError::Config(anyhow!(
                "please set the username when the password is set."
            )));
        }
        Ok(())
    }

    pub fn build_elasticsearch_client(&self) -> Result<ElasticSearchOpenSearchClient> {
        let mut transport_builder = elasticsearch::http::transport::TransportBuilder::new(
            elasticsearch::http::transport::SingleNodeConnectionPool::new(self.url()?),
        );
        if let Some(username) = &self.username
            && let Some(password) = &self.password
        {
            transport_builder = transport_builder.auth(elasticsearch::auth::Credentials::Basic(
                username.clone(),
                password.clone(),
            ));
        }
        self.check_username_password()?;
        let transport = transport_builder
            .build()
            .map_err(|e| SinkError::ElasticSearchOpenSearch(anyhow!(e)))?;
        let client = elasticsearch::Elasticsearch::new(transport);
        Ok(ElasticSearchOpenSearchClient::ElasticSearch(client))
    }

    pub fn validate_config(&self, schema: &Schema) -> Result<()> {
        if self.index_column.is_some() && self.index.is_some()
            || self.index_column.is_none() && self.index.is_none()
        {
            return Err(SinkError::Config(anyhow!(
                "please set only one of the 'index_column' or 'index' properties."
            )));
        }

        if let Some(index_column) = &self.index_column {
            let filed = schema
                .fields()
                .iter()
                .find(|f| &f.name == index_column)
                .unwrap();
            if filed.data_type() != DataType::Varchar {
                return Err(SinkError::Config(anyhow!(
                    "please ensure the data type of {} is varchar.",
                    index_column
                )));
            }
        }

        if let Some(routing_column) = &self.routing_column {
            let filed = schema
                .fields()
                .iter()
                .find(|f| &f.name == routing_column)
                .unwrap();
            if filed.data_type() != DataType::Varchar {
                return Err(SinkError::Config(anyhow!(
                    "please ensure the data type of {} is varchar.",
                    routing_column
                )));
            }
        }
        Ok(())
    }

    pub fn get_index_column_index(&self, schema: &Schema) -> Result<Option<usize>> {
        let index_column_idx = self
            .index_column
            .as_ref()
            .map(|n| {
                schema
                    .fields()
                    .iter()
                    .position(|s| &s.name == n)
                    .ok_or_else(|| anyhow!("Cannot find {}", ES_OPTION_INDEX_COLUMN))
            })
            .transpose()?;
        Ok(index_column_idx)
    }

    pub fn get_routing_column_index(&self, schema: &Schema) -> Result<Option<usize>> {
        let routing_column_idx = self
            .routing_column
            .as_ref()
            .map(|n| {
                schema
                    .fields()
                    .iter()
                    .position(|s| &s.name == n)
                    .ok_or_else(|| anyhow!("Cannot find {}", ES_OPTION_ROUTING_COLUMN))
            })
            .transpose()?;
        Ok(routing_column_idx)
    }
}

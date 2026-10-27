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

use std::collections::BTreeMap;

use anyhow::anyhow;
use risingwave_common::catalog::Schema;
use tonic::async_trait;
use url::Url;

use super::super::writer::{AsyncTruncateLogSinkerOf, AsyncTruncateSinkWriterExt};
use super::super::{Sink, SinkError, SinkParam, SinkWriterParam};
use super::backend::SearchBackend;
use super::client::{ElasticSearchOpenSearchClient, ElasticSearchOpenSearchSinkWriter};
use super::config::{ElasticSearchOpenSearchConfig, OpenSearchConfig};
use crate::enforce_secret::EnforceSecret;
use crate::sink::Result;

pub const OPENSEARCH_SINK: &str = "opensearch";

#[derive(Debug)]
pub struct OpenSearchBackend;

impl SearchBackend for OpenSearchBackend {
    type Config = OpenSearchConfig;

    const SINK_NAME: &'static str = OPENSEARCH_SINK;
    const SUPPORT_SCHEMA_CHANGE: bool = false;

    fn parse_config(properties: BTreeMap<String, String>) -> Result<Self::Config> {
        OpenSearchConfig::from_btreemap(properties)
    }

    fn common_config(config: &Self::Config) -> &ElasticSearchOpenSearchConfig {
        &config.inner
    }

    fn build_client(config: &Self::Config) -> Result<ElasticSearchOpenSearchClient> {
        let config = Self::common_config(config);
        let url =
            Url::parse(&config.url).map_err(|e| SinkError::ElasticSearchOpenSearch(anyhow!(e)))?;
        config.validate_credentials()?;
        let mut transport_builder = opensearch::http::transport::TransportBuilder::new(
            opensearch::http::transport::SingleNodeConnectionPool::new(url),
        );
        if let Some(username) = &config.username
            && let Some(password) = &config.password
        {
            transport_builder = transport_builder.auth(opensearch::auth::Credentials::Basic(
                username.clone(),
                password.clone(),
            ));
        }
        let transport = transport_builder
            .build()
            .map_err(|e| SinkError::ElasticSearchOpenSearch(anyhow!(e)))?;
        Ok(ElasticSearchOpenSearchClient::OpenSearch(
            opensearch::OpenSearch::new(transport),
        ))
    }

    fn check_available() -> Result<()> {
        risingwave_common::license::Feature::OpenSearchSink
            .check_available()
            .map_err(|e| anyhow!(e))?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct OpenSearchSink {
    config: OpenSearchConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
}

impl EnforceSecret for OpenSearchSink {
    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::error::ConnectorResult<()> {
        for prop in prop_iter {
            ElasticSearchOpenSearchConfig::enforce_one(prop)?;
        }
        Ok(())
    }
}
#[async_trait]
impl TryFrom<SinkParam> for OpenSearchSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let pk_indices = param.downstream_pk_or_empty();
        let config = OpenSearchBackend::parse_config(param.properties)?;
        Ok(Self {
            config,
            schema,
            pk_indices,
            is_append_only: param.sink_type.is_append_only(),
        })
    }
}

impl Sink for OpenSearchSink {
    type LogSinker = AsyncTruncateLogSinkerOf<ElasticSearchOpenSearchSinkWriter>;

    const SINK_NAME: &'static str = OpenSearchBackend::SINK_NAME;

    fn validate_unknown_fields(&self) -> Result<()> {
        crate::sink::validate_sink_unknown_fields(OpenSearchBackend::common_config(&self.config))
    }

    fn support_schema_change() -> bool {
        OpenSearchBackend::SUPPORT_SCHEMA_CHANGE
    }

    async fn validate(&self) -> Result<()> {
        OpenSearchBackend::check_available()?;
        OpenSearchBackend::common_config(&self.config).validate_config(&self.schema)?;
        let client = OpenSearchBackend::build_client(&self.config)?;
        client.ping().await?;
        Ok(())
    }

    fn validate_alter_config(config: &BTreeMap<String, String>) -> Result<()> {
        OpenSearchBackend::parse_config(config.clone())?;
        Ok(())
    }

    async fn new_log_sinker(&self, _writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        let client = OpenSearchBackend::build_client(&self.config)?;
        let config = OpenSearchBackend::common_config(&self.config);
        Ok(ElasticSearchOpenSearchSinkWriter::new(
            config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
            client,
            self.is_append_only,
        )?
        .into_log_sinker(config.concurrent_requests))
    }
}

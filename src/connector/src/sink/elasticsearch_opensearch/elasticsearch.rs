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
use super::config::{ElasticSearchConfig, ElasticSearchOpenSearchConfig};
use crate::enforce_secret::EnforceSecret;
use crate::sink::Result;

pub const ES_SINK: &str = "elasticsearch";

#[derive(Debug)]
pub struct ElasticSearchBackend;

impl SearchBackend for ElasticSearchBackend {
    type Config = ElasticSearchConfig;

    const SINK_NAME: &'static str = ES_SINK;
    const SUPPORT_SCHEMA_CHANGE: bool = true;

    fn parse_config(properties: BTreeMap<String, String>) -> Result<Self::Config> {
        ElasticSearchConfig::from_btreemap(properties)
    }

    fn common_config(config: &Self::Config) -> &ElasticSearchOpenSearchConfig {
        &config.inner
    }

    fn build_client(config: &Self::Config) -> Result<ElasticSearchOpenSearchClient> {
        let config = Self::common_config(config);
        let url =
            Url::parse(&config.url).map_err(|e| SinkError::ElasticSearchOpenSearch(anyhow!(e)))?;
        config.validate_credentials()?;
        let mut transport_builder = elasticsearch::http::transport::TransportBuilder::new(
            elasticsearch::http::transport::SingleNodeConnectionPool::new(url),
        );
        if let Some(username) = &config.username
            && let Some(password) = &config.password
        {
            transport_builder = transport_builder.auth(elasticsearch::auth::Credentials::Basic(
                username.clone(),
                password.clone(),
            ));
        }
        let transport = transport_builder
            .build()
            .map_err(|e| SinkError::ElasticSearchOpenSearch(anyhow!(e)))?;
        Ok(ElasticSearchOpenSearchClient::ElasticSearch(
            elasticsearch::Elasticsearch::new(transport),
        ))
    }
}

#[derive(Debug)]
pub struct ElasticSearchSink {
    config: ElasticSearchConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
}

impl EnforceSecret for ElasticSearchSink {
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
impl TryFrom<SinkParam> for ElasticSearchSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let pk_indices = param.downstream_pk_or_empty();
        let config = ElasticSearchBackend::parse_config(param.properties)?;
        Ok(Self {
            config,
            schema,
            pk_indices,
            is_append_only: param.sink_type.is_append_only(),
        })
    }
}

impl Sink for ElasticSearchSink {
    type LogSinker = AsyncTruncateLogSinkerOf<ElasticSearchOpenSearchSinkWriter>;

    const SINK_NAME: &'static str = ElasticSearchBackend::SINK_NAME;

    fn validate_unknown_fields(&self) -> Result<()> {
        crate::sink::validate_sink_unknown_fields(ElasticSearchBackend::common_config(&self.config))
    }

    fn support_schema_change() -> bool {
        ElasticSearchBackend::SUPPORT_SCHEMA_CHANGE
    }

    async fn validate(&self) -> Result<()> {
        ElasticSearchBackend::check_available()?;
        ElasticSearchBackend::common_config(&self.config).validate_config(&self.schema)?;
        let client = ElasticSearchBackend::build_client(&self.config)?;
        client.ping().await?;
        Ok(())
    }

    fn validate_alter_config(config: &BTreeMap<String, String>) -> Result<()> {
        ElasticSearchBackend::parse_config(config.clone())?;
        Ok(())
    }

    async fn new_log_sinker(&self, _writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        let client = ElasticSearchBackend::build_client(&self.config)?;
        let config = ElasticSearchBackend::common_config(&self.config);
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

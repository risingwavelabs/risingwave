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
use url::Url;

use super::backend::SearchBackend;
use super::client::ElasticSearchOpenSearchClient;
use super::config::{ElasticSearchConfig, ElasticSearchOpenSearchConfig};
use super::sink::SearchSink;
use crate::sink::{Result, SinkError};

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

pub type ElasticSearchSink = SearchSink<ElasticSearchBackend>;

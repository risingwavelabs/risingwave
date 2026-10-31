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
use serde_json::Value;
use url::Url;

use super::backend::SearchBackend;
use super::client::SearchClient;
use super::config::{ElasticSearchOpenSearchConfig, OpenSearchConfig};
use super::sink::SearchSink;
use crate::sink::{Result, SinkError};

pub const OPENSEARCH_SINK: &str = "opensearch";

#[derive(Debug)]
pub struct OpenSearchBackend;

impl SearchBackend for OpenSearchBackend {
    type Client = opensearch::OpenSearch;
    type Config = OpenSearchConfig;

    const SINK_NAME: &'static str = OPENSEARCH_SINK;
    const SUPPORT_SCHEMA_CHANGE: bool = false;

    fn parse_config(properties: BTreeMap<String, String>) -> Result<Self::Config> {
        OpenSearchConfig::from_btreemap(properties)
    }

    fn common_config(config: &Self::Config) -> &ElasticSearchOpenSearchConfig {
        &config.inner
    }

    fn build_client(config: &Self::Config) -> Result<Self::Client> {
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
        Ok(opensearch::OpenSearch::new(transport))
    }

    fn check_available() -> Result<()> {
        risingwave_common::license::Feature::OpenSearchSink
            .check_available()
            .map_err(|e| anyhow!(e))?;
        Ok(())
    }
}

pub type OpenSearchSink = SearchSink<OpenSearchBackend>;

impl SearchClient for opensearch::OpenSearch {
    type BulkOperation = opensearch::BulkOperation<Value>;

    async fn send(&self, bulks: Vec<Self::BulkOperation>) -> Result<Value> {
        let result = self
            .bulk(opensearch::BulkParts::None)
            .body(bulks)
            .send()
            .await?;
        Ok(result.json::<Value>().await?)
    }

    async fn ping(&self) -> Result<()> {
        self.ping().send().await?;
        Ok(())
    }

    fn new_update(
        &self,
        key: String,
        index: String,
        retry_on_conflict: i32,
        routing_column: Option<String>,
        value: Value,
    ) -> Self::BulkOperation {
        let bulk = opensearch::BulkOperation::update(key, value)
            .index(index)
            .retry_on_conflict(retry_on_conflict);
        if let Some(routing_column) = routing_column {
            bulk.routing(routing_column).into()
        } else {
            bulk.into()
        }
    }

    fn new_delete(
        &self,
        key: String,
        index: String,
        routing_column: Option<String>,
    ) -> Self::BulkOperation {
        let bulk = opensearch::BulkOperation::delete(key).index(index);
        if let Some(routing_column) = routing_column {
            bulk.routing(routing_column).into()
        } else {
            bulk.into()
        }
    }
}

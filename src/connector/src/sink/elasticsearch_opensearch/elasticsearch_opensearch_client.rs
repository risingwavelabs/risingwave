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

use std::sync::Arc;

use anyhow::anyhow;
use futures::{FutureExt, TryFuture};
use itertools::Itertools;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use serde_json::{Value, json};

use super::super::SinkError;
use super::elasticsearch_opensearch_config::ElasticSearchOpenSearchConfig;
use super::elasticsearch_opensearch_formatter::{BuildBulkPara, ElasticSearchOpenSearchFormatter};
use crate::sink::Result;
use crate::sink::log_store::DeliveryFutureManagerAddFuture;
use crate::sink::writer::AsyncTruncateSinkWriter;

pub enum ElasticSearchOpenSearchClient {
    ElasticSearch(elasticsearch::Elasticsearch),
    OpenSearch(opensearch::OpenSearch),
}
enum ElasticSearchOpenSearchBulk {
    ElasticSearch(elasticsearch::BulkOperation<serde_json::Value>),
    OpenSearch(opensearch::BulkOperation<serde_json::Value>),
}

impl ElasticSearchOpenSearchBulk {
    pub fn into_elasticsearch_bulk(self) -> elasticsearch::BulkOperation<serde_json::Value> {
        if let ElasticSearchOpenSearchBulk::ElasticSearch(bulk) = self {
            bulk
        } else {
            panic!("not a elasticsearch bulk")
        }
    }

    pub fn into_opensearch_bulk(self) -> opensearch::BulkOperation<serde_json::Value> {
        if let ElasticSearchOpenSearchBulk::OpenSearch(bulk) = self {
            bulk
        } else {
            panic!("not a opensearch bulk")
        }
    }
}

impl ElasticSearchOpenSearchClient {
    async fn send(&self, bulks: Vec<ElasticSearchOpenSearchBulk>) -> Result<Value> {
        match self {
            ElasticSearchOpenSearchClient::ElasticSearch(client) => {
                let bulks = bulks
                    .into_iter()
                    .map(ElasticSearchOpenSearchBulk::into_elasticsearch_bulk)
                    .collect_vec();
                let result = client
                    .bulk(elasticsearch::BulkParts::None)
                    .body(bulks)
                    .send()
                    .await?;
                Ok(result.json::<Value>().await?)
            }
            ElasticSearchOpenSearchClient::OpenSearch(client) => {
                let bulks = bulks
                    .into_iter()
                    .map(ElasticSearchOpenSearchBulk::into_opensearch_bulk)
                    .collect_vec();
                let result = client
                    .bulk(opensearch::BulkParts::None)
                    .body(bulks)
                    .send()
                    .await?;
                Ok(result.json::<Value>().await?)
            }
        }
    }

    pub async fn ping(&self) -> Result<()> {
        match self {
            ElasticSearchOpenSearchClient::ElasticSearch(client) => {
                client.ping().send().await?;
            }
            ElasticSearchOpenSearchClient::OpenSearch(client) => {
                client.ping().send().await?;
            }
        }
        Ok(())
    }

    fn new_update(
        &self,
        key: String,
        index: String,
        retry_on_conflict: i32,
        routing_column: Option<String>,
        value: serde_json::Value,
    ) -> ElasticSearchOpenSearchBulk {
        match self {
            ElasticSearchOpenSearchClient::ElasticSearch(_) => {
                let bulk = elasticsearch::BulkOperation::update(key, value)
                    .index(index)
                    .retry_on_conflict(retry_on_conflict);
                if let Some(routing_column) = routing_column {
                    ElasticSearchOpenSearchBulk::ElasticSearch(bulk.routing(routing_column).into())
                } else {
                    ElasticSearchOpenSearchBulk::ElasticSearch(bulk.into())
                }
            }
            ElasticSearchOpenSearchClient::OpenSearch(_) => {
                let bulk = opensearch::BulkOperation::update(key, value)
                    .index(index)
                    .retry_on_conflict(retry_on_conflict);
                if let Some(routing_column) = routing_column {
                    ElasticSearchOpenSearchBulk::OpenSearch(bulk.routing(routing_column).into())
                } else {
                    ElasticSearchOpenSearchBulk::OpenSearch(bulk.into())
                }
            }
        }
    }

    fn new_delete(
        &self,
        key: String,
        index: String,
        routing_column: Option<String>,
    ) -> ElasticSearchOpenSearchBulk {
        match self {
            ElasticSearchOpenSearchClient::ElasticSearch(_) => {
                let bulk = elasticsearch::BulkOperation::delete(key).index(index);
                if let Some(routing_column) = routing_column {
                    ElasticSearchOpenSearchBulk::ElasticSearch(bulk.routing(routing_column).into())
                } else {
                    ElasticSearchOpenSearchBulk::ElasticSearch(bulk.into())
                }
            }
            ElasticSearchOpenSearchClient::OpenSearch(_) => {
                let bulk = opensearch::BulkOperation::delete(key).index(index);
                if let Some(routing_column) = routing_column {
                    ElasticSearchOpenSearchBulk::OpenSearch(bulk.routing(routing_column).into())
                } else {
                    ElasticSearchOpenSearchBulk::OpenSearch(bulk.into())
                }
            }
        }
    }
}

pub struct ElasticSearchOpenSearchSinkWriter {
    client: Arc<ElasticSearchOpenSearchClient>,
    formatter: ElasticSearchOpenSearchFormatter,
    config: ElasticSearchOpenSearchConfig,
    is_append_only: bool,
}

impl ElasticSearchOpenSearchSinkWriter {
    pub fn new(
        config: ElasticSearchOpenSearchConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        connector: &str,
        is_append_only: bool,
    ) -> Result<Self> {
        let client = Arc::new(config.build_client(connector)?);
        let formatter = ElasticSearchOpenSearchFormatter::new(
            pk_indices,
            &schema,
            config.delimiter.clone(),
            config.get_index_column_index(&schema)?,
            config.index.clone(),
            config.get_routing_column_index(&schema)?,
        )?;
        Ok(Self {
            client,
            formatter,
            config,
            is_append_only,
        })
    }
}

pub type ElasticSearchOpenSearchSinkDeliveryFuture =
    impl TryFuture<Ok = (), Error = SinkError> + Unpin + 'static;

impl AsyncTruncateSinkWriter for ElasticSearchOpenSearchSinkWriter {
    type DeliveryFuture = ElasticSearchOpenSearchSinkDeliveryFuture;

    #[define_opaque(ElasticSearchOpenSearchSinkDeliveryFuture)]
    async fn write_chunk<'a>(
        &'a mut self,
        chunk: StreamChunk,
        mut add_future: DeliveryFutureManagerAddFuture<'a, Self::DeliveryFuture>,
    ) -> Result<()> {
        let chunk_capacity = chunk.capacity();
        let mut all_bulks: Vec<Vec<ElasticSearchOpenSearchBulk>> = vec![];
        let mut bulks: Vec<ElasticSearchOpenSearchBulk> = Vec::with_capacity(chunk_capacity);

        let mut bulks_size = 0;
        for build_bulk_para in self.formatter.convert_chunk(chunk, self.is_append_only)? {
            let BuildBulkPara {
                key,
                value,
                index,
                mem_size_b,
                routing_column,
            } = build_bulk_para;

            bulks_size += mem_size_b;
            if let Some(value) = value {
                let value = json!({
                    "doc": value,
                    "doc_as_upsert": true
                });
                let bulk = self.client.new_update(
                    key,
                    index,
                    self.config.retry_on_conflict,
                    routing_column,
                    value,
                );
                bulks.push(bulk);
            } else {
                let bulk = self.client.new_delete(key, index, routing_column);
                bulks.push(bulk);
            };

            if bulks.len() >= self.config.batch_num_messages
                || bulks_size >= self.config.batch_size_kb * 1024
            {
                all_bulks.push(bulks);
                bulks = Vec::with_capacity(chunk_capacity);
                bulks_size = 0;
            }
        }
        if !bulks.is_empty() {
            all_bulks.push(bulks);
        }
        for bulks in all_bulks {
            let client_clone = self.client.clone();
            let future = async move {
                let result = client_clone.send(bulks).await?;
                if result["errors"].as_bool().is_none() || result["errors"].as_bool().unwrap() {
                    Err(SinkError::ElasticSearchOpenSearch(anyhow!(
                        "send bulk to elasticsearch failed: {:?}",
                        result
                    )))
                } else {
                    Ok(())
                }
            }
            .boxed();
            add_future.add_future_may_await(future).await?;
        }
        Ok(())
    }
}

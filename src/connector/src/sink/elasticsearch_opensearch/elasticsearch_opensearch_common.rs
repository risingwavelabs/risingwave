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
use std::sync::Arc;

use anyhow::anyhow;
use futures::{FutureExt, TryFuture};
use itertools::Itertools;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::row::Row;
use risingwave_common::types::DataType;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use serde_with::{serde_as, DisplayFromStr};
use url::Url;
use with_options::WithOptions;

use super::super::encoder::template::TemplateEncoder;
use super::super::encoder::{JsonEncoder, RowEncoder};
use super::super::SinkError;
use super::elasticsearch::ES_SINK;
use super::opensearch::OPENSEARCH_SINK;
use crate::sink::log_store::DeliveryFutureManagerAddFuture;
use crate::sink::writer::AsyncTruncateSinkWriter;
use crate::sink::Result;

pub const ES_OPTION_INDEX_COLUMN: &str = "index_column";

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
    pub username: String,
    /// The username of elasticsearch or openserach
    #[serde(rename = "password")]
    pub password: String,
    /// It is used for dynamic index, if it is be set, the value of this column will be used as the index. It and `index` can only set one
    #[serde(rename = "index_column")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub index_column: Option<usize>,

    /// It is used for dynamic route, if it is be set, the value of this column will be used as the route
    #[serde(rename = "routing_column")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub routing_column: Option<usize>,

    #[serde(rename = "retry_on_conflict")]
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "default_retry_on_conflict")]
    pub retry_on_conflict: i32,

    #[serde(rename = "batch_num_messages")]
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "default_batch_num_messages")]
    pub batch_num_messages: usize,

    #[serde(rename = "batch_size_kb")]
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "default_batch_size_kb")]
    pub batch_size_kb: usize,

    #[serde(rename = "concurrent_requests")]
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "default_concurrent_requests")]
    pub concurrent_requests: usize,
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

impl ElasticSearchOpenSearchConfig {
    pub fn from_btreemap(
        mut properties: BTreeMap<String, String>,
        schema: &Schema,
    ) -> Result<Self> {
        let index_column = properties
                .get(ES_OPTION_INDEX_COLUMN)
                .cloned()
                .map(|n| {
                    schema
                        .fields()
                        .iter()
                        .position(|s| s.name == n)
                        .ok_or_else(|| anyhow!("please ensure that '{}' is set to an existing column within the schema.", n))
                })
                .transpose()?;
        if let Some(index_column) = index_column {
            properties.insert(ES_OPTION_INDEX_COLUMN.to_string(), index_column.to_string());
        }
        let config = serde_json::from_value::<ElasticSearchOpenSearchConfig>(
            serde_json::to_value(properties).unwrap(),
        )
        .map_err(|e| SinkError::Config(anyhow!(e)))?;
        Ok(config)
    }

    pub fn build_client(&self, connector: &str) -> Result<ElasticSearchOpenSearchClient> {
        let url =
            Url::parse(&self.url).map_err(|e| SinkError::ElasticSearchOpenSearch(anyhow!(e)))?;
        if connector.eq(ES_SINK) {
            let transport = elasticsearch::http::transport::TransportBuilder::new(
                elasticsearch::http::transport::SingleNodeConnectionPool::new(url),
            )
            .auth(elasticsearch::auth::Credentials::Basic(
                self.username.clone(),
                self.password.clone(),
            ))
            .build()
            .map_err(|e| SinkError::ElasticSearchOpenSearch(anyhow!(e)))?;
            let client = elasticsearch::Elasticsearch::new(transport);
            Ok(ElasticSearchOpenSearchClient::ElasticSearch(client))
        } else if connector.eq(OPENSEARCH_SINK) {
            let transport = opensearch::http::transport::TransportBuilder::new(
                opensearch::http::transport::SingleNodeConnectionPool::new(url),
            )
            .auth(opensearch::auth::Credentials::Basic(
                self.username.clone(),
                self.password.clone(),
            ))
            .build()
            .map_err(|e| SinkError::ElasticSearchOpenSearch(anyhow!(e)))?;
            let client = opensearch::OpenSearch::new(transport);
            Ok(ElasticSearchOpenSearchClient::OpenSearch(client))
        } else {
            panic!(
                "connector type must be {} or {}, but get {}",
                ES_SINK, OPENSEARCH_SINK, connector
            );
        }
    }
}

pub struct ElasticSearchOpenSearchFormatter {
    key_encoder: TemplateEncoder,
    value_encoder: JsonEncoder,
    index_column: Option<usize>,
    index: Option<String>,
    routing_column: Option<usize>,
}

pub struct BuildBulkPara {
    pub index: String,
    pub key: String,
    pub value: Option<Map<String, Value>>,
    pub mem_size_b: usize,
    pub routing_column: Option<String>,
}

impl ElasticSearchOpenSearchFormatter {
    pub fn new(
        pk_indices: Vec<usize>,
        schema: &Schema,
        delimiter: Option<String>,
        index_column: Option<usize>,
        index: Option<String>,
        routing_column: Option<usize>,
    ) -> Result<Self> {
        let key_format = if pk_indices.is_empty() {
            let name = &schema
                .fields()
                .get(0)
                .ok_or_else(|| {
                    SinkError::ElasticSearchOpenSearch(anyhow!(
                        "no value find in sink schema, index is 0"
                    ))
                })?
                .name;
            format!("{{{}}}", name)
        } else if pk_indices.len() == 1 {
            let index = *pk_indices.get(0).unwrap();
            let name = &schema
                .fields()
                .get(index)
                .ok_or_else(|| {
                    SinkError::ElasticSearchOpenSearch(anyhow!(
                        "no value find in sink schema, index is {:?}",
                        index
                    ))
                })?
                .name;
            format!("{{{}}}", name)
        } else {
            let delimiter = delimiter
                .as_ref()
                .ok_or_else(|| anyhow!("please set the separator in the with option, when there are multiple primary key values"))?
                .clone();
            let mut names = Vec::with_capacity(pk_indices.len());
            for index in &pk_indices {
                names.push(format!(
                    "{{{}}}",
                    schema
                        .fields()
                        .get(*index)
                        .ok_or_else(|| {
                            SinkError::ElasticSearchOpenSearch(anyhow!(
                                "no value find in sink schema, index is {:?}",
                                index
                            ))
                        })?
                        .name
                ));
            }
            names.join(&delimiter)
        };
        let col_indices = if let Some(index) = index_column {
            let mut col_indices: Vec<usize> = (0..schema.len()).collect();
            col_indices.remove(index);
            Some(col_indices)
        } else {
            None
        };
        let key_encoder = TemplateEncoder::new(schema.clone(), col_indices.clone(), key_format);
        let value_encoder = JsonEncoder::new_with_es(schema.clone(), col_indices.clone());
        Ok(Self {
            key_encoder,
            value_encoder,
            index_column,
            index,
            routing_column,
        })
    }

    pub fn covert_chunk(&self, chunk: StreamChunk) -> Result<Vec<BuildBulkPara>> {
        let mut result_vec = Vec::with_capacity(chunk.capacity());
        for (op, rows) in chunk.rows() {
            let index = if let Some(index_column) = self.index_column {
                rows.datum_at(index_column)
                    .ok_or_else(|| {
                        SinkError::ElasticSearchOpenSearch(anyhow!(
                            "no value find in sink schema, index is {:?}",
                            index_column
                        ))
                    })?
                    .into_utf8()
            } else {
                self.index.as_ref().unwrap()
            };
            let routing_column = self
                .routing_column
                .map(|routing_column| {
                    Ok::<String, SinkError>(
                        rows.datum_at(routing_column)
                            .ok_or_else(|| {
                                SinkError::ElasticSearchOpenSearch(anyhow!(
                                    "no value find in sink schema, index is {:?}",
                                    routing_column
                                ))
                            })?
                            .into_utf8()
                            .to_string(),
                    )
                })
                .transpose()?;
            match op {
                Op::Insert | Op::UpdateInsert => {
                    let key = self.key_encoder.encode(rows)?;
                    let value = self.value_encoder.encode(rows)?;
                    result_vec.push(BuildBulkPara {
                        index: index.to_string(),
                        key,
                        value: Some(value),
                        mem_size_b: rows.value_estimate_size(),
                        routing_column,
                    });
                }
                Op::Delete => {
                    let key = self.key_encoder.encode(rows)?;
                    let mem_size_b = std::mem::size_of_val(&key);
                    result_vec.push(BuildBulkPara {
                        index: index.to_string(),
                        key,
                        value: None,
                        mem_size_b,
                        routing_column,
                    });
                }
                Op::UpdateDelete => continue,
            }
        }
        Ok(result_vec)
    }
}

pub fn validate_config(config: &ElasticSearchOpenSearchConfig, schema: &Schema) -> Result<()> {
    if config.index_column.is_some() && config.index.is_some()
        || config.index_column.is_none() && config.index.is_none()
    {
        return Err(SinkError::Config(anyhow!(
            "please set only one of the 'index_column' or 'index' properties."
        )));
    }

    if let Some(index_column) = &config.index_column {
        let filed = schema.fields().get(*index_column).unwrap();
        if filed.data_type() != DataType::Varchar {
            return Err(SinkError::Config(anyhow!(
                "please ensure the data type of {} is varchar.",
                index_column
            )));
        }
    }
    Ok(())
}

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
}

impl ElasticSearchOpenSearchSinkWriter {
    pub fn new(
        config: ElasticSearchOpenSearchConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        connector: &str,
    ) -> Result<Self> {
        let client = Arc::new(config.build_client(connector)?);
        let formatter = ElasticSearchOpenSearchFormatter::new(
            pk_indices,
            &schema,
            config.delimiter.clone(),
            config.index_column,
            config.index.clone(),
            config.routing_column,
        )?;
        Ok(Self {
            client,
            formatter,
            config,
        })
    }
}

pub type ElasticSearchOpenSearchSinkDeliveryFuture =
    impl TryFuture<Ok = (), Error = SinkError> + Unpin + 'static;

impl AsyncTruncateSinkWriter for ElasticSearchOpenSearchSinkWriter {
    type DeliveryFuture = ElasticSearchOpenSearchSinkDeliveryFuture;

    async fn write_chunk<'a>(
        &'a mut self,
        chunk: StreamChunk,
        mut add_future: DeliveryFutureManagerAddFuture<'a, Self::DeliveryFuture>,
    ) -> Result<()> {
        let chunk_capacity = chunk.capacity();
        let mut all_bulks: Vec<Vec<ElasticSearchOpenSearchBulk>> = vec![];
        let mut bulks: Vec<ElasticSearchOpenSearchBulk> = Vec::with_capacity(chunk_capacity);

        let mut bulks_size = 0;
        for build_bulk_para in self.formatter.covert_chunk(chunk)? {
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

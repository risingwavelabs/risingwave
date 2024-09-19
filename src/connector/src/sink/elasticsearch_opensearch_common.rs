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
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::row::Row;
use risingwave_common::types::DataType;
use serde::Deserialize;
use serde_json::{Map, Value};
use serde_with::{serde_as, DisplayFromStr};
use url::Url;
use with_options::WithOptions;

use super::encoder::template::TemplateEncoder;
use super::encoder::{JsonEncoder, RowEncoder};
use super::SinkError;
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

    // #[serde(rename = "max_task_num")]
    // #[serde_as(as = "Option<DisplayFromStr>")]
    // pub max_task_num: Option<usize>,
    #[serde(rename = "retry_on_conflict")]
    pub retry_on_conflict: Option<i32>,

    #[serde(rename = "batch_num_messages")]
    pub batch_num_messages: Option<usize>,

    #[serde(rename = "batch_size_kb")]
    pub batch_size_kb: Option<usize>,

    #[serde(rename = "concurrent_requests")]
    pub concurrent_requests: Option<usize>,
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

    pub fn build_elasticsearch_client(&self) -> Result<elasticsearch::Elasticsearch> {
        let url =
            Url::parse(&self.url).map_err(|e| SinkError::ElasticSearchOpenSearch(anyhow!(e)))?;
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
        Ok(client)
    }

    pub fn build_opensearch_client(&self) -> Result<opensearch::OpenSearch> {
        let url =
            Url::parse(&self.url).map_err(|e| SinkError::ElasticSearchOpenSearch(anyhow!(e)))?;
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
        Ok(client)
    }
}

pub struct ElasticSearchOpenSearchFormatter {
    key_encoder: TemplateEncoder,
    value_encoder: JsonEncoder,
    index_column: Option<usize>,
    index: Option<String>,
}

type BuildBulkPara = (String, String, Option<Map<String, Value>>);

impl ElasticSearchOpenSearchFormatter {
    pub fn new(
        pk_indices: Vec<usize>,
        schema: &Schema,
        delimiter: Option<String>,
        index_column: Option<usize>,
        index: Option<String>,
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
            match op {
                Op::Insert | Op::UpdateInsert => {
                    let key = self.key_encoder.encode(rows)?;
                    let value = self.value_encoder.encode(rows)?;
                    result_vec.push((index.to_string(), key, Some(value)));
                }
                Op::Delete => {
                    let key = self.key_encoder.encode(rows)?;
                    result_vec.push((index.to_string(), key, None));
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

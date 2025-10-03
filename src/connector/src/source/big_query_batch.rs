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

use std::collections::{BTreeMap, HashMap};

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use futures_async_stream::try_stream;
use gcp_bigquery_client::Client;
use gcp_bigquery_client::model::query_request::QueryRequest;
use phf::{Set, phf_set};
use risingwave_common::array::StreamChunk;
use risingwave_common::types::JsonbVal;
use serde::{Deserialize, Serialize};
use with_options::WithOptions;

use crate::connector_common::AwsAuthProps;
use crate::enforce_secret::EnforceSecret;
use crate::error::ConnectorResult;
use crate::parser::{ByteStreamSourceParserImpl, ParserConfig, SpecificParserConfig};
use crate::sink::big_query::BigQueryCommon;
use crate::source::batch::BatchSourceSplit;
use crate::source::{
    BoxSourceChunkStream, Column, SourceContextRef, SourceEnumeratorContextRef, SourceMessage,
    SourceMeta, SourceProperties, SplitEnumerator, SplitId, SplitMetaData, SplitReader,
    UnknownFields,
};

pub const BATCH_BIGQUERY_CONNECTOR: &str = "batch_bigquery";

#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct BatchBigQueryProperties {
    #[serde(flatten)]
    pub common: BigQueryCommon,
    #[serde(flatten)]
    pub aws_auth_props: AwsAuthProps,
    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

impl BatchBigQueryProperties {
    pub const SOURCE_NAME: &'static str = BATCH_BIGQUERY_CONNECTOR;

    pub fn from_btreemap(properties: BTreeMap<String, String>) -> ConnectorResult<Self> {
        let config = serde_json::from_value::<BatchBigQueryProperties>(
            serde_json::to_value(properties).unwrap(),
        )
        .map_err(|e| anyhow!(e))?;
        Ok(config)
    }

    /// Build SQL query from table information
    pub fn build_query(&self) -> String {
        format!(
            "SELECT * FROM `{}`.`{}`.`{}`",
            self.common.project, self.common.dataset, self.common.table
        )
    }
}

impl EnforceSecret for BatchBigQueryProperties {
    const ENFORCE_SECRET_PROPERTIES: Set<&'static str> = phf_set! {
        "bigquery.credentials",
    };
}

impl UnknownFields for BatchBigQueryProperties {
    fn unknown_fields(&self) -> HashMap<String, String> {
        self.unknown_fields.clone()
    }
}

impl SourceProperties for BatchBigQueryProperties {
    type Split = BatchBigQuerySplit;
    type SplitEnumerator = BatchBigQueryEnumerator;
    type SplitReader = BatchBigQueryReader;

    const SOURCE_NAME: &'static str = BATCH_BIGQUERY_CONNECTOR;
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct BatchBigQuerySplit {
    pub split_id: SplitId,
    pub query: String, // Generated query
    #[serde(skip)]
    pub finished: bool,
}

impl SplitMetaData for BatchBigQuerySplit {
    fn id(&self) -> SplitId {
        self.split_id.clone()
    }

    fn restore_from_json(value: JsonbVal) -> ConnectorResult<Self> {
        serde_json::from_value(value.take()).map_err(|e| anyhow!(e).into())
    }

    fn encode_to_json(&self) -> JsonbVal {
        serde_json::to_value(self.clone()).unwrap().into()
    }

    fn update_offset(&mut self, _last_seen_offset: String) -> ConnectorResult<()> {
        // Batch source doesn't use offsets - query is executed once completely
        Ok(())
    }
}

impl BatchSourceSplit for BatchBigQuerySplit {
    fn finished(&self) -> bool {
        self.finished
    }

    fn finish(&mut self) {
        self.finished = true;
    }

    fn refresh(&mut self) {
        self.finished = false;
    }
}

impl BatchBigQuerySplit {
    pub fn new(split_id: SplitId, query: String) -> Self {
        Self {
            split_id,
            query,
            finished: false,
        }
    }
}

#[derive(Debug)]
pub struct BatchBigQueryEnumerator {
    properties: BatchBigQueryProperties,
}

#[async_trait]
impl SplitEnumerator for BatchBigQueryEnumerator {
    type Properties = BatchBigQueryProperties;
    type Split = BatchBigQuerySplit;

    async fn new(
        properties: Self::Properties,
        _context: SourceEnumeratorContextRef,
    ) -> ConnectorResult<Self> {
        Ok(Self { properties })
    }

    async fn list_splits(&mut self) -> ConnectorResult<Vec<BatchBigQuerySplit>> {
        // For batch source, we return exactly one split with auto-generated "SELECT * FROM table" query
        let query = self.properties.build_query();
        Ok(vec![BatchBigQuerySplit::new(
            "bigquery_batch_split".into(),
            query,
        )])
    }
}

#[derive(Debug)]
pub struct BatchBigQueryReader {
    properties: BatchBigQueryProperties,
    splits: Vec<BatchBigQuerySplit>,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
}

#[async_trait]
impl SplitReader for BatchBigQueryReader {
    type Properties = BatchBigQueryProperties;
    type Split = BatchBigQuerySplit;

    async fn new(
        properties: Self::Properties,
        splits: Vec<Self::Split>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> ConnectorResult<Self> {
        Ok(Self {
            properties,
            splits,
            parser_config,
            source_ctx,
        })
    }

    fn into_stream(self) -> BoxSourceChunkStream {
        self.into_stream_inner()
    }
}

impl BatchBigQueryReader {
    #[try_stream(boxed, ok = StreamChunk, error = crate::error::ConnectorError)]
    async fn into_stream_inner(self) {
        for split in &self.splits {
            let client = self.build_client().await?;

            let query_request = QueryRequest::new(split.query.clone());
            let mut result_set = client
                .job()
                .query(&self.properties.common.project, query_request)
                .await
                .with_context(|| format!("Failed to execute BigQuery query: {}", split.query))?;
            let mut row_count = 0;
            while result_set.next_row() {
                // Convert BigQuery row to JSON
                let mut row_json = serde_json::Map::new();

                // Get all column names from the schema
                let query_response = result_set.query_response();
                if let Some(schema) = &query_response.schema
                    && let Some(fields) = &schema.fields
                {
                    for field in fields {
                        let value = match result_set.get_json_value_by_name(&field.name) {
                            Ok(Some(val)) => val,
                            Ok(None) => serde_json::Value::Null,
                            Err(e) => {
                                return Err(anyhow!(
                                    "Failed to get BigQuery column value for '{}': {}",
                                    field.name,
                                    e
                                )
                                .into());
                            }
                        };
                        row_json.insert(field.name.clone(), value);
                    }
                }

                let json_string = serde_json::to_string(&row_json)
                    .with_context(|| "Failed to serialize BigQuery row to JSON")?;

                // Create a message for each row
                let message = SourceMessage {
                    key: None,
                    payload: Some(json_string.into_bytes()),
                    offset: row_count.to_string(),
                    split_id: split.id(),
                    meta: SourceMeta::Empty,
                };

                // The bigquery source is FORMAT NONE ENCODE NONE.
                // Here we just create a json parser to convert json value to chunk.
                let mut parser_config = self.parser_config.clone();
                parser_config.specific = SpecificParserConfig::DEFAULT_PLAIN_JSON;
                // Parse the content
                let parser =
                    ByteStreamSourceParserImpl::create(parser_config, self.source_ctx.clone())
                        .await?;
                let chunk_stream = parser
                    .parse_stream(Box::pin(futures::stream::once(async { Ok(vec![message]) })));

                #[for_await]
                for chunk in chunk_stream {
                    yield chunk?;
                }

                row_count += 1;
            }

            tracing::info!(
                "BatchBigQuery finished reading {} rows from query: {}",
                row_count,
                split.query
            );
        }
    }

    async fn build_client(&self) -> ConnectorResult<Client> {
        let client = self
            .properties
            .common
            .build_client(&self.properties.aws_auth_props)
            .await
            .map_err(|e| match e {
                crate::sink::SinkError::BigQuery(err) => {
                    tracing::error!("Failed to build BigQuery client for batch source: {}", err);
                    anyhow!("Failed to build BigQuery client: {}. Please check your credentials (supports both JSON and base64-encoded JSON).", err)
                },
                _ => {
                    tracing::error!("Failed to build BigQuery client for batch source: {}", e);
                    anyhow!("Failed to build BigQuery client: {}", e)
                },
            })?;
        Ok(client)
    }
}

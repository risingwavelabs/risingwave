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
use std::sync::Arc;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use futures::StreamExt;
use futures_async_stream::try_stream;
use google_cloud_bigquery::client::Client as BigQueryClient;
use google_cloud_bigquery::client::google_cloud_auth::credentials::CredentialsFile;
use google_cloud_bigquery::http::job::query::QueryRequest;
use google_cloud_bigquery::query::row::Row;
use google_cloud_bigquery::storage::value::StructDecodable;
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
    BoxSourceChunkStream, BoxSourceMessageStream, Column, SourceContextRef,
    SourceEnumeratorContextRef, SourceMessage, SourceMeta, SourceProperties, SplitEnumerator,
    SplitId, SplitMetaData, SplitReader, UnknownFields,
};

pub const BATCH_BIGQUERY_CONNECTOR: &str = "batch_bigquery";
pub const BATCH_BIGQUERY_CONNECTOR_PAGE_SIZE: usize = 1000;

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

#[try_stream(boxed, ok = Vec<SourceMessage>, error = crate::error::ConnectorError)]
async fn build_bigquery_stream(
    client: BigQueryClient,
    project_id: String,
    split_id: Arc<str>,
    query_str: String,
) {
    tracing::info!("Starting BigQuery query: {}", query_str);

    let request = QueryRequest {
        query: query_str,
        ..Default::default()
    };

    let mut iter = client
        .query::<Row>(&project_id, request)
        .await
        .context("Failed to execute BigQuery query")?;

    let mut offset: u64 = 0;
    let mut batch = Vec::with_capacity(BATCH_BIGQUERY_CONNECTOR_PAGE_SIZE);

    while let Some(row_value) = iter.next().await.context("Failed to fetch BigQuery row")? {
        // row_value is already a serde_json::Value representing the row
        let x = row_value.column::<serde_json::Value>(0)?;
        let json_bytes = serde_json::to_vec(&row_value)
            .map_err(|e| anyhow!("Failed to serialize row to JSON: {}", e))?;

        batch.push(SourceMessage {
            key: None,
            payload: Some(json_bytes),
            offset: offset.to_string(),
            split_id: split_id.clone(),
            meta: SourceMeta::Empty,
        });
        offset += 1;

        // Yield in batches for better performance
        if batch.len() >= BATCH_BIGQUERY_CONNECTOR_PAGE_SIZE {
            yield std::mem::take(&mut batch);
        }
    }

    // Yield remaining rows
    if !batch.is_empty() {
        yield batch;
    }

    tracing::info!("Finished fetching all rows from BigQuery");
}

impl BatchBigQueryReader {
    #[try_stream(boxed, ok = StreamChunk, error = crate::error::ConnectorError)]
    async fn into_stream_inner(self) {
        for split in &self.splits {
            let client = self
                .properties
                .common
                .build_client(&self.properties.aws_auth_props)
                .await?;

            let source_message_stream: BoxSourceMessageStream = build_bigquery_stream(
                client,
                self.properties.common.project.clone(),
                split.id(),
                split.query.clone(),
            )
            .boxed();

            // The bigquery source is FORMAT NONE ENCODE NONE.
            // Here we just create a json parser to convert json value to chunk.
            let mut parser_config = self.parser_config.clone();
            parser_config.specific = SpecificParserConfig::DEFAULT_PLAIN_JSON;
            // Parse the content
            let parser =
                ByteStreamSourceParserImpl::create(parser_config, self.source_ctx.clone()).await?;
            let chunk_stream = parser.parse_stream(source_message_stream);

            #[for_await]
            for chunk in chunk_stream {
                yield chunk?;
            }
        }
    }
}

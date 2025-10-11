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

use anyhow::anyhow;
use async_trait::async_trait;
use futures::StreamExt;
use futures_async_stream::try_stream;
use gcp_bigquery_client::Client;
use gcp_bigquery_client::model::job_configuration_query::JobConfigurationQuery;
use gcp_bigquery_client::model::query_request::QueryRequest;
use phf::{Set, phf_set};
use risingwave_common::array::StreamChunk;
use risingwave_common::types::JsonbVal;
use risingwave_common::util::iter_util::ZipEqFast;
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

const BATCH_BIGQUERY_CONNECTOR_PAGE_SIZE: i32 = 1000;

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
    client: Client,
    project: String,
    split_id: Arc<str>,
    mut query: QueryRequest,
) {
    let query_str = query.query.clone();
    // get metadata only in the call
    query.max_results = Some(0);

    // get schema and create job
    let result_set = client.job().query(&project, query).await?;

    let Some(job_ref) = result_set.query_response().job_reference.clone() else {
        return Err(crate::error::ConnectorError::from(anyhow::anyhow!(
            "Failed to get job reference"
        )));
    };

    let fields = {
        if let Some(schema) = result_set.query_response().schema.clone()
            && let Some(fields) = schema.fields
        {
            fields
        } else {
            return Err(crate::error::ConnectorError::from(anyhow::anyhow!(
                "Failed to get result schema from BigQuery"
            )));
        }
    };

    drop(result_set);

    let mut offset: u64 = 0;

    let query_all_params = JobConfigurationQuery {
        query: query_str,
        ..Default::default()
    };
    let stream = client.job().query_all(
        &project,
        query_all_params,
        Some(BATCH_BIGQUERY_CONNECTOR_PAGE_SIZE),
    );

    #[for_await]
    for row_batch in stream {
        let row_batch = row_batch?;
        let mut source_message_batch = Vec::with_capacity(row_batch.len());
        for row in row_batch {
            let mut json_obj = serde_json::Map::new();
            if let Some(columns) = row.columns {
                assert_eq!(columns.len(), fields.len());
                columns
                    .into_iter()
                    .zip_eq_fast(fields.iter())
                    .for_each(|(column, field)| {
                        json_obj.insert(
                            field.name.clone(),
                            column.value.unwrap_or(serde_json::Value::Null),
                        );
                    })
            } else {
                unreachable!();
            }

            source_message_batch.push(SourceMessage {
                key: None,
                payload: Some(serde_json::to_vec(&json_obj).unwrap()),
                offset: offset.to_string(),
                split_id: split_id.clone(),
                meta: SourceMeta::Empty,
            });
            offset += 1;
        }

        yield source_message_batch;
    }

    tracing::info!("Finished fetching all rows from BigQuery");
}

impl BatchBigQueryReader {
    #[try_stream(boxed, ok = StreamChunk, error = crate::error::ConnectorError)]
    async fn into_stream_inner(self) {
        for split in &self.splits {
            let client = self.build_client().await?;

            let query_request = QueryRequest::new(split.query.clone());
            let source_message_stream: BoxSourceMessageStream = build_bigquery_stream(
                client,
                self.properties.common.project.clone(),
                split.id(),
                query_request,
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

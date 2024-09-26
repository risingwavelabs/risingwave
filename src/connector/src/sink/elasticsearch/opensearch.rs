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
use std::sync::Arc;

use anyhow::anyhow;
use futures::prelude::TryFuture;
use futures::FutureExt;
use opensearch::{BulkOperation, BulkParts, OpenSearch};
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use serde_json::{json, Value};
use tonic::async_trait;

use super::super::log_store::DeliveryFutureManagerAddFuture;
use super::super::writer::{
    AsyncTruncateLogSinkerOf, AsyncTruncateSinkWriter, AsyncTruncateSinkWriterExt,
};
use super::super::{DummySinkCommitCoordinator, Sink, SinkError, SinkParam, SinkWriterParam};
use super::elasticsearch_opensearch_common::{
    validate_config, BuildBulkPara, ElasticSearchOpenSearchConfig, ElasticSearchOpenSearchFormatter,
};
use crate::sink::Result;

pub const OPENSEARCH_SINK: &str = "opensearch";

pub type OpenSearchSinkDeliveryFuture =
    impl TryFuture<Ok = (), Error = SinkError> + Unpin + 'static;

#[derive(Debug)]
pub struct OpenSearchSink {
    config: ElasticSearchOpenSearchConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
}

#[async_trait]
impl TryFrom<SinkParam> for OpenSearchSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let config = ElasticSearchOpenSearchConfig::from_btreemap(param.properties, &schema)?;
        Ok(Self {
            config,
            schema,
            pk_indices: param.downstream_pk,
        })
    }
}

impl Sink for OpenSearchSink {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = AsyncTruncateLogSinkerOf<OpenSearchSinkWriter>;

    const SINK_NAME: &'static str = OPENSEARCH_SINK;

    async fn validate(&self) -> Result<()> {
        validate_config(&self.config, &self.schema)?;
        let client = self.config.build_opensearch_client()?;
        client.ping().send().await?;
        Ok(())
    }

    async fn new_log_sinker(&self, _writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(OpenSearchSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
        )?
        .into_log_sinker(self.config.concurrent_requests))
    }
}

pub struct OpenSearchSinkWriter {
    client: Arc<OpenSearch>,
    formatter: ElasticSearchOpenSearchFormatter,
    config: ElasticSearchOpenSearchConfig,
}

impl OpenSearchSinkWriter {
    pub fn new(
        config: ElasticSearchOpenSearchConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
    ) -> Result<Self> {
        let client = Arc::new(config.build_opensearch_client()?);
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

impl AsyncTruncateSinkWriter for OpenSearchSinkWriter {
    type DeliveryFuture = OpenSearchSinkDeliveryFuture;

    async fn write_chunk<'a>(
        &'a mut self,
        chunk: StreamChunk,
        mut add_future: DeliveryFutureManagerAddFuture<'a, Self::DeliveryFuture>,
    ) -> Result<()> {
        let chunk_capacity = chunk.capacity();
        let mut all_bulks: Vec<Vec<BulkOperation<_>>> = vec![];
        let mut bulks: Vec<BulkOperation<_>> = Vec::with_capacity(chunk_capacity);

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
                let bulk = BulkOperation::update(key, value)
                    .index(index)
                    .retry_on_conflict(self.config.retry_on_conflict);
                if let Some(routing_column) = routing_column {
                    bulks.push(bulk.routing(routing_column).into());
                } else {
                    bulks.push(bulk.into());
                }
            } else {
                let bulk = BulkOperation::delete(key).index(index);
                if let Some(routing_column) = routing_column {
                    bulks.push(bulk.routing(routing_column).into());
                } else {
                    bulks.push(bulk.into());
                }
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
                let result = client_clone
                    .bulk(BulkParts::None)
                    .body(bulks)
                    .send()
                    .await?;
                let json = result.json::<Value>().await?;
                if json["errors"].as_bool().is_none() || json["errors"].as_bool().unwrap() {
                    Err(SinkError::ElasticSearchOpenSearch(anyhow!(
                        "send bulk to elasticsearch failed: {:?}",
                        json
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

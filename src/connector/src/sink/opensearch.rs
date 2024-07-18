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
use risingwave_common::session_config::sink_decouple::SinkDecouple;
use serde_json::Value;
use tonic::async_trait;

use super::catalog::desc::SinkDesc;
use super::elasticsearch_opensearch_common::{
    validate_config, ElasticSearchOpenSearchConfig, ElasticSearchOpenSearchFormatter,
};
use super::log_store::DeliveryFutureManagerAddFuture;
use super::writer::{
    AsyncTruncateLogSinkerOf, AsyncTruncateSinkWriter, AsyncTruncateSinkWriterExt,
};
use super::{DummySinkCommitCoordinator, Sink, SinkError, SinkParam, SinkWriterParam};
use crate::sink::Result;

pub const OPENSEARCH_SINK: &str = "opensearch_rust";

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

    fn is_sink_decouple(_desc: &SinkDesc, user_specified: &SinkDecouple) -> Result<bool> {
        match user_specified {
            SinkDecouple::Default | SinkDecouple::Enable => Ok(true),
            SinkDecouple::Disable => Ok(false),
        }
    }

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
        .into_log_sinker(usize::MAX))
    }
}

pub struct OpenSearchSinkWriter {
    client: Arc<OpenSearch>,
    formatter: ElasticSearchOpenSearchFormatter,
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
            config.delimiter,
            config.index_column,
            config.index,
        )?;
        Ok(Self { client, formatter })
    }
}

impl AsyncTruncateSinkWriter for OpenSearchSinkWriter {
    type DeliveryFuture = OpenSearchSinkDeliveryFuture;

    async fn write_chunk<'a>(
        &'a mut self,
        chunk: StreamChunk,
        mut add_future: DeliveryFutureManagerAddFuture<'a, Self::DeliveryFuture>,
    ) -> Result<()> {
        let mut bulks: Vec<BulkOperation<_>> = Vec::with_capacity(chunk.capacity());
        for (index, key, value) in self.formatter.covert_chunk(chunk)? {
            if let Some(value) = value {
                bulks.push(BulkOperation::index(value).index(index).id(key).into());
            } else {
                bulks.push(BulkOperation::delete(key).index(index).into());
            }
        }
        let clent_clone = self.client.clone();
        let future = async move {
            let result = clent_clone.bulk(BulkParts::None).body(bulks).send().await?;
            let json = result.json::<Value>().await?;
            if json["errors"].as_bool().is_none() || json["errors"].as_bool().unwrap() {
                Err(SinkError::ElasticSearchOpenSearch(anyhow!(
                    "send bulk to opensearch failed: {:?}",
                    json
                )))
            } else {
                Ok(())
            }
        }
        .boxed();
        add_future.add_future_may_await(future).await?;
        Ok(())
    }
}

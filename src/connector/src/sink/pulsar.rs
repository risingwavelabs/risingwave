// Copyright 2023 RisingWave Labs
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

use std::collections::HashMap;
use std::fmt::Debug;

use anyhow::anyhow;
use futures::TryFutureExt;
use serde::Deserialize;
use async_trait::async_trait;
use futures_async_stream::for_await;
use pulsar::{Pulsar, TokioExecutor, Producer};
use risingwave_common::catalog::Schema;
use risingwave_common::array::StreamChunk;
use risingwave_rpc_client::ConnectorClient;

use super::{
    Sink, SinkError, SinkWriterParam, SinkWriter, SinkParam, 
    SINK_TYPE_OPTION, SINK_TYPE_APPEND_ONLY, SINK_TYPE_UPSERT,
};
use crate::sink::{Result, DummySinkCommitCoordinator};
use crate::sink::utils::{
    gen_append_only_message_stream,
    AppendOnlyAdapterOpts,
};
use crate::common::PulsarCommon;

pub const PULSAR_SINK: &str = "pulsar";

#[derive(Debug, Clone, Deserialize)]
pub struct PulsarConfig {
    #[serde(flatten)]
    pub common: PulsarCommon,
    pub r#type: String, // accept "append-only" or "upsert"
}

impl PulsarConfig {
    pub fn from_hashmap(values: HashMap<String, String>) -> Result<Self> {
        let config = serde_json::from_value::<PulsarConfig>(serde_json::to_value(values).unwrap())
            .map_err(|e| SinkError::Config(anyhow!(e)))?;
        
        if config.r#type != SINK_TYPE_APPEND_ONLY
            && config.r#type != SINK_TYPE_UPSERT {
            return Err(SinkError::Config(anyhow!(
                "`{}` must be {}, or {}",
                SINK_TYPE_OPTION,
                SINK_TYPE_APPEND_ONLY,
                SINK_TYPE_UPSERT
            )));
        }
        Ok(config)
    }
}

#[derive(Debug)]
pub struct PulsarSink {
    pub config: PulsarConfig,
    schema: Schema,
    downstream_pk: Vec<usize>,
    is_append_only: bool,
}

impl PulsarSink {
    pub fn new(config: PulsarConfig, param: SinkParam) -> Self {
        Self {
            config,
            schema: param.schema(),
            downstream_pk: param.downstream_pk,
            is_append_only: param.sink_type.is_append_only(),
        }
    }
}

#[async_trait]
impl Sink for PulsarSink {
    type Coordinator = DummySinkCommitCoordinator;
    type Writer = PulsarSinkWriter;
    async fn new_writer(&self, _writer_param: SinkWriterParam) -> Result<Self::Writer> {
        PulsarSinkWriter::new(
            self.config.clone(), 
            self.schema.clone(), 
            self.downstream_pk.clone(),
            self.is_append_only,
        ).await
    }

    async fn validate(&self, _client: Option<ConnectorClient>) -> Result<()> {
        // For upsert Pulsar sink, the primary key must be defined.
        if !self.is_append_only && self.downstream_pk.is_empty() {
            return Err(SinkError::Config(anyhow!(
                "primary key not defined for {} pulsar sink (please define in `primary_key` field)",
                self.config.r#type
            )));
        }

        // TODO: validate pulsar connection

        Ok(())
    }
}

pub struct PulsarSinkWriter {
    pulsar: Pulsar<TokioExecutor>,
    producer: Producer<TokioExecutor>,
    schema: Schema,
    downstream_pk: Vec<usize>,
    is_append_only: bool,
}

impl PulsarSinkWriter {
    pub async fn new(
        config: PulsarConfig, 
        schema: Schema, 
        downstream_pk: Vec<usize>, 
        is_append_only: bool
    ) -> Result<Self> {
        let pulsar = config.common.build_client().await?;
        let producer = pulsar
            .producer()
            .with_topic(&config.common.topic)
            .build()
            .map_err(|e| SinkError::Pulsar(anyhow!("Pulsar sink error: {}", e)))
            .await?;
        Ok(Self {
            pulsar,
            producer,
            schema,
            downstream_pk,
            is_append_only,
        })
    }

    async fn append_only(&mut self, chunk: StreamChunk) -> Result<()> {
        let append_only_stream = gen_append_only_message_stream(
            &self.schema,
            &self.downstream_pk,
            chunk,
            AppendOnlyAdapterOpts::default(),
        );

        #[for_await]
        for msg in append_only_stream {
            let (event_key_object, event_object) = msg?;
            tracing::warn!("pulsar sink: {:?} {:?} {:?}", self.downstream_pk,
                event_key_object.as_ref().unwrap().to_string(), event_object.as_ref().unwrap().to_string());
            self.producer.send(pulsar::producer::Message {
                payload: serde_json::to_vec(&event_object).
                    map_err(|e| SinkError::Pulsar(anyhow!("Pulsar sink error: {}", e)))?,
                ..Default::default()
            }).map_err(|e| SinkError::Pulsar(anyhow!("Pulsar sink error: {}", e))).await?;
        }
        Ok(())
    }

    async fn upsert(&self, _chunk: StreamChunk) -> Result<()> {
        // TODO
        Ok(())
    }
}

#[async_trait]
impl SinkWriter for PulsarSinkWriter {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        if self.is_append_only {
            self.append_only(chunk).await
        } else {
            self.upsert(chunk).await
        }
    }

    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        Ok(())
    }

    async fn barrier(&mut self, _is_checkpoint: bool) -> Result<Self::CommitMetadata> {
        Ok(())
    }
}
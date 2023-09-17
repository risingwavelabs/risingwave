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

use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use futures::future::try_join_all;
use futures::TryFutureExt;
use futures_async_stream::for_await;
use pulsar::producer::{Message, SendFuture};
use pulsar::{Producer, Pulsar, TokioExecutor};
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use risingwave_rpc_client::ConnectorClient;
use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};

use super::encoder::{JsonEncoder, TimestampHandlingMode};
use super::{
    Sink, SinkError, SinkParam, SinkWriter, SinkWriterParam, SINK_TYPE_APPEND_ONLY,
    SINK_TYPE_OPTION, SINK_TYPE_UPSERT,
};
use crate::common::PulsarCommon;
use crate::deserialize_duration_from_string;
use crate::sink::utils::{
    gen_append_only_message_stream, gen_upsert_message_stream, AppendOnlyAdapterOpts,
    UpsertAdapterOpts,
};
use crate::sink::{DummySinkCommitCoordinator, Result};

pub const PULSAR_SINK: &str = "pulsar";

/// The delivery buffer queue size
/// When the `SendFuture` the current `send_future_buffer`
/// is buffering is greater than this size, then enforcing commit once
const PULSAR_SEND_FUTURE_BUFFER_MAX_SIZE: usize = 65536;

const fn _default_max_retries() -> u32 {
    3
}

const fn _default_retry_backoff() -> Duration {
    Duration::from_millis(100)
}

fn pulsar_to_sink_err(e: pulsar::Error) -> SinkError {
    SinkError::Pulsar(anyhow!(e))
}

async fn build_pulsar_producer(
    pulsar: &Pulsar<TokioExecutor>,
    config: &PulsarConfig,
) -> Result<Producer<TokioExecutor>> {
    pulsar
        .producer()
        .with_topic(&config.common.topic)
        .build()
        .map_err(pulsar_to_sink_err)
        .await
}

#[serde_as]
#[derive(Debug, Clone, Deserialize)]
pub struct PulsarConfig {
    #[serde(rename = "properties.retry.max", default = "_default_max_retries")]
    #[serde_as(as = "DisplayFromStr")]
    pub max_retry_num: u32,

    #[serde(
        rename = "properties.retry.interval",
        default = "_default_retry_backoff",
        deserialize_with = "deserialize_duration_from_string"
    )]
    pub retry_interval: Duration,

    #[serde(flatten)]
    pub common: PulsarCommon,

    pub r#type: String, // accept "append-only" or "upsert"
}

impl PulsarConfig {
    pub fn from_hashmap(values: HashMap<String, String>) -> Result<Self> {
        let config = serde_json::from_value::<PulsarConfig>(serde_json::to_value(values).unwrap())
            .map_err(|e| SinkError::Config(anyhow!(e)))?;

        if config.r#type != SINK_TYPE_APPEND_ONLY && config.r#type != SINK_TYPE_UPSERT {
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
        )
        .await
    }

    async fn validate(&self, _client: Option<ConnectorClient>) -> Result<()> {
        // For upsert Pulsar sink, the primary key must be defined.
        if !self.is_append_only && self.downstream_pk.is_empty() {
            return Err(SinkError::Config(anyhow!(
                "primary key not defined for {} pulsar sink (please define in `primary_key` field)",
                self.config.r#type
            )));
        }

        // Validate pulsar connection.
        let pulsar = self.config.common.build_client().await?;
        build_pulsar_producer(&pulsar, &self.config).await?;

        Ok(())
    }
}

pub struct PulsarSinkWriter {
    pulsar: Pulsar<TokioExecutor>,
    producer: Producer<TokioExecutor>,
    config: PulsarConfig,
    schema: Schema,
    downstream_pk: Vec<usize>,
    is_append_only: bool,
    send_future_buffer: VecDeque<SendFuture>,
}

impl PulsarSinkWriter {
    pub async fn new(
        config: PulsarConfig,
        schema: Schema,
        downstream_pk: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        let pulsar = config.common.build_client().await?;
        let producer = build_pulsar_producer(&pulsar, &config).await?;
        Ok(Self {
            pulsar,
            producer,
            config,
            schema,
            downstream_pk,
            is_append_only,
            send_future_buffer: VecDeque::new(),
        })
    }

    async fn send_message(&mut self, message: Message) -> Result<()> {
        let mut success_flag = false;
        let mut connection_err = None;

        for _ in 0..self.config.max_retry_num {
            match self.producer.send(message.clone()).await {
                // If the message is sent successfully,
                // a SendFuture holding the message receipt
                // or error after sending is returned
                Ok(send_future) => {
                    // Check if send_future_buffer is greater than the preset limit
                    while self.send_future_buffer.len() >= PULSAR_SEND_FUTURE_BUFFER_MAX_SIZE {
                        self.send_future_buffer
                            .pop_front()
                            .expect("Expect the SendFuture not to be None")
                            .map_err(|e| SinkError::Pulsar(anyhow!(e)))
                            .await?;
                    }

                    success_flag = true;
                    self.send_future_buffer.push_back(send_future);
                    break;
                }
                // error upon sending
                Err(e) => match e {
                    pulsar::Error::Connection(e) => {
                        connection_err = Some(e);
                        tokio::time::sleep(self.config.retry_interval).await;
                        continue;
                    }
                    _ => return Err(SinkError::Pulsar(anyhow!(e))),
                },
            }
        }

        if !success_flag {
            Err(SinkError::Pulsar(anyhow!(connection_err.unwrap())))
        } else {
            Ok(())
        }
    }

    async fn write_json_objects(
        &mut self,
        event_key_object: Option<serde_json::Value>,
        event_object: Option<serde_json::Value>,
    ) -> Result<()> {
        let message = Message {
            partition_key: event_key_object.map(|key| key.to_string()),
            payload: serde_json::to_vec(&event_object)
                .map_err(|e| SinkError::Pulsar(anyhow!(e)))?,
            ..Default::default()
        };

        self.send_message(message).await?;
        Ok(())
    }

    async fn append_only(&mut self, chunk: StreamChunk) -> Result<()> {
        // TODO: Remove the clones here, only to satisfy borrow checker at present
        let schema = self.schema.clone();
        let downstream_pk = self.downstream_pk.clone();
        let key_encoder =
            JsonEncoder::new(&schema, Some(&downstream_pk), TimestampHandlingMode::Milli);
        let val_encoder = JsonEncoder::new(&schema, None, TimestampHandlingMode::Milli);

        let append_only_stream = gen_append_only_message_stream(
            chunk,
            AppendOnlyAdapterOpts::default(),
            key_encoder,
            val_encoder,
        );

        #[for_await]
        for msg in append_only_stream {
            let (event_key_object, event_object) = msg?;
            self.write_json_objects(event_key_object, event_object)
                .await?;
        }
        Ok(())
    }

    async fn upsert(&mut self, chunk: StreamChunk) -> Result<()> {
        // TODO: Remove the clones here, only to satisfy borrow checker at present
        let schema = self.schema.clone();
        let downstream_pk = self.downstream_pk.clone();
        let key_encoder =
            JsonEncoder::new(&schema, Some(&downstream_pk), TimestampHandlingMode::Milli);
        let val_encoder = JsonEncoder::new(&schema, None, TimestampHandlingMode::Milli);

        let upsert_stream = gen_upsert_message_stream(
            chunk,
            UpsertAdapterOpts::default(),
            key_encoder,
            val_encoder,
        );

        #[for_await]
        for msg in upsert_stream {
            let (event_key_object, event_object) = msg?;
            self.write_json_objects(event_key_object, event_object)
                .await?;
        }
        Ok(())
    }

    async fn commit_inner(&mut self) -> Result<()> {
        try_join_all(
            self.send_future_buffer
                .drain(..)
                .map(|send_future| send_future.map_err(|e| SinkError::Pulsar(anyhow!(e)))),
        )
        .await?;

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

    async fn barrier(&mut self, is_checkpoint: bool) -> Result<Self::CommitMetadata> {
        if is_checkpoint {
            self.commit_inner().await?;
        }

        Ok(())
    }
}

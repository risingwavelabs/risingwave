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
use pulsar::producer::{Message, SendFuture};
use pulsar::{Producer, ProducerOptions, Pulsar, TokioExecutor};
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};

use super::catalog::{SinkFormat, SinkFormatDesc};
use super::{Sink, SinkError, SinkParam, SinkWriter, SinkWriterParam};
use crate::common::PulsarCommon;
use crate::sink::formatter::SinkFormatterImpl;
use crate::sink::writer::{FormattedSink, LogSinkerOf, SinkWriterExt};
use crate::sink::{DummySinkCommitCoordinator, Result};
use crate::{deserialize_duration_from_string, dispatch_sink_formatter_impl};

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

const fn _default_batch_size() -> u32 {
    10000
}

const fn _default_batch_byte_size() -> usize {
    1 << 20
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
        .with_options(ProducerOptions {
            batch_size: Some(config.producer_properties.batch_size),
            batch_byte_size: Some(config.producer_properties.batch_byte_size),
            ..Default::default()
        })
        .with_topic(&config.common.topic)
        .build()
        .map_err(pulsar_to_sink_err)
        .await
}

#[serde_as]
#[derive(Debug, Clone, Deserialize)]
pub struct PulsarPropertiesProducer {
    #[serde(rename = "properties.batch.size", default = "_default_batch_size")]
    #[serde_as(as = "DisplayFromStr")]
    batch_size: u32,

    #[serde(
        rename = "properties.batch.byte.size",
        default = "_default_batch_byte_size"
    )]
    #[serde_as(as = "DisplayFromStr")]
    batch_byte_size: usize,
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

    #[serde(flatten)]
    pub producer_properties: PulsarPropertiesProducer,
}

impl PulsarConfig {
    pub fn from_hashmap(values: HashMap<String, String>) -> Result<Self> {
        let config = serde_json::from_value::<PulsarConfig>(serde_json::to_value(values).unwrap())
            .map_err(|e| SinkError::Config(anyhow!(e)))?;

        Ok(config)
    }
}

#[derive(Debug)]
pub struct PulsarSink {
    pub config: PulsarConfig,
    schema: Schema,
    downstream_pk: Vec<usize>,
    format_desc: SinkFormatDesc,
    db_name: String,
    sink_from_name: String,
}

impl TryFrom<SinkParam> for PulsarSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let config = PulsarConfig::from_hashmap(param.properties)?;
        Ok(Self {
            config,
            schema,
            downstream_pk: param.downstream_pk,
            format_desc: param
                .format_desc
                .ok_or_else(|| SinkError::Config(anyhow!("missing FORMAT ... ENCODE ...")))?,
            db_name: param.db_name,
            sink_from_name: param.sink_from_name,
        })
    }
}

impl Sink for PulsarSink {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = LogSinkerOf<PulsarSinkWriter>;

    const SINK_NAME: &'static str = PULSAR_SINK;

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(PulsarSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.downstream_pk.clone(),
            &self.format_desc,
            self.db_name.clone(),
            self.sink_from_name.clone(),
        )
        .await?
        .into_log_sinker(writer_param.sink_metrics))
    }

    async fn validate(&self) -> Result<()> {
        // For upsert Pulsar sink, the primary key must be defined.
        if self.format_desc.format != SinkFormat::AppendOnly && self.downstream_pk.is_empty() {
            return Err(SinkError::Config(anyhow!(
                "primary key not defined for {:?} pulsar sink (please define in `primary_key` field)",
                self.format_desc.format
            )));
        }

        // Validate pulsar connection.
        let pulsar = self.config.common.build_client().await?;
        build_pulsar_producer(&pulsar, &self.config).await?;

        Ok(())
    }
}

pub struct PulsarSinkWriter {
    payload_writer: PulsarPayloadWriter,
    formatter: SinkFormatterImpl,
}

struct PulsarPayloadWriter {
    pulsar: Pulsar<TokioExecutor>,
    producer: Producer<TokioExecutor>,
    config: PulsarConfig,
    send_future_buffer: VecDeque<SendFuture>,
}

impl PulsarSinkWriter {
    pub async fn new(
        config: PulsarConfig,
        schema: Schema,
        downstream_pk: Vec<usize>,
        format_desc: &SinkFormatDesc,
        db_name: String,
        sink_from_name: String,
    ) -> Result<Self> {
        let formatter =
            SinkFormatterImpl::new(format_desc, schema, downstream_pk, db_name, sink_from_name)?;
        let pulsar = config.common.build_client().await?;
        let producer = build_pulsar_producer(&pulsar, &config).await?;
        Ok(Self {
            formatter,
            payload_writer: PulsarPayloadWriter {
                pulsar,
                producer,
                config,
                send_future_buffer: VecDeque::new(),
            },
        })
    }
}

impl PulsarPayloadWriter {
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
                    pulsar::Error::Connection(_)
                    | pulsar::Error::Producer(_)
                    | pulsar::Error::Consumer(_) => {
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

    async fn write_inner(
        &mut self,
        event_key_object: Option<String>,
        event_object: Option<Vec<u8>>,
    ) -> Result<()> {
        let message = Message {
            partition_key: event_key_object,
            payload: event_object.unwrap_or_default(),
            ..Default::default()
        };

        self.send_message(message).await?;
        Ok(())
    }

    async fn commit_inner(&mut self) -> Result<()> {
        self.producer
            .send_batch()
            .map_err(pulsar_to_sink_err)
            .await?;
        try_join_all(
            self.send_future_buffer
                .drain(..)
                .map(|send_future| send_future.map_err(|e| SinkError::Pulsar(anyhow!(e)))),
        )
        .await?;

        Ok(())
    }
}

impl FormattedSink for PulsarPayloadWriter {
    type K = String;
    type V = Vec<u8>;

    async fn write_one(&mut self, k: Option<Self::K>, v: Option<Self::V>) -> Result<()> {
        self.write_inner(k, v).await
    }
}

#[async_trait]
impl SinkWriter for PulsarSinkWriter {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        dispatch_sink_formatter_impl!(&self.formatter, formatter, {
            self.payload_writer.write_chunk(chunk, formatter).await
        })
    }

    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        Ok(())
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> Result<Self::CommitMetadata> {
        if is_checkpoint {
            self.payload_writer.commit_inner().await?;
        }

        Ok(())
    }
}

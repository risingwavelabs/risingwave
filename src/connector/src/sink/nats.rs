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
use core::fmt::Debug;
use std::collections::HashMap;

use anyhow::anyhow;
use async_nats::jetstream::context::Context;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::error::anyhow_error;
use serde_derive::Deserialize;
use serde_with::serde_as;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tokio_retry::Retry;

use super::utils::chunk_to_json;
use super::{DummySinkCommitCoordinator, SinkWriter, SinkWriterParam};
use crate::common::NatsCommon;
use crate::sink::encoder::{JsonEncoder, TimestampHandlingMode};
use crate::sink::writer::{LogSinkerOf, SinkWriterExt};
use crate::sink::{Result, Sink, SinkError, SinkParam, SINK_TYPE_APPEND_ONLY};

pub const NATS_SINK: &str = "nats";

#[serde_as]
#[derive(Clone, Debug, Deserialize)]
pub struct NatsConfig {
    #[serde(flatten)]
    pub common: NatsCommon,
    // accept "append-only"
    pub r#type: String,
}

#[derive(Clone, Debug)]
pub struct NatsSink {
    pub config: NatsConfig,
    schema: Schema,
    is_append_only: bool,
}

// sink write
pub struct NatsSinkWriter {
    pub config: NatsConfig,
    context: Context,
    schema: Schema,
    json_encoder: JsonEncoder,
}

/// Basic data types for use with the nats interface
impl NatsConfig {
    pub fn from_hashmap(values: HashMap<String, String>) -> Result<Self> {
        let config = serde_json::from_value::<NatsConfig>(serde_json::to_value(values).unwrap())
            .map_err(|e| SinkError::Config(anyhow!(e)))?;
        if config.r#type != SINK_TYPE_APPEND_ONLY {
            Err(SinkError::Config(anyhow!(
                "Nats sink only support append-only mode"
            )))
        } else {
            Ok(config)
        }
    }
}

impl TryFrom<SinkParam> for NatsSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let config = NatsConfig::from_hashmap(param.properties)?;
        Ok(Self {
            config,
            schema,
            is_append_only: param.sink_type.is_append_only(),
        })
    }
}

impl Sink for NatsSink {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = LogSinkerOf<NatsSinkWriter>;

    const SINK_NAME: &'static str = NATS_SINK;

    async fn validate(&self) -> Result<()> {
        if !self.is_append_only {
            return Err(SinkError::Nats(anyhow!(
                "Nats sink only support append-only mode"
            )));
        }
        match self.config.common.build_client().await {
            Ok(_client) => {}
            Err(error) => {
                return Err(SinkError::Nats(anyhow_error!(
                    "validate nats sink error: {:?}",
                    error
                )));
            }
        }
        Ok(())
    }

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(
            NatsSinkWriter::new(self.config.clone(), self.schema.clone())
                .await?
                .into_log_sinker(writer_param.sink_metrics),
        )
    }
}

impl NatsSinkWriter {
    pub async fn new(config: NatsConfig, schema: Schema) -> Result<Self> {
        let context = config
            .common
            .build_context()
            .await
            .map_err(|e| SinkError::Nats(anyhow_error!("nats sink error: {:?}", e)))?;
        Ok::<_, SinkError>(Self {
            config: config.clone(),
            context,
            schema: schema.clone(),
            json_encoder: JsonEncoder::new(schema, None, TimestampHandlingMode::Milli),
        })
    }

    async fn append_only(&mut self, chunk: StreamChunk) -> Result<()> {
        Retry::spawn(
            ExponentialBackoff::from_millis(100).map(jitter).take(3),
            || async {
                let data = chunk_to_json(chunk.clone(), &self.json_encoder).unwrap();
                for item in data {
                    self.context
                        .publish(self.config.common.subject.clone(), item.into())
                        .await
                        .map_err(|e| SinkError::Nats(anyhow_error!("nats sink error: {:?}", e)))?;
                }
                Ok::<_, SinkError>(())
            },
        )
        .await
        .map_err(|e| SinkError::Nats(anyhow_error!("nats sink error: {:?}", e)))
    }
}

#[async_trait::async_trait]
impl SinkWriter for NatsSinkWriter {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        self.append_only(chunk).await
    }

    async fn begin_epoch(&mut self, _epoch_id: u64) -> Result<()> {
        Ok(())
    }

    async fn barrier(&mut self, _is_checkpoint: bool) -> Result<()> {
        Ok(())
    }
}

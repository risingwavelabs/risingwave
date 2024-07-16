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
use core::fmt::Debug;
use core::future::IntoFuture;
use std::collections::BTreeMap;

use anyhow::{anyhow, Context as _};
use async_nats::jetstream::context::Context;
use futures::prelude::TryFuture;
use futures::FutureExt;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::session_config::sink_decouple::SinkDecouple;
use serde_derive::Deserialize;
use serde_with::serde_as;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tokio_retry::Retry;
use with_options::WithOptions;

use super::encoder::{DateHandlingMode, TimeHandlingMode, TimestamptzHandlingMode};
use super::utils::chunk_to_json;
use super::{DummySinkCommitCoordinator, SinkWriterParam};
use crate::connector_common::NatsCommon;
use crate::sink::catalog::desc::SinkDesc;
use crate::sink::encoder::{JsonEncoder, TimestampHandlingMode};
use crate::sink::log_store::DeliveryFutureManagerAddFuture;
use crate::sink::writer::{
    AsyncTruncateLogSinkerOf, AsyncTruncateSinkWriter, AsyncTruncateSinkWriterExt,
};
use crate::sink::{Result, Sink, SinkError, SinkParam, SINK_TYPE_APPEND_ONLY};

pub const NATS_SINK: &str = "nats";
const NATS_SEND_FUTURE_BUFFER_MAX_SIZE: usize = 65536;

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
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
    #[expect(dead_code)]
    schema: Schema,
    json_encoder: JsonEncoder,
}

pub type NatsSinkDeliveryFuture = impl TryFuture<Ok = (), Error = SinkError> + Unpin + 'static;

/// Basic data types for use with the nats interface
impl NatsConfig {
    pub fn from_btreemap(values: BTreeMap<String, String>) -> Result<Self> {
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
        let config = NatsConfig::from_btreemap(param.properties)?;
        Ok(Self {
            config,
            schema,
            is_append_only: param.sink_type.is_append_only(),
        })
    }
}

impl Sink for NatsSink {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = AsyncTruncateLogSinkerOf<NatsSinkWriter>;

    const SINK_NAME: &'static str = NATS_SINK;

    fn is_sink_decouple(_desc: &SinkDesc, user_specified: &SinkDecouple) -> Result<bool> {
        match user_specified {
            SinkDecouple::Default | SinkDecouple::Enable => Ok(true),
            SinkDecouple::Disable => Ok(false),
        }
    }

    async fn validate(&self) -> Result<()> {
        if !self.is_append_only {
            return Err(SinkError::Nats(anyhow!(
                "Nats sink only support append-only mode"
            )));
        }
        let _client = (self.config.common.build_client().await)
            .context("validate nats sink error")
            .map_err(SinkError::Nats)?;
        Ok(())
    }

    async fn new_log_sinker(&self, _writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(
            NatsSinkWriter::new(self.config.clone(), self.schema.clone())
                .await?
                .into_log_sinker(NATS_SEND_FUTURE_BUFFER_MAX_SIZE),
        )
    }
}

impl NatsSinkWriter {
    pub async fn new(config: NatsConfig, schema: Schema) -> Result<Self> {
        let context = config
            .common
            .build_context()
            .await
            .map_err(|e| SinkError::Nats(anyhow!(e)))?;
        Ok::<_, SinkError>(Self {
            config: config.clone(),
            context,
            schema: schema.clone(),
            json_encoder: JsonEncoder::new(
                schema,
                None,
                DateHandlingMode::FromCe,
                TimestampHandlingMode::Milli,
                TimestamptzHandlingMode::UtcWithoutSuffix,
                TimeHandlingMode::Milli,
            ),
        })
    }
}

impl AsyncTruncateSinkWriter for NatsSinkWriter {
    type DeliveryFuture = NatsSinkDeliveryFuture;

    async fn write_chunk<'a>(
        &'a mut self,
        chunk: StreamChunk,
        mut add_future: DeliveryFutureManagerAddFuture<'a, Self::DeliveryFuture>,
    ) -> Result<()> {
        let mut data = chunk_to_json(chunk, &self.json_encoder).unwrap();
        for item in &mut data {
            let publish_ack_future = Retry::spawn(
                ExponentialBackoff::from_millis(100).map(jitter).take(3),
                || async {
                    self.context
                        .publish(self.config.common.subject.clone(), item.clone().into())
                        .await
                        .context("nats sink error")
                        .map_err(SinkError::Nats)
                },
            )
            .await
            .context("nats sink error")
            .map_err(SinkError::Nats)?;
            let future = publish_ack_future.into_future().map(|result| {
                result
                    .context("Nats sink error")
                    .map_err(SinkError::Nats)
                    .map(|_| ())
            });
            add_future.add_future_may_await(future).await?;
        }
        Ok(())
    }
}

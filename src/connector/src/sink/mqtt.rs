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
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use anyhow::{anyhow, Context as _};
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::ConnectionError;
use serde_derive::Deserialize;
use serde_with::serde_as;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tokio_retry::Retry;
use with_options::WithOptions;

use super::encoder::{DateHandlingMode, TimeHandlingMode, TimestamptzHandlingMode};
use super::utils::chunk_to_json;
use super::{DummySinkCommitCoordinator, SinkWriterParam};
use crate::common::MqttCommon;
use crate::sink::catalog::desc::SinkDesc;
use crate::sink::encoder::{JsonEncoder, TimestampHandlingMode};
use crate::sink::log_store::DeliveryFutureManagerAddFuture;
use crate::sink::writer::{
    AsyncTruncateLogSinkerOf, AsyncTruncateSinkWriter, AsyncTruncateSinkWriterExt,
};
use crate::sink::{Result, Sink, SinkError, SinkParam, SINK_TYPE_APPEND_ONLY};
use crate::{deserialize_bool_from_string, deserialize_u32_from_string};

pub const MQTT_SINK: &str = "mqtt";

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct MqttConfig {
    #[serde(flatten)]
    pub common: MqttCommon,

    // 0 - AtLeastOnce, 1 - AtMostOnce, 2 - ExactlyOnce
    #[serde(default, deserialize_with = "deserialize_u32_from_string")]
    pub qos: u32,

    #[serde(default, deserialize_with = "deserialize_bool_from_string")]
    pub retain: bool,

    // accept "append-only"
    pub r#type: String,
}

#[derive(Clone, Debug)]
pub struct MqttSink {
    pub config: MqttConfig,
    schema: Schema,
    is_append_only: bool,
}

// sink write
pub struct MqttSinkWriter {
    pub config: MqttConfig,
    client: rumqttc::v5::AsyncClient,
    qos: QoS,
    retain: bool,
    schema: Schema,
    json_encoder: JsonEncoder,
    stopped: Arc<AtomicBool>,
}

/// Basic data types for use with the mqtt interface
impl MqttConfig {
    pub fn from_hashmap(values: HashMap<String, String>) -> Result<Self> {
        let config = serde_json::from_value::<MqttConfig>(serde_json::to_value(values).unwrap())
            .map_err(|e| SinkError::Config(anyhow!(e)))?;
        if config.r#type != SINK_TYPE_APPEND_ONLY {
            Err(SinkError::Config(anyhow!(
                "Mqtt sink only support append-only mode"
            )))
        } else {
            Ok(config)
        }
    }
}

impl TryFrom<SinkParam> for MqttSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let config = MqttConfig::from_hashmap(param.properties)?;
        Ok(Self {
            config,
            schema,
            is_append_only: param.sink_type.is_append_only(),
        })
    }
}

impl Sink for MqttSink {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = AsyncTruncateLogSinkerOf<MqttSinkWriter>;

    const SINK_NAME: &'static str = MQTT_SINK;

    fn default_sink_decouple(desc: &SinkDesc) -> bool {
        desc.sink_type.is_append_only()
    }

    async fn validate(&self) -> Result<()> {
        if !self.is_append_only {
            return Err(SinkError::Mqtt(anyhow!(
                "Nats sink only support append-only mode"
            )));
        }
        let _client = (self.config.common.build_client(0))
            .context("validate mqtt sink error")
            .map_err(SinkError::Mqtt)?;
        Ok(())
    }

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(MqttSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            writer_param.executor_id,
        )?
        .into_log_sinker(usize::MAX))
    }
}

impl MqttSinkWriter {
    pub fn new(config: MqttConfig, schema: Schema, id: u64) -> Result<Self> {
        let qos = match config.qos {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            2 => QoS::ExactlyOnce,
            _ => {
                return Err(SinkError::Mqtt(anyhow!(
                    "Invalid QoS level: {}",
                    config.qos
                )))
            }
        };

        let (client, mut eventloop) = config
            .common
            .build_client(id as u32)
            .map_err(|e| SinkError::Mqtt(anyhow!(e)))?;

        let stopped = Arc::new(AtomicBool::new(false));
        let stopped_clone = stopped.clone();

        tokio::spawn(async move {
            while !stopped_clone.load(std::sync::atomic::Ordering::Relaxed) {
                match eventloop.poll().await {
                    Ok(_) => (),
                    Err(err) => {
                        if let ConnectionError::Timeout(_) = err {
                            continue;
                        }

                        if let ConnectionError::MqttState(rumqttc::v5::StateError::Io(err)) = err {
                            if err.kind() != std::io::ErrorKind::ConnectionAborted {
                                tracing::error!("[Sink] Failed to poll mqtt eventloop: {}", err);
                                std::thread::sleep(std::time::Duration::from_secs(1));
                            }
                        } else {
                            tracing::error!("[Sink] Failed to poll mqtt eventloop: {}", err);
                            std::thread::sleep(std::time::Duration::from_secs(1));
                        }
                    }
                }
            }
        });

        Ok::<_, SinkError>(Self {
            config: config.clone(),
            client,
            qos,
            retain: config.retain,
            schema: schema.clone(),
            stopped,
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

    async fn append_only(&mut self, chunk: StreamChunk) -> Result<()> {
        Retry::spawn(
            ExponentialBackoff::from_millis(100).map(jitter).take(3),
            || async {
                let data = chunk_to_json(chunk.clone(), &self.json_encoder).unwrap();
                for item in data {
                    self.client
                        .publish(
                            &self.config.common.topic,
                            self.qos,
                            self.retain,
                            item.into_bytes(),
                        )
                        .await
                        .context("mqtt sink error")
                        .map_err(SinkError::Mqtt)?;
                }
                Ok::<_, SinkError>(())
            },
        )
        .await
        .context("mqtts sink error")
        .map_err(SinkError::Mqtt)
    }
}

impl AsyncTruncateSinkWriter for MqttSinkWriter {
    async fn write_chunk<'a>(
        &'a mut self,
        chunk: StreamChunk,
        _add_future: DeliveryFutureManagerAddFuture<'a, Self::DeliveryFuture>,
    ) -> Result<()> {
        self.append_only(chunk).await
    }
}

impl Drop for MqttSinkWriter {
    fn drop(&mut self) {
        self.stopped
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

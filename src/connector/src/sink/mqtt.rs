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
use thiserror_ext::AsReport;
use with_options::WithOptions;

use super::catalog::SinkFormatDesc;
use super::formatter::SinkFormatterImpl;
use super::writer::FormattedSink;
use super::{DummySinkCommitCoordinator, SinkWriterParam};
use crate::mqtt_common::MqttCommon;
use crate::sink::catalog::desc::SinkDesc;
use crate::sink::log_store::DeliveryFutureManagerAddFuture;
use crate::sink::writer::{
    AsyncTruncateLogSinkerOf, AsyncTruncateSinkWriter, AsyncTruncateSinkWriterExt,
};
use crate::sink::{Result, Sink, SinkError, SinkParam, SINK_TYPE_APPEND_ONLY};
use crate::{deserialize_bool_from_string, dispatch_sink_formatter_impl};

pub const MQTT_SINK: &str = "mqtt";

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct MqttConfig {
    #[serde(flatten)]
    pub common: MqttCommon,

    /// Whether the message should be retained by the broker
    #[serde(default, deserialize_with = "deserialize_bool_from_string")]
    pub retain: bool,

    // accept "append-only"
    pub r#type: String,
}

#[derive(Clone, Debug)]
pub struct MqttSink {
    pub config: MqttConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    format_desc: SinkFormatDesc,
    db_name: String,
    sink_from_name: String,
    is_append_only: bool,
}

// sink write
pub struct MqttSinkWriter {
    pub config: MqttConfig,
    payload_writer: MqttSinkPayloadWriter,
    schema: Schema,
    formatter: SinkFormatterImpl,
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
            pk_indices: param.downstream_pk,
            format_desc: param
                .format_desc
                .ok_or_else(|| SinkError::Config(anyhow!("missing FORMAT ... ENCODE ...")))?,
            db_name: param.db_name,
            sink_from_name: param.sink_from_name,
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
                "Mqtt sink only support append-only mode"
            )));
        }

        let _client = (self.config.common.build_client(0, 0))
            .context("validate mqtt sink error")
            .map_err(SinkError::Mqtt)?;

        Ok(())
    }

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(MqttSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
            &self.format_desc,
            self.db_name.clone(),
            self.sink_from_name.clone(),
            writer_param.executor_id,
        )
        .await?
        .into_log_sinker(usize::MAX))
    }
}

impl MqttSinkWriter {
    pub async fn new(
        config: MqttConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        format_desc: &SinkFormatDesc,
        db_name: String,
        sink_from_name: String,
        id: u64,
    ) -> Result<Self> {
        let formatter = SinkFormatterImpl::new(
            format_desc,
            schema.clone(),
            pk_indices.clone(),
            db_name,
            sink_from_name,
            &config.common.topic,
        )
        .await?;

        let qos = config.common.qos();

        let (client, mut eventloop) = config
            .common
            .build_client(0, id)
            .map_err(|e| SinkError::Mqtt(anyhow!(e)))?;

        let stopped = Arc::new(AtomicBool::new(false));
        let stopped_clone = stopped.clone();
        tokio::spawn(async move {
            while !stopped_clone.load(std::sync::atomic::Ordering::Relaxed) {
                match eventloop.poll().await {
                    Ok(_) => (),
                    Err(err) => match err {
                        ConnectionError::Timeout(_) => (),
                        ConnectionError::MqttState(rumqttc::v5::StateError::Io(err))
                        | ConnectionError::Io(err)
                            if err.kind() == std::io::ErrorKind::ConnectionAborted
                                || err.kind() == std::io::ErrorKind::ConnectionReset =>
                        {
                            continue;
                        }
                        err => {
                            tracing::error!("Failed to poll mqtt eventloop: {}", err.as_report());
                            std::thread::sleep(std::time::Duration::from_secs(1));
                        }
                    },
                }
            }
        });

        let payload_writer = MqttSinkPayloadWriter {
            client,
            config: config.clone(),
            qos,
            retain: config.retain,
        };

        Ok::<_, SinkError>(Self {
            config: config.clone(),
            payload_writer,
            schema: schema.clone(),
            stopped,
            formatter,
        })
    }
}

impl AsyncTruncateSinkWriter for MqttSinkWriter {
    async fn write_chunk<'a>(
        &'a mut self,
        chunk: StreamChunk,
        _add_future: DeliveryFutureManagerAddFuture<'a, Self::DeliveryFuture>,
    ) -> Result<()> {
        dispatch_sink_formatter_impl!(&self.formatter, formatter, {
            self.payload_writer.write_chunk(chunk, formatter).await
        })
    }
}

impl Drop for MqttSinkWriter {
    fn drop(&mut self) {
        self.stopped
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

struct MqttSinkPayloadWriter {
    // connection to mqtt, one per executor
    client: rumqttc::v5::AsyncClient,
    config: MqttConfig,
    qos: QoS,
    retain: bool,
}

impl FormattedSink for MqttSinkPayloadWriter {
    type K = Vec<u8>;
    type V = Vec<u8>;

    async fn write_one(&mut self, _k: Option<Self::K>, v: Option<Self::V>) -> Result<()> {
        match v {
            Some(v) => self
                .client
                .publish(&self.config.common.topic, self.qos, self.retain, v)
                .await
                .context("mqtt sink error")
                .map_err(SinkError::Mqtt),
            None => Ok(()),
        }
    }
}

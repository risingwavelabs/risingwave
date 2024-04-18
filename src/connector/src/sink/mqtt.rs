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
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::row::Row;
use risingwave_common::session_config::sink_decouple::SinkDecouple;
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::ConnectionError;
use serde_derive::Deserialize;
use serde_with::serde_as;
use thiserror_ext::AsReport;
use with_options::WithOptions;

use super::catalog::{SinkEncode, SinkFormat, SinkFormatDesc};
use super::encoder::{
    DateHandlingMode, JsonEncoder, ProtoEncoder, ProtoHeader, RowEncoder, SerTo, TimeHandlingMode,
    TimestampHandlingMode, TimestamptzHandlingMode,
};
use super::writer::AsyncTruncateSinkWriterExt;
use super::{DummySinkCommitCoordinator, SinkWriterParam};
use crate::connector_common::nested_fields::{
    get_string_field_index_path, get_string_from_index_path,
};
use crate::connector_common::MqttCommon;
use crate::deserialize_bool_from_string;
use crate::sink::catalog::desc::SinkDesc;
use crate::sink::log_store::DeliveryFutureManagerAddFuture;
use crate::sink::writer::{AsyncTruncateLogSinkerOf, AsyncTruncateSinkWriter};
use crate::sink::{Result, Sink, SinkError, SinkParam, SINK_TYPE_APPEND_ONLY};

pub const MQTT_SINK: &str = "mqtt";

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct MqttConfig {
    #[serde(flatten)]
    pub common: MqttCommon,

    /// The topic name to subscribe or publish to. When subscribing, it can be a wildcard topic. e.g /topic/#
    pub topic: Option<String>,

    /// Whether the message should be retained by the broker
    #[serde(default, deserialize_with = "deserialize_bool_from_string")]
    pub retain: bool,

    // accept "append-only"
    pub r#type: String,

    /// if set, will use a field value as the topic name, if topic is also set it will be used as a fallback
    #[serde(rename = "topic.field")]
    pub topic_field: Option<String>,
}

pub enum RowEncoderWrapper {
    Json(JsonEncoder),
    Proto(ProtoEncoder),
}

impl RowEncoder for RowEncoderWrapper {
    type Output = Vec<u8>;

    fn encode_cols(
        &self,
        row: impl Row,
        col_indices: impl Iterator<Item = usize>,
    ) -> Result<Self::Output> {
        match self {
            RowEncoderWrapper::Json(json) => json.encode_cols(row, col_indices)?.ser_to(),
            RowEncoderWrapper::Proto(proto) => proto.encode_cols(row, col_indices)?.ser_to(),
        }
    }

    fn schema(&self) -> &Schema {
        match self {
            RowEncoderWrapper::Json(json) => json.schema(),
            RowEncoderWrapper::Proto(proto) => proto.schema(),
        }
    }

    fn col_indices(&self) -> Option<&[usize]> {
        match self {
            RowEncoderWrapper::Json(json) => json.col_indices(),
            RowEncoderWrapper::Proto(proto) => proto.col_indices(),
        }
    }

    fn encode(&self, row: impl Row) -> Result<Self::Output> {
        match self {
            RowEncoderWrapper::Json(json) => json.encode(row)?.ser_to(),
            RowEncoderWrapper::Proto(proto) => proto.encode(row)?.ser_to(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct MqttSink {
    pub config: MqttConfig,
    schema: Schema,
    format_desc: SinkFormatDesc,
    is_append_only: bool,
    name: String,
}

// sink write
pub struct MqttSinkWriter {
    pub config: MqttConfig,
    payload_writer: MqttSinkPayloadWriter,
    schema: Schema,
    encoder: RowEncoderWrapper,
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
            name: param.sink_name,
            format_desc: param
                .format_desc
                .ok_or_else(|| SinkError::Config(anyhow!("missing FORMAT ... ENCODE ...")))?,
            is_append_only: param.sink_type.is_append_only(),
        })
    }
}

impl Sink for MqttSink {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = AsyncTruncateLogSinkerOf<MqttSinkWriter>;

    const SINK_NAME: &'static str = MQTT_SINK;

    fn is_sink_decouple(desc: &SinkDesc, user_specified: &SinkDecouple) -> Result<bool> {
        match user_specified {
            SinkDecouple::Default => Ok(desc.sink_type.is_append_only()),
            SinkDecouple::Disable => Ok(false),
            SinkDecouple::Enable => Ok(true),
        }
    }

    async fn validate(&self) -> Result<()> {
        if !self.is_append_only {
            return Err(SinkError::Mqtt(anyhow!(
                "Mqtt sink only support append-only mode"
            )));
        }

        if let Some(field) = &self.config.topic_field {
            let _ = get_string_field_index_path(&self.schema, field.as_str())
                .map_err(SinkError::Config)?;
        } else if self.config.topic.is_none() {
            return Err(SinkError::Config(anyhow!(
                "either topic or topic.field must be set"
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
            &self.format_desc,
            &self.name,
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
        format_desc: &SinkFormatDesc,
        name: &str,
        id: u64,
    ) -> Result<Self> {
        let mut topic_index_path = vec![];
        if let Some(field) = &config.topic_field {
            topic_index_path =
                get_string_field_index_path(&schema, field.as_str()).map_err(SinkError::Config)?;
        }

        let timestamptz_mode = TimestamptzHandlingMode::from_options(&format_desc.options)?;

        let encoder = match format_desc.format {
            SinkFormat::AppendOnly => match format_desc.encode {
                SinkEncode::Json => RowEncoderWrapper::Json(JsonEncoder::new(
                    schema.clone(),
                    None,
                    DateHandlingMode::FromCe,
                    TimestampHandlingMode::Milli,
                    timestamptz_mode,
                    TimeHandlingMode::Milli,
                )),
                SinkEncode::Protobuf => {
                    let (descriptor, sid) = crate::schema::protobuf::fetch_descriptor(
                        &format_desc.options,
                        config.topic.as_deref().unwrap_or(name),
                        None,
                    )
                    .await
                    .map_err(|e| SinkError::Config(anyhow!(e)))?;
                    let header = match sid {
                        None => ProtoHeader::None,
                        Some(sid) => ProtoHeader::ConfluentSchemaRegistry(sid),
                    };
                    RowEncoderWrapper::Proto(ProtoEncoder::new(
                        schema.clone(),
                        None,
                        descriptor,
                        header,
                    )?)
                }
                _ => {
                    return Err(SinkError::Config(anyhow!(
                        "mqtt sink encode unsupported: {:?}",
                        format_desc.encode,
                    )))
                }
            },
            _ => {
                return Err(SinkError::Config(anyhow!(
                    "Mqtt sink only support append-only mode"
                )))
            }
        };
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
            topic: config.topic.clone(),
            client,
            qos,
            retain: config.retain,
            topic_index_path,
        };

        Ok::<_, SinkError>(Self {
            config: config.clone(),
            payload_writer,
            schema: schema.clone(),
            stopped,
            encoder,
        })
    }
}

impl AsyncTruncateSinkWriter for MqttSinkWriter {
    async fn write_chunk<'a>(
        &'a mut self,
        chunk: StreamChunk,
        _add_future: DeliveryFutureManagerAddFuture<'a, Self::DeliveryFuture>,
    ) -> Result<()> {
        self.payload_writer.write_chunk(chunk, &self.encoder).await
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
    topic: Option<String>,
    qos: QoS,
    retain: bool,
    topic_index_path: Vec<usize>,
}

impl MqttSinkPayloadWriter {
    async fn write_chunk(&mut self, chunk: StreamChunk, encoder: &RowEncoderWrapper) -> Result<()> {
        for (op, row) in chunk.rows() {
            if op != Op::Insert {
                continue;
            }

            let topic = match get_string_from_index_path(
                &self.topic_index_path,
                self.topic.as_deref(),
                &row,
            ) {
                Some(s) => s,
                None => {
                    tracing::error!("topic field not found in row, skipping: {:?}", row);
                    return Ok(());
                }
            };

            let v = encoder.encode(row)?;

            self.client
                .publish(topic, self.qos, self.retain, v)
                .await
                .context("mqtt sink error")
                .map_err(SinkError::Mqtt)?;
        }

        Ok(())
    }
}

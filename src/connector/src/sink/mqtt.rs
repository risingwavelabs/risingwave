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
use risingwave_common::types::{DataType, ScalarRefImpl};
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
use crate::deserialize_bool_from_string;
use crate::connector_common::MqttCommon;
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

    // if set, will use a field value as the topic name, if topic is also set it will be used as a fallback
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
            let mut iter = field.split('.');
            let dt = iter
                .next()
                .and_then(|field| {
                    self.schema
                        .fields()
                        .iter()
                        .find(|f| f.name == field)
                        .map(|f| &f.data_type)
                })
                .and_then(|dt| {
                    iter.try_fold(dt, |dt, field| match dt {
                        DataType::Struct(st) => {
                            st.iter().find(|(s, _)| *s == field).map(|(_, dt)| dt)
                        }
                        _ => None,
                    })
                });
            match dt {
                Some(DataType::Varchar) => (),
                Some(dt) => {
                    return Err(SinkError::Config(anyhow!(
                        "topic field `{}` must be of type string but got {:?}",
                        field,
                        dt
                    )))
                }
                None => {
                    return Err(SinkError::Config(anyhow!(
                        "topic field `{}`  not found",
                        field
                    )))
                }
            }
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
        let mut path = vec![];
        if let Some(field) = &config.topic_field {
            let mut iter = field.split('.');
            let dt = iter
                .next()
                .and_then(|field| {
                    schema
                        .fields()
                        .iter()
                        .enumerate()
                        .find(|(_, f)| f.name == field)
                        .map(|(pos, f)| {
                            path.push(pos);
                            &f.data_type
                        })
                })
                .and_then(|dt| {
                    iter.try_fold(dt, |dt, field| match dt {
                        DataType::Struct(st) => {
                            st.iter().enumerate().find(|(_, (s, _))| *s == field).map(
                                |(pos, (_, dt))| {
                                    path.push(pos);
                                    dt
                                },
                            )
                        }
                        _ => None,
                    })
                });
            if !matches!(dt, Some(DataType::Varchar)) {
                return Err(SinkError::Config(anyhow!(
                    "topic field must be of type string but got {:?}",
                    dt
                )));
            }
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
            path,
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
    path: Vec<usize>,
}

impl MqttSinkPayloadWriter {}

impl MqttSinkPayloadWriter {
    async fn write_chunk(&mut self, chunk: StreamChunk, encoder: &RowEncoderWrapper) -> Result<()> {
        for (op, row) in chunk.rows() {
            if op != Op::Insert {
                continue;
            }

            let topic = if let Some(topic) = &self.topic
                && self.path.is_empty()
            {
                topic.as_str()
            } else {
                let mut iter = self.path.iter();

                let scalar = iter
                    .next()
                    .and_then(|pos| row.datum_at(*pos))
                    .and_then(|d| {
                        iter.try_fold(d, |d, pos| match d {
                            ScalarRefImpl::Struct(struct_ref) => {
                                struct_ref.iter_fields_ref().nth(*pos).flatten()
                            }
                            _ => None,
                        })
                    });
                match scalar {
                    Some(ScalarRefImpl::Utf8(s)) => s,
                    _ => {
                        if let Some(topic) = &self.topic {
                            topic.as_str()
                        } else {
                            tracing::error!("topic field not found in row, skipping: {:?}", row);
                            return Ok(());
                        }
                    }
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

// Copyright 2026 RisingWave Labs
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
use std::collections::BTreeMap;

use anyhow::{Context as _, anyhow};
use lapin::options::{BasicPublishOptions, ConfirmSelectOptions};
use lapin::{BasicProperties, Channel, Confirmation, Connection};
use risingwave_common::array::{Op, RowRef, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, ScalarRefImpl};
use serde::Deserialize;
use serde_with::serde_as;
use with_options::WithOptions;

use super::SinkWriterParam;
use super::catalog::{SinkEncode, SinkFormat, SinkFormatDesc};
use super::encoder::{
    DateHandlingMode, JsonEncoder, JsonbHandlingMode, RowEncoder, SerTo, TimeHandlingMode,
    TimestampHandlingMode, TimestamptzHandlingMode,
};
use super::writer::{
    AsyncTruncateLogSinkerOf, AsyncTruncateSinkWriter, AsyncTruncateSinkWriterExt,
};
use crate::connector_common::RabbitMqCommon;
use crate::enforce_secret::EnforceSecret;
use crate::sink::log_store::DeliveryFutureManagerAddFuture;
use crate::sink::{Result, SINK_TYPE_APPEND_ONLY, Sink, SinkError, SinkParam};

pub const RABBITMQ_SINK: &str = "rabbitmq";

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct RabbitMqConfig {
    #[serde(flatten)]
    pub common: RabbitMqCommon,

    /// The exchange to which messages are published.
    pub exchange: String,

    /// The routing key used for every message.
    pub routing_key: Option<String>,

    /// A `varchar` column whose value is used as the routing key.
    /// `routing_key` is used as a fallback when this column is null.
    #[serde(rename = "routing_key.field")]
    pub routing_key_field: Option<String>,

    /// The sink operation mode. `RabbitMQ` currently only supports `append-only`.
    pub r#type: String,
}

#[derive(Clone, Debug)]
pub struct RabbitMqSink {
    pub config: RabbitMqConfig,
    schema: Schema,
    format_desc: SinkFormatDesc,
    is_append_only: bool,
}

impl EnforceSecret for RabbitMqSink {
    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::error::ConnectorResult<()> {
        for prop in prop_iter {
            RabbitMqCommon::enforce_one(prop)?;
        }
        Ok(())
    }
}

pub struct RabbitMqSinkWriter {
    config: RabbitMqConfig,
    // Keep the connection alive for as long as the channel is in use.
    #[expect(dead_code)]
    connection: Connection,
    channel: Channel,
    encoder: JsonEncoder,
    routing_key_index_path: Vec<usize>,
}

impl RabbitMqConfig {
    pub fn from_btreemap(values: BTreeMap<String, String>) -> Result<Self> {
        let config =
            serde_json::from_value::<RabbitMqConfig>(serde_json::to_value(values).unwrap())
                .map_err(|e| SinkError::Config(anyhow!(e)))?;

        if config.r#type != SINK_TYPE_APPEND_ONLY {
            return Err(SinkError::Config(anyhow!(
                "RabbitMQ sink only supports append-only mode"
            )));
        }

        if config.routing_key.is_none() && config.routing_key_field.is_none() {
            return Err(SinkError::Config(anyhow!(
                "either routing_key or routing_key.field must be set"
            )));
        }

        Ok(config)
    }
}

impl TryFrom<SinkParam> for RabbitMqSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let config = RabbitMqConfig::from_btreemap(param.properties)?;
        let format_desc = param
            .format_desc
            .ok_or_else(|| SinkError::Config(anyhow!("missing FORMAT ... ENCODE ...")))?;

        Ok(Self {
            config,
            schema,
            format_desc,
            is_append_only: param.sink_type.is_append_only(),
        })
    }
}

impl Sink for RabbitMqSink {
    type LogSinker = AsyncTruncateLogSinkerOf<RabbitMqSinkWriter>;

    const SINK_NAME: &'static str = RABBITMQ_SINK;

    async fn validate(&self) -> Result<()> {
        if !self.is_append_only {
            return Err(SinkError::RabbitMq(anyhow!(
                "RabbitMQ sink only supports append-only mode"
            )));
        }

        validate_format(&self.format_desc)?;

        if let Some(field) = &self.config.routing_key_field {
            get_routing_key_field_index_path(&self.schema, field)?;
        }

        let connection = self
            .config
            .common
            .build_client()
            .await
            .context("failed to validate RabbitMQ sink connection")
            .map_err(SinkError::RabbitMq)?;
        let channel = connection
            .create_channel()
            .await
            .context("failed to create RabbitMQ channel")
            .map_err(SinkError::RabbitMq)?;

        if self.config.common.publisher_confirm {
            channel
                .confirm_select(ConfirmSelectOptions::default())
                .await
                .context("failed to enable RabbitMQ publisher confirms")
                .map_err(SinkError::RabbitMq)?;
        }

        Ok(())
    }

    async fn new_log_sinker(&self, _writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(
            RabbitMqSinkWriter::new(self.config.clone(), self.schema.clone(), &self.format_desc)
                .await?
                .into_log_sinker(usize::MAX),
        )
    }
}

impl RabbitMqSinkWriter {
    async fn new(
        config: RabbitMqConfig,
        schema: Schema,
        format_desc: &SinkFormatDesc,
    ) -> Result<Self> {
        validate_format(format_desc)?;

        let routing_key_index_path = config
            .routing_key_field
            .as_deref()
            .map(|field| get_routing_key_field_index_path(&schema, field))
            .transpose()?
            .unwrap_or_default();

        let encoder = JsonEncoder::new(
            schema,
            None,
            DateHandlingMode::FromCe,
            TimestampHandlingMode::Milli,
            TimestamptzHandlingMode::from_options(&format_desc.options)?,
            TimeHandlingMode::Milli,
            JsonbHandlingMode::from_options(&format_desc.options)?,
        );

        let connection = config
            .common
            .build_client()
            .await
            .context("failed to connect RabbitMQ sink writer")
            .map_err(SinkError::RabbitMq)?;
        let channel = connection
            .create_channel()
            .await
            .context("failed to create RabbitMQ sink channel")
            .map_err(SinkError::RabbitMq)?;

        if config.common.publisher_confirm {
            channel
                .confirm_select(ConfirmSelectOptions::default())
                .await
                .context("failed to enable RabbitMQ publisher confirms")
                .map_err(SinkError::RabbitMq)?;
        }

        Ok(Self {
            config,
            connection,
            channel,
            encoder,
            routing_key_index_path,
        })
    }

    async fn publish(&self, routing_key: &str, payload: &[u8]) -> Result<()> {
        let confirm = self
            .channel
            .basic_publish(
                self.config.exchange.as_str().into(),
                routing_key.into(),
                BasicPublishOptions {
                    mandatory: true,
                    ..Default::default()
                },
                payload,
                BasicProperties::default().with_content_type("application/json".into()),
            )
            .await
            .context("failed to publish message to RabbitMQ")
            .map_err(SinkError::RabbitMq)?;

        match confirm
            .await
            .context("failed to receive RabbitMQ publisher confirmation")
            .map_err(SinkError::RabbitMq)?
        {
            Confirmation::Ack(None) | Confirmation::NotRequested => Ok(()),
            Confirmation::Ack(Some(returned)) => Err(SinkError::RabbitMq(anyhow!(
                "RabbitMQ returned an unroutable message: {returned:?}"
            ))),
            Confirmation::Nack(returned) => Err(SinkError::RabbitMq(anyhow!(
                "RabbitMQ negatively acknowledged a published message: {returned:?}"
            ))),
        }
    }
}

impl AsyncTruncateSinkWriter for RabbitMqSinkWriter {
    async fn write_chunk<'a>(
        &'a mut self,
        chunk: StreamChunk,
        _add_future: DeliveryFutureManagerAddFuture<'a, Self::DeliveryFuture>,
    ) -> Result<()> {
        for (op, row) in chunk.rows() {
            if op != Op::Insert {
                continue;
            }

            let routing_key = get_routing_key(
                &self.routing_key_index_path,
                self.config.routing_key.as_deref(),
                &row,
            )
            .ok_or_else(|| {
                SinkError::RabbitMq(anyhow!(
                    "routing key field `{}` is null",
                    self.config.routing_key_field.as_deref().unwrap_or_default()
                ))
            })?;
            let payload: Vec<u8> = self.encoder.encode(row)?.ser_to()?;

            self.publish(&routing_key, &payload).await?;
        }

        Ok(())
    }
}

fn validate_format(format_desc: &SinkFormatDesc) -> Result<()> {
    if format_desc.format != SinkFormat::AppendOnly || format_desc.encode != SinkEncode::Json {
        return Err(SinkError::Config(anyhow!(
            "RabbitMQ sink only supports FORMAT PLAIN ENCODE JSON"
        )));
    }
    Ok(())
}

fn get_routing_key(
    path: &[usize],
    default_routing_key: Option<&str>,
    row: &RowRef<'_>,
) -> Option<String> {
    if path.is_empty() {
        return default_routing_key.map(ToOwned::to_owned);
    }

    let mut positions = path.iter();
    let value = positions
        .next()
        .and_then(|position| row.datum_at(*position))
        .and_then(|value| {
            positions.try_fold(value, |value, position| match value {
                ScalarRefImpl::Struct(struct_ref) => {
                    struct_ref.iter_fields_ref().nth(*position).flatten()
                }
                _ => None,
            })
        });

    match value {
        Some(ScalarRefImpl::Utf8(value)) => Some(value.to_owned()),
        _ => default_routing_key.map(ToOwned::to_owned),
    }
}

fn get_routing_key_field_index_path(schema: &Schema, field_name: &str) -> Result<Vec<usize>> {
    let mut names = field_name.split('.');
    let mut path = Vec::new();

    let mut data_type = names
        .next()
        .and_then(|name| {
            schema
                .fields()
                .iter()
                .enumerate()
                .find(|(_, field)| field.name == name)
                .map(|(index, field)| {
                    path.push(index);
                    &field.data_type
                })
        })
        .ok_or_else(|| {
            SinkError::Config(anyhow!("routing key field `{field_name}` does not exist"))
        })?;

    for name in names {
        data_type = match data_type {
            DataType::Struct(struct_type) => struct_type
                .iter()
                .enumerate()
                .find(|(_, (nested_name, _))| *nested_name == name)
                .map(|(index, (_, nested_type))| {
                    path.push(index);
                    nested_type
                })
                .ok_or_else(|| {
                    SinkError::Config(anyhow!("routing key field `{field_name}` does not exist"))
                })?,
            _ => {
                return Err(SinkError::Config(anyhow!(
                    "routing key field `{field_name}` traverses a non-struct value"
                )));
            }
        };
    }

    if data_type != &DataType::Varchar {
        return Err(SinkError::Config(anyhow!(
            "routing key field `{field_name}` must be varchar, got {data_type}"
        )));
    }

    Ok(path)
}

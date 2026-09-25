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

use anyhow::anyhow;
use risingwave_common::catalog::Schema;
use risingwave_common::row::Row;

use crate::sink::catalog::{SinkEncode, SinkFormat, SinkFormatDesc};
use crate::sink::encoder::{
    DateHandlingMode, JsonEncoder, JsonbHandlingMode, ProtoEncoder, ProtoHeader, RowEncoder, SerTo,
    TimeHandlingMode, TimestampHandlingMode, TimestamptzHandlingMode,
};
use crate::sink::{Result, SinkError};

/// Encodes one append-only row into an AMQP message body.
pub enum RabbitMqEncoder {
    Json(JsonEncoder),
    Protobuf(ProtoEncoder),
}

impl RabbitMqEncoder {
    /// `schema_subject` is the topic-equivalent name used by schema registry subject strategies.
    pub async fn new(
        schema: Schema,
        format_desc: &SinkFormatDesc,
        schema_subject: &str,
    ) -> Result<Self> {
        if format_desc.format != SinkFormat::AppendOnly {
            return Err(SinkError::Config(anyhow!(
                "RabbitMQ sink only supports append-only mode"
            )));
        }
        if format_desc.key_encode.is_some() {
            return Err(SinkError::Config(anyhow!(
                "RabbitMQ sink does not support KEY ENCODE"
            )));
        }
        match format_desc.encode {
            SinkEncode::Json => Ok(Self::Json(JsonEncoder::new(
                schema,
                None,
                DateHandlingMode::FromCe,
                TimestampHandlingMode::Milli,
                TimestamptzHandlingMode::from_options(&format_desc.options)?,
                TimeHandlingMode::Milli,
                JsonbHandlingMode::from_options(&format_desc.options)?,
            ))),
            SinkEncode::Protobuf => {
                let (descriptor, schema_id) = crate::schema::protobuf::fetch_descriptor(
                    &format_desc.options,
                    schema_subject,
                    None,
                )
                .await
                .map_err(|e| SinkError::Config(anyhow!(e)))?;
                let header = match schema_id {
                    Some(id) => ProtoHeader::ConfluentSchemaRegistry(id),
                    None => ProtoHeader::None,
                };
                Ok(Self::Protobuf(ProtoEncoder::new(
                    schema, None, descriptor, header,
                )?))
            }
            _ => Err(SinkError::Config(anyhow!(
                "RabbitMQ sink encode unsupported: {:?}",
                format_desc.encode
            ))),
        }
    }

    pub fn encode(&self, row: impl Row) -> Result<Vec<u8>> {
        match self {
            Self::Json(encoder) => encoder.encode(row)?.ser_to(),
            Self::Protobuf(encoder) => encoder.encode(row)?.ser_to(),
        }
    }

    pub fn content_type(&self) -> &'static str {
        match self {
            Self::Json(_) => "application/json",
            Self::Protobuf(_) => "application/x-protobuf",
        }
    }
}

impl From<JsonEncoder> for RabbitMqEncoder {
    fn from(encoder: JsonEncoder) -> Self {
        Self::Json(encoder)
    }
}

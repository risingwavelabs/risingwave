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

use anyhow::anyhow;
use risingwave_common::array::StreamChunk;

use crate::sink::{Result, SinkError};

mod append_only;
mod debezium_json;
mod upsert;

pub use append_only::AppendOnlyFormatter;
pub use debezium_json::{DebeziumAdapterOpts, DebeziumJsonFormatter};
use risingwave_common::catalog::Schema;
pub use upsert::UpsertFormatter;

use super::catalog::{SinkEncode, SinkFormat, SinkFormatDesc};
use super::encoder::template::TemplateEncoder;
use super::encoder::KafkaConnectParams;
use crate::sink::encoder::{
    AvroEncoder, AvroHeader, JsonEncoder, ProtoEncoder, TimestampHandlingMode,
};

/// Transforms a `StreamChunk` into a sequence of key-value pairs according a specific format,
/// for example append-only, upsert or debezium.
pub trait SinkFormatter {
    type K;
    type V;

    /// * Key may be None so that messages are partitioned using round-robin.
    /// For example append-only without `primary_key` (aka `downstream_pk`) set.
    /// * Value may be None so that messages with same key are removed during log compaction.
    /// For example debezium tombstone event.
    fn format_chunk(
        &self,
        chunk: &StreamChunk,
    ) -> impl Iterator<Item = Result<(Option<Self::K>, Option<Self::V>)>>;
}

/// `tri!` in generators yield `Err` and return `()`
/// `?` in generators return `Err`
#[macro_export]
macro_rules! tri {
    ($expr:expr) => {
        match $expr {
            Ok(val) => val,
            Err(err) => {
                yield Err(err);
                return;
            }
        }
    };
}

pub enum SinkFormatterImpl {
    AppendOnlyJson(AppendOnlyFormatter<JsonEncoder, JsonEncoder>),
    AppendOnlyProto(AppendOnlyFormatter<JsonEncoder, ProtoEncoder>),
    UpsertJson(UpsertFormatter<JsonEncoder, JsonEncoder>),
    UpsertAvro(UpsertFormatter<AvroEncoder, AvroEncoder>),
    DebeziumJson(DebeziumJsonFormatter),
    AppendOnlyTemplate(AppendOnlyFormatter<TemplateEncoder, TemplateEncoder>),
    UpsertTemplate(UpsertFormatter<TemplateEncoder, TemplateEncoder>),
}

impl SinkFormatterImpl {
    pub async fn new(
        format_desc: &SinkFormatDesc,
        schema: Schema,
        pk_indices: Vec<usize>,
        db_name: String,
        sink_from_name: String,
        topic: &str,
    ) -> Result<Self> {
        let err_unsupported = || {
            Err(SinkError::Config(anyhow!(
                "sink format/encode unsupported: {:?} {:?}",
                format_desc.format,
                format_desc.encode,
            )))
        };

        match format_desc.format {
            SinkFormat::AppendOnly => {
                let key_encoder = (!pk_indices.is_empty()).then(|| {
                    JsonEncoder::new(
                        schema.clone(),
                        Some(pk_indices),
                        TimestampHandlingMode::Milli,
                    )
                });

                match format_desc.encode {
                    SinkEncode::Json => {
                        let val_encoder =
                            JsonEncoder::new(schema, None, TimestampHandlingMode::Milli);
                        let formatter = AppendOnlyFormatter::new(key_encoder, val_encoder);
                        Ok(SinkFormatterImpl::AppendOnlyJson(formatter))
                    }
                    SinkEncode::Protobuf => {
                        // By passing `None` as `aws_auth_props`, reading from `s3://` not supported yet.
                        let descriptor =
                            crate::schema::protobuf::fetch_descriptor(&format_desc.options, None)
                                .await
                                .map_err(|e| SinkError::Config(anyhow!("{e:?}")))?;
                        let val_encoder = ProtoEncoder::new(schema, None, descriptor)?;
                        let formatter = AppendOnlyFormatter::new(key_encoder, val_encoder);
                        Ok(SinkFormatterImpl::AppendOnlyProto(formatter))
                    }
                    SinkEncode::Avro => err_unsupported(),
                }
            }
            SinkFormat::Debezium => {
                if format_desc.encode != SinkEncode::Json {
                    return err_unsupported();
                }

                Ok(SinkFormatterImpl::DebeziumJson(DebeziumJsonFormatter::new(
                    schema,
                    pk_indices,
                    db_name,
                    sink_from_name,
                    DebeziumAdapterOpts::default(),
                )))
            }
            SinkFormat::Upsert => {
                match format_desc.encode {
                    SinkEncode::Json => {
                        let mut key_encoder = JsonEncoder::new(
                            schema.clone(),
                            Some(pk_indices),
                            TimestampHandlingMode::Milli,
                        );
                        let mut val_encoder =
                            JsonEncoder::new(schema, None, TimestampHandlingMode::Milli);

                        if let Some(s) = format_desc.options.get("schemas.enable") {
                            match s.to_lowercase().parse::<bool>() {
                                Ok(true) => {
                                    let kafka_connect = KafkaConnectParams {
                                        schema_name: format!("{}.{}", db_name, sink_from_name),
                                    };
                                    key_encoder =
                                        key_encoder.with_kafka_connect(kafka_connect.clone());
                                    val_encoder = val_encoder.with_kafka_connect(kafka_connect);
                                }
                                Ok(false) => {}
                                _ => {
                                    return Err(SinkError::Config(anyhow!(
                                        "schemas.enable is expected to be `true` or `false`, got {}",
                                        s
                                    )));
                                }
                            }
                        };

                        // Initialize the upsert_stream
                        let formatter = UpsertFormatter::new(key_encoder, val_encoder);
                        Ok(SinkFormatterImpl::UpsertJson(formatter))
                    }
                    SinkEncode::Avro => {
                        let (key_schema, val_schema) =
                            crate::schema::avro::fetch_schema(&format_desc.options, topic)
                                .await
                                .map_err(|e| SinkError::Config(anyhow!("{e:?}")))?;
                        let key_encoder = AvroEncoder::new(
                            schema.clone(),
                            Some(pk_indices),
                            key_schema.schema,
                            AvroHeader::ConfluentSchemaRegistry(key_schema.id),
                        )?;
                        let val_encoder = AvroEncoder::new(
                            schema.clone(),
                            None,
                            val_schema.schema,
                            AvroHeader::ConfluentSchemaRegistry(val_schema.id),
                        )?;
                        let formatter = UpsertFormatter::new(key_encoder, val_encoder);
                        Ok(SinkFormatterImpl::UpsertAvro(formatter))
                    }
                    SinkEncode::Protobuf => err_unsupported(),
                }
            }
        }
    }

    pub fn new_with_redis(
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
        key_format: Option<String>,
        value_format: Option<String>,
    ) -> Result<Self> {
        match (key_format, value_format) {
            (Some(k), Some(v)) => {
                let key_encoder = TemplateEncoder::new(
                    schema.clone(),
                    Some(pk_indices),
                    k,
                );
                let val_encoder =
                    TemplateEncoder::new(schema, None, v);
                if is_append_only {
                    Ok(SinkFormatterImpl::AppendOnlyTemplate(AppendOnlyFormatter::new(Some(key_encoder), val_encoder)))
                } else {
                    Ok(SinkFormatterImpl::UpsertTemplate(UpsertFormatter::new(key_encoder, val_encoder)))
                }
            }
            (None, None) => {
                let key_encoder = JsonEncoder::new(
                    schema.clone(),
                    Some(pk_indices),
                    TimestampHandlingMode::Milli,
                );
                let val_encoder = JsonEncoder::new(
                    schema,
                    None,
                    TimestampHandlingMode::Milli,
                );
                if is_append_only {
                    Ok(SinkFormatterImpl::AppendOnlyJson(AppendOnlyFormatter::new(Some(key_encoder), val_encoder)))
                } else {
                    Ok(SinkFormatterImpl::UpsertJson(UpsertFormatter::new(key_encoder, val_encoder)))
                }
            }
            _ => {
                Err(SinkError::Encode("Please provide template formats for both key and value, or choose the JSON format.".to_string()))
            }
        }
    }
}

#[macro_export]
macro_rules! dispatch_sink_formatter_impl {
    ($impl:expr, $name:ident, $body:expr) => {
        match $impl {
            SinkFormatterImpl::AppendOnlyJson($name) => $body,
            SinkFormatterImpl::AppendOnlyProto($name) => $body,
            SinkFormatterImpl::UpsertJson($name) => $body,
            SinkFormatterImpl::UpsertAvro($name) => $body,
            SinkFormatterImpl::DebeziumJson($name) => $body,
            SinkFormatterImpl::AppendOnlyTemplate($name) => $body,
            SinkFormatterImpl::UpsertTemplate($name) => $body,
        }
    };
}

#[macro_export]
macro_rules! dispatch_sink_formatter_str_key_impl {
    ($impl:expr, $name:ident, $body:expr) => {
        match $impl {
            SinkFormatterImpl::AppendOnlyJson($name) => $body,
            SinkFormatterImpl::AppendOnlyProto($name) => $body,
            SinkFormatterImpl::UpsertJson($name) => $body,
            SinkFormatterImpl::UpsertAvro(_) => unreachable!(),
            SinkFormatterImpl::DebeziumJson($name) => $body,
            SinkFormatterImpl::AppendOnlyTemplate($name) => $body,
            SinkFormatterImpl::UpsertTemplate($name) => $body,
        }
    };
}

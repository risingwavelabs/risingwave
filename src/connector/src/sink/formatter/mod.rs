// Copyright 2025 RisingWave Labs
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

use anyhow::{Context, anyhow};
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Field;

use crate::sink::{Result, SinkError};

mod append_only;
mod debezium_json;
mod upsert;

pub use append_only::AppendOnlyFormatter;
pub use debezium_json::{DebeziumAdapterOpts, DebeziumJsonFormatter};
use risingwave_common::catalog::Schema;
use risingwave_common::types::DataType;
pub use upsert::UpsertFormatter;

use super::catalog::{SinkEncode, SinkFormat, SinkFormatDesc};
use super::encoder::bytes::BytesEncoder;
use super::encoder::template::TemplateEncoder;
use super::encoder::text::TextEncoder;
use super::encoder::{
    DateHandlingMode, JsonbHandlingMode, KafkaConnectParams, TimeHandlingMode,
    TimestamptzHandlingMode,
};
use super::redis::{
    CHANNEL, CHANNEL_COLUMN, KEY_FORMAT, LAT_NAME, LON_NAME, MEMBER_NAME, REDIS_VALUE_TYPE,
    REDIS_VALUE_TYPE_GEO, REDIS_VALUE_TYPE_PUBSUB, REDIS_VALUE_TYPE_STRING, VALUE_FORMAT,
};
use crate::sink::encoder::{
    AvroEncoder, AvroHeader, JsonEncoder, ProtoEncoder, ProtoHeader, TimestampHandlingMode,
};

/// Transforms a `StreamChunk` into a sequence of key-value pairs according a specific format,
/// for example append-only, upsert or debezium.
pub trait SinkFormatter {
    type K;
    type V;

    /// * Key may be None so that messages are partitioned using round-robin.
    ///   For example append-only without `primary_key` (aka `downstream_pk`) set.
    /// * Value may be None so that messages with same key are removed during log compaction.
    ///   For example debezium tombstone event.
    #[expect(clippy::type_complexity)]
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
    // append-only
    AppendOnlyJson(AppendOnlyFormatter<JsonEncoder, JsonEncoder>),
    AppendOnlyTextJson(AppendOnlyFormatter<TextEncoder, JsonEncoder>),
    AppendOnlyBytesJson(AppendOnlyFormatter<BytesEncoder, JsonEncoder>),
    AppendOnlyAvro(AppendOnlyFormatter<AvroEncoder, AvroEncoder>),
    AppendOnlyTextAvro(AppendOnlyFormatter<TextEncoder, AvroEncoder>),
    AppendOnlyBytesAvro(AppendOnlyFormatter<BytesEncoder, AvroEncoder>),
    AppendOnlyProto(AppendOnlyFormatter<JsonEncoder, ProtoEncoder>),
    AppendOnlyTextProto(AppendOnlyFormatter<TextEncoder, ProtoEncoder>),
    AppendOnlyBytesProto(AppendOnlyFormatter<BytesEncoder, ProtoEncoder>),
    AppendOnlyTemplate(AppendOnlyFormatter<TemplateEncoder, TemplateEncoder>),
    AppendOnlyTextTemplate(AppendOnlyFormatter<TextEncoder, TemplateEncoder>),
    AppendOnlyBytesTemplate(AppendOnlyFormatter<BytesEncoder, TemplateEncoder>),
    // upsert
    UpsertJson(UpsertFormatter<JsonEncoder, JsonEncoder>),
    UpsertTextJson(UpsertFormatter<TextEncoder, JsonEncoder>),
    UpsertBytesJson(UpsertFormatter<BytesEncoder, JsonEncoder>),
    UpsertAvro(UpsertFormatter<AvroEncoder, AvroEncoder>),
    UpsertTextAvro(UpsertFormatter<TextEncoder, AvroEncoder>),
    UpsertBytesAvro(UpsertFormatter<BytesEncoder, AvroEncoder>),
    // `UpsertFormatter<ProtoEncoder, ProtoEncoder>` is intentionally left out
    // to avoid using `ProtoEncoder` as key:
    // <https://docs.confluent.io/platform/7.7/control-center/topics/schema.html#c3-schemas-best-practices-key-value-pairs>
    UpsertTextProto(UpsertFormatter<TextEncoder, ProtoEncoder>),
    UpsertBytesProto(UpsertFormatter<BytesEncoder, ProtoEncoder>),
    UpsertTemplate(UpsertFormatter<TemplateEncoder, TemplateEncoder>),
    UpsertTextTemplate(UpsertFormatter<TextEncoder, TemplateEncoder>),
    UpsertBytesTemplate(UpsertFormatter<BytesEncoder, TemplateEncoder>),
    // debezium
    DebeziumJson(DebeziumJsonFormatter),
}

#[derive(Debug, Clone)]
pub struct EncoderParams<'a> {
    format_desc: &'a SinkFormatDesc,
    schema: Schema,
    db_name: String,
    sink_from_name: String,
    topic: &'a str,
}

/// Each encoder shall be able to be built from parameters.
///
/// This is not part of `RowEncoder` trait, because that one is about how an encoder completes its
/// own job as a self-contained unit, with a custom `new` asking for only necessary info; while this
/// one is about how different encoders can be selected from a common SQL interface.
pub trait EncoderBuild: Sized {
    /// Pass `pk_indices: None` for value/payload and `Some` for key. Certain encoder builds
    /// differently when used as key vs value.
    async fn build(params: EncoderParams<'_>, pk_indices: Option<Vec<usize>>) -> Result<Self>;
}

impl EncoderBuild for JsonEncoder {
    async fn build(b: EncoderParams<'_>, pk_indices: Option<Vec<usize>>) -> Result<Self> {
        let timestamptz_mode = TimestamptzHandlingMode::from_options(&b.format_desc.options)?;
        let jsonb_handling_mode = JsonbHandlingMode::from_options(&b.format_desc.options)?;
        let encoder = JsonEncoder::new(
            b.schema,
            pk_indices,
            DateHandlingMode::FromCe,
            TimestampHandlingMode::Milli,
            timestamptz_mode,
            TimeHandlingMode::Milli,
            jsonb_handling_mode,
        );
        let encoder = if let Some(s) = b.format_desc.options.get("schemas.enable") {
            match s.to_lowercase().parse::<bool>() {
                Ok(true) => {
                    let kafka_connect = KafkaConnectParams {
                        schema_name: format!("{}.{}", b.db_name, b.sink_from_name),
                    };
                    encoder.with_kafka_connect(kafka_connect)
                }
                Ok(false) => encoder,
                _ => {
                    return Err(SinkError::Config(anyhow!(
                        "schemas.enable is expected to be `true` or `false`, got {s}",
                    )));
                }
            }
        } else {
            encoder
        };
        Ok(encoder)
    }
}

impl EncoderBuild for ProtoEncoder {
    async fn build(b: EncoderParams<'_>, pk_indices: Option<Vec<usize>>) -> Result<Self> {
        // TODO: better to be a compile-time assert
        assert!(pk_indices.is_none());
        // By passing `None` as `aws_auth_props`, reading from `s3://` not supported yet.
        let (descriptor, sid) =
            crate::schema::protobuf::fetch_descriptor(&b.format_desc.options, b.topic, None)
                .await
                .map_err(|e| SinkError::Config(anyhow!(e)))?;
        let header = match sid {
            None => ProtoHeader::None,
            Some(sid) => ProtoHeader::ConfluentSchemaRegistry(sid),
        };
        ProtoEncoder::new(b.schema, None, descriptor, header)
    }
}

fn ensure_only_one_pk<'a>(
    data_type_name: &'a str,
    params: &'a EncoderParams<'_>,
    pk_indices: &'a Option<Vec<usize>>,
) -> Result<(usize, &'a Field)> {
    let Some(pk_indices) = pk_indices else {
        return Err(SinkError::Config(anyhow!(
            "{}Encoder requires primary key columns to be specified",
            data_type_name
        )));
    };
    if pk_indices.len() != 1 {
        return Err(SinkError::Config(anyhow!(
            "KEY ENCODE {} expects only one primary key, but got {}",
            data_type_name,
            pk_indices.len(),
        )));
    }

    let schema_ref = params.schema.fields().get(pk_indices[0]).ok_or_else(|| {
        SinkError::Config(anyhow!(
            "The primary key column index {} is out of bounds in schema {:?}",
            pk_indices[0],
            params.schema
        ))
    })?;

    Ok((pk_indices[0], schema_ref))
}

impl EncoderBuild for BytesEncoder {
    async fn build(params: EncoderParams<'_>, pk_indices: Option<Vec<usize>>) -> Result<Self> {
        let (pk_index, schema_ref) = ensure_only_one_pk("BYTES", &params, &pk_indices)?;
        if let DataType::Bytea = schema_ref.data_type() {
            Ok(BytesEncoder::new(params.schema, pk_index))
        } else {
            Err(SinkError::Config(anyhow!(
                "The key encode is BYTES, but the primary key column {} has type {}",
                schema_ref.name,
                schema_ref.data_type
            )))
        }
    }
}

impl EncoderBuild for TextEncoder {
    async fn build(params: EncoderParams<'_>, pk_indices: Option<Vec<usize>>) -> Result<Self> {
        let (pk_index, schema_ref) = ensure_only_one_pk("TEXT", &params, &pk_indices)?;
        match &schema_ref.data_type() {
            DataType::Varchar
            | DataType::Boolean
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Int256
            | DataType::Serial => {}
            _ => {
                // why we don't allow float as text for key encode: https://github.com/risingwavelabs/risingwave/pull/16377#discussion_r1591864960
                return Err(SinkError::Config(anyhow!(
                    "The key encode is TEXT, but the primary key column {} has type {}. The key encode TEXT requires the primary key column to be of type varchar, bool, small int, int, big int, serial or rw_int256.",
                    schema_ref.name,
                    schema_ref.data_type
                )));
            }
        }

        Ok(Self::new(params.schema, pk_index))
    }
}

impl EncoderBuild for AvroEncoder {
    async fn build(b: EncoderParams<'_>, pk_indices: Option<Vec<usize>>) -> Result<Self> {
        use crate::schema::{SchemaLoader, SchemaVersion};

        let loader = SchemaLoader::from_format_options(b.topic, &b.format_desc.options)
            .await
            .map_err(|e| SinkError::Config(anyhow!(e)))?;

        let (schema_version, avro) = match pk_indices {
            Some(_) => loader
                .load_key_schema()
                .await
                .map_err(|e| SinkError::Config(anyhow!(e)))?,
            None => loader
                .load_val_schema()
                .await
                .map_err(|e| SinkError::Config(anyhow!(e)))?,
        };
        AvroEncoder::new(
            b.schema,
            pk_indices,
            std::sync::Arc::new(avro),
            match schema_version {
                SchemaVersion::Confluent(x) => AvroHeader::ConfluentSchemaRegistry(x),
                SchemaVersion::Glue(x) => AvroHeader::GlueSchemaRegistry(x),
            },
        )
    }
}

impl EncoderBuild for TemplateEncoder {
    async fn build(b: EncoderParams<'_>, pk_indices: Option<Vec<usize>>) -> Result<Self> {
        let redis_value_type = b
            .format_desc
            .options
            .get(REDIS_VALUE_TYPE)
            .map_or(REDIS_VALUE_TYPE_STRING, |s| s.as_str());
        match redis_value_type {
            REDIS_VALUE_TYPE_STRING => {
                let option_name = match pk_indices {
                    Some(_) => KEY_FORMAT,
                    None => VALUE_FORMAT,
                };
                let template = b.format_desc.options.get(option_name).ok_or_else(|| {
                    SinkError::Config(anyhow!("Cannot find '{option_name}',please set it."))
                })?;
                Ok(TemplateEncoder::new_string(
                    b.schema,
                    pk_indices,
                    template.clone(),
                ))
            }
            REDIS_VALUE_TYPE_GEO => match pk_indices {
                Some(_) => {
                    let member_name = b.format_desc.options.get(MEMBER_NAME).ok_or_else(|| {
                        SinkError::Config(anyhow!("Cannot find `{MEMBER_NAME}`,please set it."))
                    })?;
                    let template = b.format_desc.options.get(KEY_FORMAT).ok_or_else(|| {
                        SinkError::Config(anyhow!("Cannot find `{KEY_FORMAT}`,please set it."))
                    })?;
                    TemplateEncoder::new_geo_key(
                        b.schema,
                        pk_indices,
                        member_name,
                        template.clone(),
                    )
                }
                None => {
                    let lat_name = b.format_desc.options.get(LAT_NAME).ok_or_else(|| {
                        SinkError::Config(anyhow!("Cannot find `{LAT_NAME}`, please set it."))
                    })?;
                    let lon_name = b.format_desc.options.get(LON_NAME).ok_or_else(|| {
                        SinkError::Config(anyhow!("Cannot find `{LON_NAME}`,please set it."))
                    })?;
                    TemplateEncoder::new_geo_value(b.schema, pk_indices, lat_name, lon_name)
                }
            },
            REDIS_VALUE_TYPE_PUBSUB => match pk_indices {
                Some(_) => {
                    let channel = b.format_desc.options.get(CHANNEL).cloned();
                    let channel_column = b.format_desc.options.get(CHANNEL_COLUMN).cloned();
                    if (channel.is_none() && channel_column.is_none())
                        || (channel.is_some() && channel_column.is_some())
                    {
                        return Err(SinkError::Config(anyhow!(
                            "`{CHANNEL}` and `{CHANNEL_COLUMN}` only one can be set"
                        )));
                    }
                    TemplateEncoder::new_pubsub_key(b.schema, pk_indices, channel, channel_column)
                }
                None => {
                    let template = b.format_desc.options.get(VALUE_FORMAT).ok_or_else(|| {
                        SinkError::Config(anyhow!("Cannot find '{VALUE_FORMAT}',please set it."))
                    })?;
                    Ok(TemplateEncoder::new_string(
                        b.schema,
                        pk_indices,
                        template.clone(),
                    ))
                }
            },
            _ => Err(SinkError::Config(anyhow!(
                "The value type {} is not supported",
                redis_value_type
            ))),
        }
    }
}

struct FormatterParams<'a> {
    builder: EncoderParams<'a>,
    pk_indices: Vec<usize>,
}

/// Each formatter shall be able to be built from parameters.
///
/// This is not part of `SinkFormatter` trait, because that is about how a formatter completes its
/// own job as a self-contained unit, with a custom `new` asking for only necessary info; while this
/// one is about how different formatters can be selected from a common SQL interface.
trait FormatterBuild: Sized {
    async fn build(b: FormatterParams<'_>) -> Result<Self>;
}

impl<KE: EncoderBuild, VE: EncoderBuild> FormatterBuild for AppendOnlyFormatter<KE, VE> {
    async fn build(b: FormatterParams<'_>) -> Result<Self> {
        let key_encoder = match b.pk_indices.is_empty() {
            true => None,
            false => Some(KE::build(b.builder.clone(), Some(b.pk_indices)).await?),
        };
        let val_encoder = VE::build(b.builder, None).await?;
        Ok(AppendOnlyFormatter::new(key_encoder, val_encoder))
    }
}

impl<KE: EncoderBuild, VE: EncoderBuild> FormatterBuild for UpsertFormatter<KE, VE> {
    async fn build(b: FormatterParams<'_>) -> Result<Self> {
        let key_encoder = KE::build(b.builder.clone(), Some(b.pk_indices))
            .await
            .with_context(|| "Failed to build key encoder")?;
        let val_encoder = VE::build(b.builder, None)
            .await
            .with_context(|| "Failed to build value encoder")?;
        Ok(UpsertFormatter::new(key_encoder, val_encoder))
    }
}

impl FormatterBuild for DebeziumJsonFormatter {
    async fn build(b: FormatterParams<'_>) -> Result<Self> {
        assert_eq!(b.builder.format_desc.encode, SinkEncode::Json);

        Ok(DebeziumJsonFormatter::new(
            b.builder.schema,
            b.pk_indices,
            b.builder.db_name,
            b.builder.sink_from_name,
            DebeziumAdapterOpts::default(),
        ))
    }
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
        use {SinkEncode as E, SinkFormat as F, SinkFormatterImpl as Impl};
        let p = FormatterParams {
            builder: EncoderParams {
                format_desc,
                schema,
                db_name,
                sink_from_name,
                topic,
            },
            pk_indices,
        };

        // When defining `SinkFormatterImpl` we already linked each variant (eg `AppendOnlyJson`) to
        // an instantiation (eg `AppendOnlyFormatter<JsonEncoder, JsonEncoder>`) that implements the
        // trait `FormatterBuild`.
        //
        // Here we just need to match a `(format, encode)` to a variant, and rustc shall be able to
        // find the corresponding instantiation.

        // However,
        //   `Impl::AppendOnlyJson(FormatterBuild::build(p).await?)`
        // fails to be inferred without the following dummy wrapper.
        async fn build<T: FormatterBuild>(p: FormatterParams<'_>) -> Result<T> {
            T::build(p).await
        }

        Ok(
            match (
                &format_desc.format,
                &format_desc.encode,
                &format_desc.key_encode,
            ) {
                (F::AppendOnly, E::Json, Some(E::Text)) => {
                    Impl::AppendOnlyTextJson(build(p).await?)
                }
                (F::AppendOnly, E::Json, Some(E::Bytes)) => {
                    Impl::AppendOnlyBytesJson(build(p).await?)
                }
                (F::AppendOnly, E::Json, None) => Impl::AppendOnlyJson(build(p).await?),
                (F::AppendOnly, E::Avro, Some(E::Text)) => {
                    Impl::AppendOnlyTextAvro(build(p).await?)
                }
                (F::AppendOnly, E::Avro, Some(E::Bytes)) => {
                    Impl::AppendOnlyBytesAvro(build(p).await?)
                }
                (F::AppendOnly, E::Avro, None) => Impl::AppendOnlyAvro(build(p).await?),
                (F::AppendOnly, E::Protobuf, Some(E::Text)) => {
                    Impl::AppendOnlyTextProto(build(p).await?)
                }
                (F::AppendOnly, E::Protobuf, Some(E::Bytes)) => {
                    Impl::AppendOnlyBytesProto(build(p).await?)
                }
                (F::AppendOnly, E::Protobuf, None) => Impl::AppendOnlyProto(build(p).await?),
                (F::AppendOnly, E::Template, Some(E::Text)) => {
                    Impl::AppendOnlyTextTemplate(build(p).await?)
                }
                (F::AppendOnly, E::Template, Some(E::Bytes)) => {
                    Impl::AppendOnlyBytesTemplate(build(p).await?)
                }
                (F::AppendOnly, E::Template, None) => Impl::AppendOnlyTemplate(build(p).await?),
                (F::Upsert, E::Json, Some(E::Text)) => Impl::UpsertTextJson(build(p).await?),
                (F::Upsert, E::Json, Some(E::Bytes)) => {
                    Impl::UpsertBytesJson(build(p).await?)
                }
                (F::Upsert, E::Json, None) => Impl::UpsertJson(build(p).await?),
                (F::Upsert, E::Avro, Some(E::Text)) => Impl::UpsertTextAvro(build(p).await?),
                (F::Upsert, E::Avro, Some(E::Bytes)) => {
                    Impl::UpsertBytesAvro(build(p).await?)
                }
                (F::Upsert, E::Avro, None) => Impl::UpsertAvro(build(p).await?),
                (F::Upsert, E::Protobuf, Some(E::Text)) => Impl::UpsertTextProto(build(p).await?),
                (F::Upsert, E::Protobuf, Some(E::Bytes)) => {
                    Impl::UpsertBytesProto(build(p).await?)
                }
                (F::Upsert, E::Template, Some(E::Text)) => {
                    Impl::UpsertTextTemplate(build(p).await?)
                }
                (F::Upsert, E::Template, Some(E::Bytes)) => {
                    Impl::UpsertBytesTemplate(build(p).await?)
                }
                (F::Upsert, E::Template, None) => Impl::UpsertTemplate(build(p).await?),
                (F::Debezium, E::Json, None) => Impl::DebeziumJson(build(p).await?),
                (F::AppendOnly | F::Upsert, E::Text, _) => {
                    return Err(SinkError::Config(anyhow!(
                        "ENCODE TEXT is only valid as key encode."
                    )));
                }
                (F::AppendOnly, E::Avro, _)
                | (F::Upsert, E::Protobuf, _)
                | (F::Debezium, E::Json, Some(_))
                | (F::Debezium, E::Avro | E::Protobuf | E::Template | E::Text, _)
                | (F::AppendOnly, E::Bytes, _)
                | (F::Upsert, E::Bytes, _)
                | (F::Debezium, E::Bytes, _)
                | (_, E::Parquet, _)
                | (_, _, Some(E::Parquet))
                | (F::AppendOnly | F::Upsert, _, Some(E::Template) | Some(E::Json) | Some(E::Avro) | Some(E::Protobuf)) // reject other encode as key encode
                => {
                    return Err(SinkError::Config(anyhow!(
                        "sink format/encode/key_encode unsupported: {:?} {:?} {:?}",
                        format_desc.format,
                        format_desc.encode,
                        format_desc.key_encode
                    )));
                }
            },
        )
    }
}

/// Macro to dispatch formatting implementation for all supported sink formatter types.
/// Used when the message key can be either bytes or string.
///
/// Takes a formatter implementation ($impl), binds it to a name ($name),
/// and executes the provided code block ($body) with that binding.
#[macro_export]
macro_rules! dispatch_sink_formatter_impl {
    ($impl:expr, $name:ident, $body:expr) => {
        match $impl {
            SinkFormatterImpl::AppendOnlyJson($name) => $body,
            SinkFormatterImpl::AppendOnlyBytesJson($name) => $body,
            SinkFormatterImpl::AppendOnlyTextJson($name) => $body,
            SinkFormatterImpl::AppendOnlyAvro($name) => $body,
            SinkFormatterImpl::AppendOnlyTextAvro($name) => $body,
            SinkFormatterImpl::AppendOnlyBytesAvro($name) => $body,
            SinkFormatterImpl::AppendOnlyProto($name) => $body,
            SinkFormatterImpl::AppendOnlyTextProto($name) => $body,
            SinkFormatterImpl::AppendOnlyBytesProto($name) => $body,

            SinkFormatterImpl::UpsertJson($name) => $body,
            SinkFormatterImpl::UpsertBytesJson($name) => $body,
            SinkFormatterImpl::UpsertTextJson($name) => $body,
            SinkFormatterImpl::UpsertAvro($name) => $body,
            SinkFormatterImpl::UpsertTextAvro($name) => $body,
            SinkFormatterImpl::UpsertBytesAvro($name) => $body,
            SinkFormatterImpl::UpsertTextProto($name) => $body,
            SinkFormatterImpl::UpsertBytesProto($name) => $body,
            SinkFormatterImpl::DebeziumJson($name) => $body,
            SinkFormatterImpl::AppendOnlyTextTemplate($name) => $body,
            SinkFormatterImpl::AppendOnlyTemplate($name) => $body,
            SinkFormatterImpl::UpsertTextTemplate($name) => $body,
            SinkFormatterImpl::UpsertTemplate($name) => $body,
            SinkFormatterImpl::AppendOnlyBytesTemplate($name) => $body,
            SinkFormatterImpl::UpsertBytesTemplate($name) => $body,
        }
    };
}

/// Macro to dispatch formatting implementation for sink formatters that require string keys.
/// Used when the message key must be a string (excludes some Avro and bytes implementations).
///
/// Similar to `dispatch_sink_formatter_impl`, but excludes certain formatter types
/// that don't support string keys (e.g., `AppendOnlyAvro`, `UpsertAvro`).
/// These cases are marked as unreachable!() since they should never occur
/// in contexts requiring string keys.
#[macro_export]
macro_rules! dispatch_sink_formatter_str_key_impl {
    ($impl:expr, $name:ident, $body:expr) => {
        match $impl {
            SinkFormatterImpl::AppendOnlyJson($name) => $body,
            SinkFormatterImpl::AppendOnlyBytesJson(_) => unreachable!(),
            SinkFormatterImpl::AppendOnlyTextJson($name) => $body,
            SinkFormatterImpl::AppendOnlyAvro(_) => unreachable!(),
            SinkFormatterImpl::AppendOnlyTextAvro($name) => $body,
            SinkFormatterImpl::AppendOnlyBytesAvro(_) => unreachable!(),
            SinkFormatterImpl::AppendOnlyProto($name) => $body,
            SinkFormatterImpl::AppendOnlyTextProto($name) => $body,
            SinkFormatterImpl::AppendOnlyBytesProto(_) => unreachable!(),

            SinkFormatterImpl::UpsertJson($name) => $body,
            SinkFormatterImpl::UpsertTextJson($name) => $body,
            SinkFormatterImpl::UpsertAvro(_) => unreachable!(),
            SinkFormatterImpl::UpsertTextAvro($name) => $body,
            SinkFormatterImpl::UpsertBytesAvro(_) => unreachable!(),
            SinkFormatterImpl::UpsertTextProto($name) => $body,
            SinkFormatterImpl::UpsertBytesProto(_) => unreachable!(),
            SinkFormatterImpl::DebeziumJson($name) => $body,
            SinkFormatterImpl::AppendOnlyTextTemplate($name) => $body,
            SinkFormatterImpl::AppendOnlyTemplate($name) => $body,
            SinkFormatterImpl::UpsertTextTemplate($name) => $body,
            SinkFormatterImpl::UpsertBytesJson(_) => unreachable!(),
            SinkFormatterImpl::UpsertTemplate($name) => $body,
            SinkFormatterImpl::AppendOnlyBytesTemplate(_) => unreachable!(),
            SinkFormatterImpl::UpsertBytesTemplate(_) => unreachable!(),
        }
    };
}

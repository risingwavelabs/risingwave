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
use super::encoder::{
    DateHandlingMode, KafkaConnectParams, TimeHandlingMode, TimestamptzHandlingMode,
};
use super::redis::{KEY_FORMAT, VALUE_FORMAT};
use crate::sink::encoder::{
    AvroEncoder, AvroHeader, JsonEncoder, ProtoEncoder, ProtoHeader, TimestampHandlingMode,
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

struct Builder<'a> {
    format_desc: &'a SinkFormatDesc,
    schema: Schema,
    db_name: String,
    sink_from_name: String,
    topic: &'a str,
}
struct FormatterParams<'a> {
    builder: Builder<'a>,
    pk_indices: Vec<usize>,
}
trait FromBuilder: Sized {
    async fn from_builder(b: &Builder<'_>, pk_indices: Option<Vec<usize>>) -> Result<Self>;
}

impl FromBuilder for JsonEncoder {
    async fn from_builder(b: &Builder<'_>, pk_indices: Option<Vec<usize>>) -> Result<Self> {
        let timestamptz_mode = TimestamptzHandlingMode::from_options(&b.format_desc.options)?;
        let encoder = JsonEncoder::new(
            b.schema.clone(),
            pk_indices,
            DateHandlingMode::FromCe,
            TimestampHandlingMode::Milli,
            timestamptz_mode,
            TimeHandlingMode::Milli,
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
impl FromBuilder for ProtoEncoder {
    async fn from_builder(b: &Builder<'_>, pk_indices: Option<Vec<usize>>) -> Result<Self> {
        // TODO: const generic
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
        ProtoEncoder::new(b.schema.clone(), None, descriptor, header)
    }
}
impl FromBuilder for AvroEncoder {
    async fn from_builder(b: &Builder<'_>, pk_indices: Option<Vec<usize>>) -> Result<Self> {
        let loader =
            crate::schema::SchemaLoader::from_format_options(b.topic, &b.format_desc.options)
                .map_err(|e| SinkError::Config(anyhow!(e)))?;

        let (schema_id, avro) = match pk_indices {
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
            b.schema.clone(),
            pk_indices,
            std::sync::Arc::new(avro),
            AvroHeader::ConfluentSchemaRegistry(schema_id),
        )
    }
}
impl FromBuilder for TemplateEncoder {
    async fn from_builder(b: &Builder<'_>, pk_indices: Option<Vec<usize>>) -> Result<Self> {
        let option_name = match pk_indices {
            Some(_) => KEY_FORMAT,
            None => VALUE_FORMAT,
        };
        let template = b.format_desc.options.get(option_name).ok_or_else(|| {
            SinkError::Config(anyhow!(
                "Cannot find '{option_name}',please set it or use JSON"
            ))
        })?;
        Ok(TemplateEncoder::new(
            b.schema.clone(),
            pk_indices,
            template.clone(),
        ))
    }
}
trait FormatFromBuilder: Sized {
    async fn from_builder(b: FormatterParams<'_>) -> Result<Self>;
}

impl<KE: FromBuilder, VE: FromBuilder> FormatFromBuilder for AppendOnlyFormatter<KE, VE> {
    async fn from_builder(b: FormatterParams<'_>) -> Result<Self> {
        let key_encoder = match b.pk_indices.is_empty() {
            true => None,
            false => Some(KE::from_builder(&b.builder, Some(b.pk_indices)).await?),
        };
        let val_encoder = VE::from_builder(&b.builder, None).await?;
        Ok(AppendOnlyFormatter::new(key_encoder, val_encoder))
    }
}
impl<KE: FromBuilder, VE: FromBuilder> FormatFromBuilder for UpsertFormatter<KE, VE> {
    async fn from_builder(b: FormatterParams<'_>) -> Result<Self> {
        let key_encoder = KE::from_builder(&b.builder, Some(b.pk_indices)).await?;
        let val_encoder = VE::from_builder(&b.builder, None).await?;
        Ok(UpsertFormatter::new(key_encoder, val_encoder))
    }
}
impl FormatFromBuilder for DebeziumJsonFormatter {
    async fn from_builder(b: FormatterParams<'_>) -> Result<Self> {
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
async fn build<T, F>(f: F, p: FormatterParams<'_>) -> Result<SinkFormatterImpl>
where
    T: FormatFromBuilder,
    F: FnOnce(T) -> SinkFormatterImpl,
{
    T::from_builder(p).await.map(f)
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
            builder: Builder {
                format_desc,
                schema,
                db_name,
                sink_from_name,
                topic,
            },
            pk_indices,
        };

        match (&format_desc.format, &format_desc.encode) {
            (F::AppendOnly, E::Json) => build(Impl::AppendOnlyJson, p).await,
            (F::AppendOnly, E::Protobuf) => build(Impl::AppendOnlyProto, p).await,
            (F::AppendOnly, E::Template) => build(Impl::AppendOnlyTemplate, p).await,
            (F::Upsert, E::Json) => build(Impl::UpsertJson, p).await,
            (F::Upsert, E::Avro) => build(Impl::UpsertAvro, p).await,
            (F::Upsert, E::Template) => build(Impl::UpsertTemplate, p).await,
            (F::Debezium, E::Json) => build(Impl::DebeziumJson, p).await,
            (F::AppendOnly, E::Avro)
            | (F::Upsert, E::Protobuf)
            | (F::Debezium, E::Avro | E::Protobuf | E::Template) => {
                Err(SinkError::Config(anyhow!(
                    "sink format/encode unsupported: {:?} {:?}",
                    format_desc.format,
                    format_desc.encode,
                )))
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

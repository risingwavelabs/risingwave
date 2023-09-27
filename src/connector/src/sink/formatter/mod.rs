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

use crate::sink::{Result, SinkError, SINK_TYPE_DEBEZIUM, SINK_TYPE_UPSERT};

mod append_only;
mod debezium_json;
mod upsert;

pub use append_only::AppendOnlyFormatter;
pub use debezium_json::{DebeziumAdapterOpts, DebeziumJsonFormatter};
use risingwave_common::catalog::Schema;
pub use upsert::UpsertFormatter;

use super::encoder::template::{RedisFormatEncoder, TemplateEncoder};
use crate::sink::encoder::{JsonEncoder, TimestampHandlingMode};

/// Transforms a `StreamChunk` into a sequence of key-value pairs according a specific format,
/// for example append-only, upsert or debezium.
pub trait SinkFormatter {
    type K;
    type V;

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

#[expect(clippy::enum_variant_names)]
pub enum SinkFormatterImpl {
    AppendOnlyJson(AppendOnlyFormatter<JsonEncoder, JsonEncoder>),
    UpsertJson(UpsertFormatter<JsonEncoder, JsonEncoder>),
    DebeziumJson(DebeziumJsonFormatter),
    RedisAppendOnly(AppendOnlyFormatter<RedisFormatEncoder, RedisFormatEncoder>),
    RedisUpsert(UpsertFormatter<RedisFormatEncoder, RedisFormatEncoder>),
}

impl SinkFormatterImpl {
    pub fn new(
        formatter_type: &str,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
        db_name: String,
        sink_from_name: String,
    ) -> Result<Self> {
        if is_append_only {
            let key_encoder = JsonEncoder::new(
                schema.clone(),
                Some(pk_indices),
                TimestampHandlingMode::Milli,
            );
            let val_encoder = JsonEncoder::new(schema, None, TimestampHandlingMode::Milli);

            let formatter = AppendOnlyFormatter::new(key_encoder, val_encoder);
            Ok(SinkFormatterImpl::AppendOnlyJson(formatter))
        } else if formatter_type == SINK_TYPE_DEBEZIUM {
            Ok(SinkFormatterImpl::DebeziumJson(DebeziumJsonFormatter::new(
                schema,
                pk_indices,
                db_name,
                sink_from_name,
                DebeziumAdapterOpts::default(),
            )))
        } else if formatter_type == SINK_TYPE_UPSERT {
            let key_encoder = JsonEncoder::new(
                schema.clone(),
                Some(pk_indices),
                TimestampHandlingMode::Milli,
            );
            let val_encoder = JsonEncoder::new(schema, None, TimestampHandlingMode::Milli);

            // Initialize the upsert_stream
            let formatter = UpsertFormatter::new(key_encoder, val_encoder);
            Ok(SinkFormatterImpl::UpsertJson(formatter))
        } else {
            Err(SinkError::Config(anyhow!(
                "unsupported upsert sink type {}",
                formatter_type
            )))
        }
    }

    pub fn new_with_redis(
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
        key_format: Option<String>,
        value_format: Option<String>,
    ) -> Result<Self> {
        let (key_encoder, val_encoder) = match (key_format, value_format) {
            (Some(k), Some(v)) => {
                let key_encoder = RedisFormatEncoder::Template(TemplateEncoder::new(
                    schema.clone(),
                    Some(pk_indices),
                    k,
                ));
                let val_encoder =
                    RedisFormatEncoder::Template(TemplateEncoder::new(schema, None, v));
                (key_encoder, val_encoder)
            }
            (None, None) => {
                let key_encoder = RedisFormatEncoder::Json(JsonEncoder::new(
                    schema.clone(),
                    Some(pk_indices),
                    TimestampHandlingMode::Milli,
                ));
                let val_encoder = RedisFormatEncoder::Json(JsonEncoder::new(
                    schema,
                    None,
                    TimestampHandlingMode::Milli,
                ));
                (key_encoder, val_encoder)
            }
            (None, Some(v)) => {
                let key_encoder = RedisFormatEncoder::Json(JsonEncoder::new(
                    schema.clone(),
                    Some(pk_indices),
                    TimestampHandlingMode::Milli,
                ));
                let val_encoder =
                    RedisFormatEncoder::Template(TemplateEncoder::new(schema, None, v));
                (key_encoder, val_encoder)
            }
            (Some(k), None) => {
                let key_encoder = RedisFormatEncoder::Template(TemplateEncoder::new(
                    schema.clone(),
                    Some(pk_indices),
                    k,
                ));
                let val_encoder = RedisFormatEncoder::Json(JsonEncoder::new(
                    schema,
                    None,
                    TimestampHandlingMode::Milli,
                ));
                (key_encoder, val_encoder)
            }
        };
        if is_append_only {
            let formatter = AppendOnlyFormatter::new(key_encoder, val_encoder);
            Ok(SinkFormatterImpl::RedisAppendOnly(formatter))
        } else {
            let formatter = UpsertFormatter::new(key_encoder, val_encoder);
            Ok(SinkFormatterImpl::RedisUpsert(formatter))
        }
    }
}

#[macro_export]
macro_rules! dispatch_sink_formatter_impl {
    ($impl:expr, $name:ident, $body:expr) => {
        match $impl {
            SinkFormatterImpl::AppendOnlyJson($name) => $body,
            SinkFormatterImpl::UpsertJson($name) => $body,
            SinkFormatterImpl::DebeziumJson($name) => $body,
            SinkFormatterImpl::RedisAppendOnly($name) => $body,
            SinkFormatterImpl::RedisUpsert($name) => $body,
        }
    };
}

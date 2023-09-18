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

use futures_async_stream::try_stream;
use risingwave_common::array::stream_chunk::Op;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::{Field, Schema};
use serde_json::{json, Map, Value};
use tracing::warn;

use super::encoder::{JsonEncoder, RowEncoder, TimestampHandlingMode};
use crate::sink::{Result, SinkError};

const DEBEZIUM_NAME_FIELD_PREFIX: &str = "RisingWave";

pub struct DebeziumAdapterOpts {
    gen_tombstone: bool,
}

impl Default for DebeziumAdapterOpts {
    fn default() -> Self {
        Self {
            gen_tombstone: true,
        }
    }
}

fn concat_debezium_name_field(db_name: &str, sink_from_name: &str, value: &str) -> String {
    DEBEZIUM_NAME_FIELD_PREFIX.to_owned() + "." + db_name + "." + sink_from_name + "." + value
}

#[try_stream(ok = (Option<Value>, Option<Value>), error = SinkError)]
pub async fn gen_debezium_message_stream<'a>(
    schema: &'a Schema,
    pk_indices: &'a [usize],
    chunk: StreamChunk,
    ts_ms: u64,
    opts: DebeziumAdapterOpts,
    db_name: &'a str,
    sink_from_name: &'a str,
) {
    let source_field = json!({
        "db": db_name,
        "table": sink_from_name,
    });

    let mut update_cache: Option<Map<String, Value>> = None;

    let key_encoder = JsonEncoder::new(schema, Some(pk_indices), TimestampHandlingMode::Milli);
    let val_encoder = JsonEncoder::new(schema, None, TimestampHandlingMode::Milli);

    for (op, row) in chunk.rows() {
        let event_key_object: Option<Value> = Some(json!({
            "schema": json!({
                "type": "struct",
                "fields": fields_pk_to_json(&schema.fields, pk_indices),
                "optional": false,
                "name": concat_debezium_name_field(db_name, sink_from_name, "Key"),
            }),
            "payload": key_encoder.encode(row)?,
        }));
        let event_object: Option<Value> = match op {
            Op::Insert => Some(json!({
                "schema": schema_to_json(schema, db_name, sink_from_name),
                "payload": {
                    "before": null,
                    "after": val_encoder.encode(row)?,
                    "op": "c",
                    "ts_ms": ts_ms,
                    "source": source_field,
                }
            })),
            Op::Delete => {
                let value_obj = Some(json!({
                    "schema": schema_to_json(schema, db_name, sink_from_name),
                    "payload": {
                        "before": val_encoder.encode(row)?,
                        "after": null,
                        "op": "d",
                        "ts_ms": ts_ms,
                        "source": source_field,
                    }
                }));
                yield (event_key_object.clone(), value_obj);

                if opts.gen_tombstone {
                    // Tomestone event
                    // https://debezium.io/documentation/reference/2.1/connectors/postgresql.html#postgresql-delete-events
                    yield (event_key_object, None);
                }

                continue;
            }
            Op::UpdateDelete => {
                update_cache = Some(val_encoder.encode(row)?);
                continue;
            }
            Op::UpdateInsert => {
                if let Some(before) = update_cache.take() {
                    Some(json!({
                        "schema": schema_to_json(schema, db_name, sink_from_name),
                        "payload": {
                            "before": before,
                            "after": val_encoder.encode(row)?,
                            "op": "u",
                            "ts_ms": ts_ms,
                            "source": source_field,
                        }
                    }))
                } else {
                    warn!(
                        "not found UpdateDelete in prev row, skipping, row index {:?}",
                        row.index()
                    );
                    continue;
                }
            }
        };
        yield (event_key_object, event_object);
    }
}

pub(crate) fn schema_to_json(schema: &Schema, db_name: &str, sink_from_name: &str) -> Value {
    let mut schema_fields = Vec::new();
    schema_fields.push(json!({
        "type": "struct",
        "fields": fields_to_json(&schema.fields),
        "optional": true,
        "field": "before",
        "name": concat_debezium_name_field(db_name, sink_from_name, "Key"),
    }));
    schema_fields.push(json!({
        "type": "struct",
        "fields": fields_to_json(&schema.fields),
        "optional": true,
        "field": "after",
        "name": concat_debezium_name_field(db_name, sink_from_name, "Key"),
    }));

    schema_fields.push(json!({
        "type": "struct",
        "optional": false,
        "name": concat_debezium_name_field(db_name, sink_from_name, "Source"),
        "fields": vec![
            json!({
                "type": "string",
                "optional": false,
                "field": "db"
            }),
            json!({
                "type": "string",
                "optional": true,
                "field": "table"
            })],
        "field": "source"
    }));
    schema_fields.push(json!({
        "type": "string",
        "optional": false,
        "field": "op"
    }));
    schema_fields.push(json!({
        "type": "int64",
        "optional": false,
        "field": "ts_ms"
    }));

    json!({
        "type": "struct",
        "fields": schema_fields,
        "optional": false,
        "name": concat_debezium_name_field(db_name, sink_from_name, "Envelope"),
    })
}

pub(crate) fn fields_pk_to_json(fields: &[Field], pk_indices: &[usize]) -> Value {
    let mut res = Vec::new();
    for idx in pk_indices {
        res.push(field_to_json(&fields[*idx]));
    }
    json!(res)
}

pub(crate) fn fields_to_json(fields: &[Field]) -> Value {
    let mut res = Vec::new();

    fields
        .iter()
        .for_each(|field| res.push(field_to_json(field)));

    json!(res)
}

pub(crate) fn field_to_json(field: &Field) -> Value {
    // mapping from 'https://debezium.io/documentation/reference/2.1/connectors/postgresql.html#postgresql-data-types'
    let r#type = match field.data_type() {
        risingwave_common::types::DataType::Boolean => "boolean",
        risingwave_common::types::DataType::Int16 => "int16",
        risingwave_common::types::DataType::Int32 => "int32",
        risingwave_common::types::DataType::Int64 => "int64",
        risingwave_common::types::DataType::Int256 => "string",
        risingwave_common::types::DataType::Float32 => "float",
        risingwave_common::types::DataType::Float64 => "double",
        // currently, we only support handling decimal as string.
        // https://debezium.io/documentation/reference/2.1/connectors/postgresql.html#postgresql-decimal-types
        risingwave_common::types::DataType::Decimal => "string",

        risingwave_common::types::DataType::Varchar => "string",

        risingwave_common::types::DataType::Date => "int32",
        risingwave_common::types::DataType::Time => "int64",
        risingwave_common::types::DataType::Timestamp => "int64",
        risingwave_common::types::DataType::Timestamptz => "string",
        risingwave_common::types::DataType::Interval => "string",

        risingwave_common::types::DataType::Bytea => "bytes",
        risingwave_common::types::DataType::Jsonb => "string",
        risingwave_common::types::DataType::Serial => "int32",
        // since the original debezium pg support HSTORE via encoded as json string by default,
        // we do the same here
        risingwave_common::types::DataType::Struct(_) => "string",
        risingwave_common::types::DataType::List { .. } => "string",
    };
    json!({
        "field": field.name,
        "optional": true,
        "type": r#type,
    })
}

pub fn chunk_to_json(chunk: StreamChunk, schema: &Schema) -> Result<Vec<String>> {
    let encoder = JsonEncoder::new(schema, None, TimestampHandlingMode::Milli);
    let mut records: Vec<String> = Vec::with_capacity(chunk.capacity());
    for (_, row) in chunk.rows() {
        let record = Value::Object(encoder.encode(row)?);
        records.push(record.to_string());
    }

    Ok(records)
}

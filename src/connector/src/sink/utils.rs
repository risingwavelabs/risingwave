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

use base64::engine::general_purpose;
use base64::Engine as _;
use chrono::{Datelike, Timelike};
use futures_async_stream::try_stream;
use risingwave_common::array::stream_chunk::Op;
use risingwave_common::array::{ArrayError, ArrayResult, RowRef, StreamChunk};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, DatumRef, ScalarRefImpl, ToText};
use risingwave_common::util::iter_util::{ZipEqDebug, ZipEqFast};
use serde_json::{json, Map, Value};
use tracing::warn;

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

    for (op, row) in chunk.rows() {
        let event_key_object: Option<Value> = Some(json!({
            "schema": json!({
                "type": "struct",
                "fields": fields_pk_to_json(&schema.fields, pk_indices),
                "optional": false,
                "name": concat_debezium_name_field(db_name, sink_from_name, "Key"),
            }),
            "payload": pk_to_json(row, &schema.fields, pk_indices)?,
        }));
        let event_object: Option<Value> = match op {
            Op::Insert => Some(json!({
                "schema": schema_to_json(schema, db_name, sink_from_name),
                "payload": {
                    "before": null,
                    "after": record_to_json(row, &schema.fields, TimestampHandlingMode::Milli)?,
                    "op": "c",
                    "ts_ms": ts_ms,
                    "source": source_field,
                }
            })),
            Op::Delete => {
                let value_obj = Some(json!({
                    "schema": schema_to_json(schema, db_name, sink_from_name),
                    "payload": {
                        "before": record_to_json(row, &schema.fields, TimestampHandlingMode::Milli)?,
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
                update_cache = Some(record_to_json(
                    row,
                    &schema.fields,
                    TimestampHandlingMode::Milli,
                )?);
                continue;
            }
            Op::UpdateInsert => {
                if let Some(before) = update_cache.take() {
                    Some(json!({
                        "schema": schema_to_json(schema, db_name, sink_from_name),
                        "payload": {
                            "before": before,
                            "after": record_to_json(row, &schema.fields, TimestampHandlingMode::Milli)?,
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

pub(crate) fn pk_to_json(
    row: RowRef<'_>,
    schema: &[Field],
    pk_indices: &[usize],
) -> Result<Map<String, Value>> {
    let mut mappings = Map::with_capacity(schema.len());
    for idx in pk_indices {
        let field = &schema[*idx];
        let key = field.name.clone();
        let value = datum_to_json_object(field, row.datum_at(*idx), TimestampHandlingMode::Milli)
            .map_err(|e| SinkError::JsonParse(e.to_string()))?;
        mappings.insert(key, value);
    }
    Ok(mappings)
}

pub fn chunk_to_json(chunk: StreamChunk, schema: &Schema) -> Result<Vec<String>> {
    let mut records: Vec<String> = Vec::with_capacity(chunk.capacity());
    for (_, row) in chunk.rows() {
        let record = Value::Object(record_to_json(
            row,
            &schema.fields,
            TimestampHandlingMode::Milli,
        )?);
        records.push(record.to_string());
    }

    Ok(records)
}

#[derive(Clone, Copy)]
pub enum TimestampHandlingMode {
    Milli,
    String,
}

pub fn record_to_json(
    row: RowRef<'_>,
    schema: &[Field],
    timestamp_handling_mode: TimestampHandlingMode,
) -> Result<Map<String, Value>> {
    let mut mappings = Map::with_capacity(schema.len());
    for (field, datum_ref) in schema.iter().zip_eq_fast(row.iter()) {
        let key = field.name.clone();
        let value = datum_to_json_object(field, datum_ref, timestamp_handling_mode)
            .map_err(|e| SinkError::JsonParse(e.to_string()))?;
        mappings.insert(key, value);
    }
    Ok(mappings)
}

fn datum_to_json_object(
    field: &Field,
    datum: DatumRef<'_>,
    timestamp_handling_mode: TimestampHandlingMode,
) -> ArrayResult<Value> {
    let scalar_ref = match datum {
        None => return Ok(Value::Null),
        Some(datum) => datum,
    };

    let data_type = field.data_type();

    tracing::debug!("datum_to_json_object: {:?}, {:?}", data_type, scalar_ref);

    let value = match (data_type, scalar_ref) {
        (DataType::Boolean, ScalarRefImpl::Bool(v)) => {
            json!(v)
        }
        (DataType::Int16, ScalarRefImpl::Int16(v)) => {
            json!(v)
        }
        (DataType::Int32, ScalarRefImpl::Int32(v)) => {
            json!(v)
        }
        (DataType::Int64, ScalarRefImpl::Int64(v)) => {
            json!(v)
        }
        (DataType::Float32, ScalarRefImpl::Float32(v)) => {
            json!(f32::from(v))
        }
        (DataType::Float64, ScalarRefImpl::Float64(v)) => {
            json!(f64::from(v))
        }
        (DataType::Varchar, ScalarRefImpl::Utf8(v)) => {
            json!(v)
        }
        (DataType::Decimal, ScalarRefImpl::Decimal(v)) => {
            json!(v.to_text())
        }
        (DataType::Timestamptz, ScalarRefImpl::Timestamptz(v)) => {
            // risingwave's timestamp with timezone is stored in UTC and does not maintain the
            // timezone info and the time is in microsecond.
            let parsed = v.to_datetime_utc().naive_utc();
            let v = parsed.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
            json!(v)
        }
        (DataType::Time, ScalarRefImpl::Time(v)) => {
            // todo: just ignore the nanos part to avoid leap second complex
            json!(v.0.num_seconds_from_midnight() as i64 * 1000)
        }
        (DataType::Date, ScalarRefImpl::Date(v)) => {
            json!(v.0.num_days_from_ce())
        }
        (DataType::Timestamp, ScalarRefImpl::Timestamp(v)) => match timestamp_handling_mode {
            TimestampHandlingMode::Milli => json!(v.0.timestamp_millis()),
            TimestampHandlingMode::String => json!(v.0.format("%Y-%m-%d %H:%M:%S%.6f").to_string()),
        },
        (DataType::Bytea, ScalarRefImpl::Bytea(v)) => {
            json!(general_purpose::STANDARD_NO_PAD.encode(v))
        }
        // P<years>Y<months>M<days>DT<hours>H<minutes>M<seconds>S
        (DataType::Interval, ScalarRefImpl::Interval(v)) => {
            json!(v.as_iso_8601())
        }
        (DataType::Jsonb, ScalarRefImpl::Jsonb(jsonb_ref)) => {
            json!(jsonb_ref.to_string())
        }
        (DataType::List(datatype), ScalarRefImpl::List(list_ref)) => {
            let elems = list_ref.iter();
            let mut vec = Vec::with_capacity(elems.len());
            let inner_field = Field::unnamed(Box::<DataType>::into_inner(datatype));
            for sub_datum_ref in elems {
                let value =
                    datum_to_json_object(&inner_field, sub_datum_ref, timestamp_handling_mode)?;
                vec.push(value);
            }
            json!(vec)
        }
        (DataType::Struct(st), ScalarRefImpl::Struct(struct_ref)) => {
            let mut map = Map::with_capacity(st.len());
            for (sub_datum_ref, sub_field) in struct_ref.iter_fields_ref().zip_eq_debug(
                st.iter()
                    .map(|(name, dt)| Field::with_name(dt.clone(), name)),
            ) {
                let value =
                    datum_to_json_object(&sub_field, sub_datum_ref, timestamp_handling_mode)?;
                map.insert(sub_field.name.clone(), value);
            }
            json!(map)
        }
        (data_type, scalar_ref) => {
            return Err(ArrayError::internal(
                format!("datum_to_json_object: unsupported data type: field name: {:?}, logical type: {:?}, physical type: {:?}", field.name, data_type, scalar_ref),
            ));
        }
    };

    Ok(value)
}

#[derive(Debug, Clone, Default)]
pub struct UpsertAdapterOpts {}

fn gen_event_object(
    schema: &Schema,
    object: Value,
    enable_schema: bool,
    name: &Option<String>,
) -> Option<Value> {
    if enable_schema {
        Some(json!({
            "schema": generate_json_converter_schema(&schema.fields, name.as_ref().unwrap()),
            "payload": object,
        }))
    } else {
        Some(object)
    }
}

#[try_stream(ok = (Option<Value>, Option<Value>), error = SinkError)]
pub async fn gen_upsert_message_stream<'a>(
    schema: &'a Schema,
    pk_indices: &'a [usize],
    chunk: StreamChunk,
    enable_schema: bool,
    schema_name: Option<String>,
    _opts: UpsertAdapterOpts,
) {
    for (op, row) in chunk.rows() {
        let event_key_object_inner = Value::Object(pk_to_json(row, &schema.fields, pk_indices)?);
        let event_key_object = if enable_schema {
            Some(json!({
                "schema": generate_json_converter_schema_with_indices(&schema.fields, schema_name.as_ref().unwrap(), pk_indices),
                "payload": event_key_object_inner,
            }))
        } else {
            Some(event_key_object_inner)
        };

        let event_object = match op {
            Op::Insert => gen_event_object(
                schema,
                Value::Object(record_to_json(
                    row,
                    &schema.fields,
                    TimestampHandlingMode::Milli,
                )?),
                enable_schema,
                &schema_name,
            ),
            Op::Delete => Some(Value::Null),
            Op::UpdateDelete => {
                // upsert semantic does not require update delete event
                continue;
            }
            Op::UpdateInsert => gen_event_object(
                schema,
                Value::Object(record_to_json(
                    row,
                    &schema.fields,
                    TimestampHandlingMode::Milli,
                )?),
                enable_schema,
                &schema_name,
            ),
        };

        yield (event_key_object, event_object);
    }
}

#[derive(Debug, Clone, Default)]
pub struct AppendOnlyAdapterOpts {}

#[try_stream(ok = (Option<Value>, Option<Value>), error = SinkError)]
pub async fn gen_append_only_message_stream<'a>(
    schema: &'a Schema,
    pk_indices: &'a [usize],
    chunk: StreamChunk,
    _opts: AppendOnlyAdapterOpts,
) {
    for (op, row) in chunk.rows() {
        if op != Op::Insert {
            continue;
        }
        let event_key_object = Some(Value::Object(pk_to_json(row, &schema.fields, pk_indices)?));
        let event_object = Some(Value::Object(record_to_json(
            row,
            &schema.fields,
            TimestampHandlingMode::Milli,
        )?));

        yield (event_key_object, event_object);
    }
}

// reference: https://github.com/apache/kafka/blob/80982c4ae3fe6be127b48ec09caff11ab5f87c69/connect/json/src/main/java/org/apache/kafka/connect/json/JsonSchema.java#L39
fn json_converter_field_to_json(field: &Field) -> Value {
    let mut mapping = Map::with_capacity(4);
    let type_mapping = |rw_type: &DataType| match rw_type {
        DataType::Boolean => "boolean",
        DataType::Int16 => "int16",
        DataType::Int32 => "int32",
        DataType::Int64 => "int64",
        DataType::Float32 => "float",
        DataType::Float64 => "double",
        DataType::Decimal => "string",
        DataType::Date => "int32",
        DataType::Varchar => "string",
        DataType::Time => "int64",
        DataType::Timestamp => "int64",
        DataType::Timestamptz => "string",
        DataType::Interval => "string",
        DataType::Struct(_) => "struct",
        DataType::List(_) => "array",
        DataType::Bytea => "bytes",
        DataType::Jsonb => "string",
        DataType::Serial => "int32",
        DataType::Int256 => "string",
    };
    mapping.insert("type".into(), json!(type_mapping(&field.data_type)));
    mapping.insert("optional".into(), json!("true"));
    mapping.insert("field".into(), json!(field.name));
    match &field.data_type {
        DataType::Struct(_) => {
            let mut sub_fields = Vec::new();
            for sub_field in &field.sub_fields {
                sub_fields.push(json_converter_field_to_json(sub_field));
            }
            mapping.insert("fields".into(), json!(sub_fields));
        }
        DataType::List(list_type) => {
            mapping.insert(
                "items".into(),
                json!({
                    "type": type_mapping(list_type),
                }),
            );
        }
        _ => {}
    }
    json!(mapping)
}

/// Generate schema for Kafka's `JsonConverter` when `schema.enable` is true
fn generate_json_converter_schema(fields: &[Field], name: &str) -> Value {
    json!({
        "type": "struct",
        "fields": fields.iter().map(json_converter_field_to_json).collect::<Vec<_>>(),
        "optional": "false",
        "name": name,
    })
}

/// Generate schema for Kafka's `JsonConverter` when `schema.enable` is true according
/// to indices
fn generate_json_converter_schema_with_indices(
    fields: &[Field],
    name: &str,
    indices: &[usize],
) -> Value {
    json!({
        "type": "struct",
        "fields": indices.iter().map(|i| json_converter_field_to_json(&fields[*i])).collect::<Vec<_>>(),
        "optional": "false",
        "name": name,
    })
}

#[cfg(test)]
mod tests {

    use risingwave_common::types::{DataType, Interval, ScalarImpl, StructType, Time, Timestamp};

    use super::*;
    #[test]
    fn test_to_json_basic_type() {
        let mock_field = Field {
            data_type: DataType::Boolean,
            name: Default::default(),
            sub_fields: Default::default(),
            type_name: Default::default(),
        };
        let boolean_value = datum_to_json_object(
            &Field {
                data_type: DataType::Boolean,
                ..mock_field.clone()
            },
            Some(ScalarImpl::Bool(false).as_scalar_ref_impl()),
            TimestampHandlingMode::String,
        )
        .unwrap();
        assert_eq!(boolean_value, json!(false));

        let int16_value = datum_to_json_object(
            &Field {
                data_type: DataType::Int16,
                ..mock_field.clone()
            },
            Some(ScalarImpl::Int16(16).as_scalar_ref_impl()),
            TimestampHandlingMode::String,
        )
        .unwrap();
        assert_eq!(int16_value, json!(16));

        let int64_value = datum_to_json_object(
            &Field {
                data_type: DataType::Int64,
                ..mock_field.clone()
            },
            Some(ScalarImpl::Int64(std::i64::MAX).as_scalar_ref_impl()),
            TimestampHandlingMode::String,
        )
        .unwrap();
        assert_eq!(
            serde_json::to_string(&int64_value).unwrap(),
            std::i64::MAX.to_string()
        );

        // https://github.com/debezium/debezium/blob/main/debezium-core/src/main/java/io/debezium/time/ZonedTimestamp.java
        let tstz_inner = "2018-01-26T18:30:09.453Z".parse().unwrap();
        let tstz_value = datum_to_json_object(
            &Field {
                data_type: DataType::Timestamptz,
                ..mock_field.clone()
            },
            Some(ScalarImpl::Timestamptz(tstz_inner).as_scalar_ref_impl()),
            TimestampHandlingMode::String,
        )
        .unwrap();
        assert_eq!(tstz_value, "2018-01-26 18:30:09.453000");

        let ts_value = datum_to_json_object(
            &Field {
                data_type: DataType::Timestamp,
                ..mock_field.clone()
            },
            Some(
                ScalarImpl::Timestamp(Timestamp::from_timestamp_uncheck(1000, 0))
                    .as_scalar_ref_impl(),
            ),
            TimestampHandlingMode::Milli,
        )
        .unwrap();
        assert_eq!(ts_value, json!(1000 * 1000));

        let ts_value = datum_to_json_object(
            &Field {
                data_type: DataType::Timestamp,
                ..mock_field.clone()
            },
            Some(
                ScalarImpl::Timestamp(Timestamp::from_timestamp_uncheck(1000, 0))
                    .as_scalar_ref_impl(),
            ),
            TimestampHandlingMode::String,
        )
        .unwrap();
        assert_eq!(ts_value, json!("1970-01-01 00:16:40.000000".to_string()));

        // Represents the number of microseconds past midnigh, io.debezium.time.Time
        let time_value = datum_to_json_object(
            &Field {
                data_type: DataType::Time,
                ..mock_field.clone()
            },
            Some(
                ScalarImpl::Time(Time::from_num_seconds_from_midnight_uncheck(1000, 0))
                    .as_scalar_ref_impl(),
            ),
            TimestampHandlingMode::String,
        )
        .unwrap();
        assert_eq!(time_value, json!(1000 * 1000));

        let interval_value = datum_to_json_object(
            &Field {
                data_type: DataType::Interval,
                ..mock_field
            },
            Some(
                ScalarImpl::Interval(Interval::from_month_day_usec(13, 2, 1000000))
                    .as_scalar_ref_impl(),
            ),
            TimestampHandlingMode::String,
        )
        .unwrap();
        assert_eq!(interval_value, json!("P1Y1M2DT0H0M1S"));
    }

    #[test]
    fn test_generate_json_converter_schema() {
        let mock_field = Field {
            data_type: DataType::Boolean,
            name: Default::default(),
            sub_fields: Default::default(),
            type_name: Default::default(),
        };
        let fields = vec![
            Field {
                data_type: DataType::Boolean,
                name: "v1".into(),
                ..mock_field.clone()
            },
            Field {
                data_type: DataType::Int16,
                name: "v2".into(),
                ..mock_field.clone()
            },
            Field {
                data_type: DataType::Int32,
                name: "v3".into(),
                ..mock_field.clone()
            },
            Field {
                data_type: DataType::Float32,
                name: "v4".into(),
                ..mock_field.clone()
            },
            Field {
                data_type: DataType::Decimal,
                name: "v5".into(),
                ..mock_field.clone()
            },
            Field {
                data_type: DataType::Date,
                name: "v6".into(),
                ..mock_field.clone()
            },
            Field {
                data_type: DataType::Varchar,
                name: "v7".into(),
                ..mock_field.clone()
            },
            Field {
                data_type: DataType::Time,
                name: "v8".into(),
                ..mock_field.clone()
            },
            Field {
                data_type: DataType::Interval,
                name: "v9".into(),
                ..mock_field.clone()
            },
            Field {
                data_type: DataType::Struct(StructType::new(vec![
                    ("a", DataType::Timestamp),
                    ("b", DataType::Timestamptz),
                    (
                        "c",
                        DataType::Struct(StructType::new(vec![
                            ("aa", DataType::Int64),
                            ("bb", DataType::Float64),
                        ])),
                    ),
                ])),
                name: "v10".into(),
                sub_fields: vec![
                    Field {
                        data_type: DataType::Timestamp,
                        name: "a".into(),
                        ..mock_field.clone()
                    },
                    Field {
                        data_type: DataType::Timestamptz,
                        name: "b".into(),
                        ..mock_field.clone()
                    },
                    Field {
                        data_type: DataType::Struct(StructType::new(vec![
                            ("aa", DataType::Int64),
                            ("bb", DataType::Float64),
                        ])),
                        name: "c".into(),
                        sub_fields: vec![
                            Field {
                                data_type: DataType::Int64,
                                name: "aa".into(),
                                ..mock_field.clone()
                            },
                            Field {
                                data_type: DataType::Float64,
                                name: "bb".into(),
                                ..mock_field.clone()
                            },
                        ],
                        ..mock_field.clone()
                    },
                ],
                ..mock_field.clone()
            },
            Field {
                data_type: DataType::List(Box::new(DataType::Bytea)),
                name: "v11".into(),
                ..mock_field.clone()
            },
            Field {
                data_type: DataType::Jsonb,
                name: "12".into(),
                ..mock_field.clone()
            },
            Field {
                data_type: DataType::Serial,
                name: "13".into(),
                ..mock_field.clone()
            },
            Field {
                data_type: DataType::Int256,
                name: "14".into(),
                ..mock_field.clone()
            },
        ];
        let schema = generate_json_converter_schema(&fields, "test").to_string();
        let ans = r#"{"fields":[{"field":"v1","optional":"true","type":"boolean"},{"field":"v2","optional":"true","type":"int16"},{"field":"v3","optional":"true","type":"int32"},{"field":"v4","optional":"true","type":"float"},{"field":"v5","optional":"true","type":"string"},{"field":"v6","optional":"true","type":"int32"},{"field":"v7","optional":"true","type":"string"},{"field":"v8","optional":"true","type":"int64"},{"field":"v9","optional":"true","type":"string"},{"field":"v10","fields":[{"field":"a","optional":"true","type":"int64"},{"field":"b","optional":"true","type":"string"},{"field":"c","fields":[{"field":"aa","optional":"true","type":"int64"},{"field":"bb","optional":"true","type":"double"}],"optional":"true","type":"struct"}],"optional":"true","type":"struct"},{"field":"v11","items":{"type":"bytes"},"optional":"true","type":"array"},{"field":"12","optional":"true","type":"string"},{"field":"13","optional":"true","type":"int32"},{"field":"14","optional":"true","type":"string"}],"name":"test","optional":"false","type":"struct"}"#;
        assert_eq!(schema, ans);
    }
}

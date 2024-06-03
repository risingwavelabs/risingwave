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

use std::sync::LazyLock;

use apache_avro::schema::{DecimalSchema, RecordSchema, Schema};
use apache_avro::types::{Value, ValueKind};
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::log::LogSuppresser;
use risingwave_common::types::{DataType, Decimal};
use risingwave_pb::plan_common::{AdditionalColumn, ColumnDesc, ColumnDescVersion};

use crate::error::ConnectorResult;
use crate::parser::unified::bail_uncategorized;
use crate::parser::{AccessError, MapHandling};

pub fn avro_schema_to_column_descs(
    schema: &Schema,
    map_handling: Option<MapHandling>,
) -> ConnectorResult<Vec<ColumnDesc>> {
    if let Schema::Record(RecordSchema { fields, .. }) = schema {
        let mut index = 0;
        let fields = fields
            .iter()
            .map(|field| {
                avro_field_to_column_desc(&field.name, &field.schema, &mut index, map_handling)
            })
            .collect::<ConnectorResult<Vec<_>>>()?;
        Ok(fields)
    } else {
        bail!("schema invalid, record type required at top level of the schema.");
    }
}

const DBZ_VARIABLE_SCALE_DECIMAL_NAME: &str = "VariableScaleDecimal";
const DBZ_VARIABLE_SCALE_DECIMAL_NAMESPACE: &str = "io.debezium.data";

fn avro_field_to_column_desc(
    name: &str,
    schema: &Schema,
    index: &mut i32,
    map_handling: Option<MapHandling>,
) -> ConnectorResult<ColumnDesc> {
    let data_type = avro_type_mapping(schema, map_handling)?;
    match schema {
        Schema::Record(RecordSchema {
            name: schema_name,
            fields,
            ..
        }) => {
            let vec_column = fields
                .iter()
                .map(|f| avro_field_to_column_desc(&f.name, &f.schema, index, map_handling))
                .collect::<ConnectorResult<Vec<_>>>()?;
            *index += 1;
            Ok(ColumnDesc {
                column_type: Some(data_type.to_protobuf()),
                column_id: *index,
                name: name.to_owned(),
                field_descs: vec_column,
                type_name: schema_name.to_string(),
                generated_or_default_column: None,
                description: None,
                additional_column_type: 0, // deprecated
                additional_column: Some(AdditionalColumn { column_type: None }),
                version: ColumnDescVersion::Pr13707 as i32,
            })
        }
        _ => {
            *index += 1;
            Ok(ColumnDesc {
                column_type: Some(data_type.to_protobuf()),
                column_id: *index,
                name: name.to_owned(),
                additional_column: Some(AdditionalColumn { column_type: None }),
                version: ColumnDescVersion::Pr13707 as i32,
                ..Default::default()
            })
        }
    }
}

fn avro_type_mapping(
    schema: &Schema,
    map_handling: Option<MapHandling>,
) -> ConnectorResult<DataType> {
    let data_type = match schema {
        Schema::String => DataType::Varchar,
        Schema::Int => DataType::Int32,
        Schema::Long => DataType::Int64,
        Schema::Boolean => DataType::Boolean,
        Schema::Float => DataType::Float32,
        Schema::Double => DataType::Float64,
        Schema::Decimal(DecimalSchema { precision, .. }) => {
            if *precision > Decimal::MAX_PRECISION.into() {
                static LOG_SUPPERSSER: LazyLock<LogSuppresser> =
                    LazyLock::new(LogSuppresser::default);
                if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                    tracing::warn!(
                        suppressed_count,
                        "RisingWave supports decimal precision up to {}, but got {}. Will truncate.",
                        Decimal::MAX_PRECISION,
                        precision
                    );
                }
            }
            DataType::Decimal
        }
        Schema::Date => DataType::Date,
        Schema::LocalTimestampMillis => DataType::Timestamp,
        Schema::LocalTimestampMicros => DataType::Timestamp,
        Schema::TimestampMillis => DataType::Timestamptz,
        Schema::TimestampMicros => DataType::Timestamptz,
        Schema::Duration => DataType::Interval,
        Schema::Bytes => DataType::Bytea,
        Schema::Enum { .. } => DataType::Varchar,
        Schema::TimeMillis => DataType::Time,
        Schema::TimeMicros => DataType::Time,
        Schema::Record(RecordSchema { fields, name, .. }) => {
            if name.name == DBZ_VARIABLE_SCALE_DECIMAL_NAME
                && name.namespace == Some(DBZ_VARIABLE_SCALE_DECIMAL_NAMESPACE.into())
            {
                return Ok(DataType::Decimal);
            }

            let struct_fields = fields
                .iter()
                .map(|f| avro_type_mapping(&f.schema, map_handling))
                .collect::<ConnectorResult<Vec<_>>>()?;
            let struct_names = fields.iter().map(|f| f.name.clone()).collect_vec();
            DataType::new_struct(struct_fields, struct_names)
        }
        Schema::Array(item_schema) => {
            let item_type = avro_type_mapping(item_schema.as_ref(), map_handling)?;
            DataType::List(Box::new(item_type))
        }
        Schema::Union(union_schema) => {
            // We only support using union to represent nullable fields, not general unions.
            let variants = union_schema.variants();
            if variants.len() != 2 || !variants.contains(&Schema::Null) {
                bail!(
                    "unsupported Avro type, only unions like [null, T] is supported: {:?}",
                    schema
                );
            }
            let nested_schema = variants
                .iter()
                .find_or_first(|s| !matches!(s, Schema::Null))
                .unwrap();

            avro_type_mapping(nested_schema, map_handling)?
        }
        Schema::Ref { name } => {
            if name.name == DBZ_VARIABLE_SCALE_DECIMAL_NAME
                && name.namespace == Some(DBZ_VARIABLE_SCALE_DECIMAL_NAMESPACE.into())
            {
                DataType::Decimal
            } else {
                bail!("unsupported Avro type: {:?}", schema);
            }
        }
        Schema::Map(value_schema) => {
            // TODO: support native map type
            match map_handling {
                Some(MapHandling::Jsonb) => {
                    if supported_avro_to_json_type(value_schema) {
                        DataType::Jsonb
                    } else {
                        bail!(
                            "unsupported Avro type, cannot convert map to jsonb: {:?}",
                            schema
                        )
                    }
                }
                None => {
                    bail!("`map.handling.mode` not specified in ENCODE AVRO (...). Currently supported modes: `jsonb`")
                }
            }
        }
        Schema::Null | Schema::Fixed(_) | Schema::Uuid => {
            bail!("unsupported Avro type: {:?}", schema)
        }
    };

    Ok(data_type)
}

/// Check for [`avro_to_jsonb`]
fn supported_avro_to_json_type(schema: &Schema) -> bool {
    match schema {
        Schema::Null | Schema::Boolean | Schema::Int | Schema::String => true,

        Schema::Map(value_schema) | Schema::Array(value_schema) => {
            supported_avro_to_json_type(value_schema)
        }
        Schema::Record(RecordSchema { fields, .. }) => fields
            .iter()
            .all(|f| supported_avro_to_json_type(&f.schema)),
        Schema::Long
        | Schema::Float
        | Schema::Double
        | Schema::Bytes
        | Schema::Enum(_)
        | Schema::Fixed(_)
        | Schema::Decimal(_)
        | Schema::Uuid
        | Schema::Date
        | Schema::TimeMillis
        | Schema::TimeMicros
        | Schema::TimestampMillis
        | Schema::TimestampMicros
        | Schema::LocalTimestampMillis
        | Schema::LocalTimestampMicros
        | Schema::Duration
        | Schema::Ref { name: _ }
        | Schema::Union(_) => false,
    }
}

pub(crate) fn avro_to_jsonb(
    avro: &Value,
    builder: &mut jsonbb::Builder,
) -> crate::parser::AccessResult<()> {
    match avro {
        Value::Null => builder.add_null(),
        Value::Boolean(b) => builder.add_bool(*b),
        Value::Int(i) => builder.add_i64(*i as i64),
        Value::String(s) => builder.add_string(s),
        Value::Map(m) => {
            builder.begin_object();
            for (k, v) in m {
                builder.add_string(k);
                avro_to_jsonb(v, builder)?;
            }
            builder.end_object()
        }
        // same representation as map
        Value::Record(r) => {
            builder.begin_object();
            for (k, v) in r {
                builder.add_string(k);
                avro_to_jsonb(v, builder)?;
            }
            builder.end_object()
        }
        Value::Array(a) => {
            builder.begin_array();
            for v in a {
                avro_to_jsonb(v, builder)?;
            }
            builder.end_array()
        }

        // TODO: figure out where the following encoding is reasonable before enabling them.
        // See discussions: https://github.com/risingwavelabs/risingwave/pull/16948

        // jsonbb supports int64, but JSON spec does not allow it. How should we handle it?
        // BTW, protobuf canonical JSON converts int64 to string.
        // Value::Long(l) => builder.add_i64(*l),
        // Value::Float(f) => {
        //     if f.is_nan() || f.is_infinite() {
        //         // XXX: pad null or return err here?
        //         builder.add_null()
        //     } else {
        //         builder.add_f64(*f as f64)
        //     }
        // }
        // Value::Double(f) => {
        //     if f.is_nan() || f.is_infinite() {
        //         // XXX: pad null or return err here?
        //         builder.add_null()
        //     } else {
        //         builder.add_f64(*f)
        //     }
        // }
        // // XXX: What encoding to use?
        // // ToText is \x plus hex string.
        // Value::Bytes(b) => builder.add_string(&ToText::to_text(&b.as_slice())),
        // Value::Enum(_, symbol) => {
        //     builder.add_string(&symbol);
        // }
        // Value::Uuid(id) => builder.add_string(&id.as_hyphenated().to_string()),
        // // For Union, one concern is that the avro union is tagged (like rust enum) but json union is untagged (like c union).
        // // When the union consists of multiple records, it is possible to distinguish which variant is active in avro, but in json they will all become jsonb objects and indistinguishable.
        // Value::Union(_, v) => avro_to_jsonb(v, builder)?
        // XXX: pad null or return err here?
        v @ (Value::Long(_)
        | Value::Float(_)
        | Value::Double(_)
        | Value::Bytes(_)
        | Value::Enum(_, _)
        | Value::Fixed(_, _)
        | Value::Date(_)
        | Value::Decimal(_)
        | Value::TimeMillis(_)
        | Value::TimeMicros(_)
        | Value::TimestampMillis(_)
        | Value::TimestampMicros(_)
        | Value::LocalTimestampMillis(_)
        | Value::LocalTimestampMicros(_)
        | Value::Duration(_)
        | Value::Uuid(_)
        | Value::Union(_, _)) => {
            bail_uncategorized!(
                "unimplemented conversion from avro to jsonb: {:?}",
                ValueKind::from(v)
            )
        }
    }
    Ok(())
}

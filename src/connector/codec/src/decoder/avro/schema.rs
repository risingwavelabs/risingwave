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

use std::sync::{Arc, LazyLock};

use apache_avro::schema::{DecimalSchema, RecordSchema, ResolvedSchema, Schema};
use apache_avro::AvroResult;
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::log::LogSuppresser;
use risingwave_common::types::{DataType, Decimal};
use risingwave_pb::plan_common::{AdditionalColumn, ColumnDesc, ColumnDescVersion};

/// Avro schema with `Ref` inlined. The newtype is used to indicate whether the schema is resolved.
///
/// TODO: Actually most of the place should use resolved schema, but currently they just happen to work (Some edge cases are not met yet).
///
/// TODO: refactor avro lib to use the feature there.
#[derive(Debug)]
pub struct ResolvedAvroSchema {
    /// Should be used for parsing bytes into Avro value
    pub original_schema: Arc<Schema>,
    /// Should be used for type mapping from Avro value to RisingWave datum
    pub resolved_schema: Schema,
}

impl ResolvedAvroSchema {
    pub fn create(schema: Arc<Schema>) -> AvroResult<Self> {
        let resolver = ResolvedSchema::try_from(schema.as_ref())?;
        // todo: to_resolved may cause stackoverflow if there's a loop in the schema
        let resolved_schema = resolver.to_resolved(schema.as_ref())?;
        Ok(Self {
            original_schema: schema,
            resolved_schema,
        })
    }
}

/// How to convert the map type from the input encoding to RisingWave's datatype.
///
/// XXX: Should this be `avro.map.handling.mode`? Can it be shared between Avro and Protobuf?
#[derive(Debug, Copy, Clone)]
pub enum MapHandling {
    Jsonb,
    // TODO: <https://github.com/risingwavelabs/risingwave/issues/13387>
    // Map
}

impl MapHandling {
    pub const OPTION_KEY: &'static str = "map.handling.mode";

    pub fn from_options(
        options: &std::collections::HashMap<String, String>,
    ) -> anyhow::Result<Option<Self>> {
        let mode = match options.get(Self::OPTION_KEY).map(std::ops::Deref::deref) {
            Some("jsonb") => Self::Jsonb,
            Some(v) => bail!("unrecognized {} value {}", Self::OPTION_KEY, v),
            None => return Ok(None),
        };
        Ok(Some(mode))
    }
}

/// This function expects resolved schema (no `Ref`).
/// FIXME: require passing resolved schema here.
pub fn avro_schema_to_column_descs(
    schema: &Schema,
    map_handling: Option<MapHandling>,
) -> anyhow::Result<Vec<ColumnDesc>> {
    if let Schema::Record(RecordSchema { fields, .. }) = schema {
        let mut index = 0;
        let fields = fields
            .iter()
            .map(|field| {
                avro_field_to_column_desc(&field.name, &field.schema, &mut index, map_handling)
            })
            .collect::<anyhow::Result<_>>()?;
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
) -> anyhow::Result<ColumnDesc> {
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
                .collect::<anyhow::Result<_>>()?;
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

/// This function expects resolved schema (no `Ref`).
fn avro_type_mapping(
    schema: &Schema,
    map_handling: Option<MapHandling>,
) -> anyhow::Result<DataType> {
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
                .collect::<anyhow::Result<_>>()?;
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
        Schema::Uuid => DataType::Varchar,
        Schema::Null | Schema::Fixed(_) => {
            bail!("unsupported Avro type: {:?}", schema)
        }
    };

    Ok(data_type)
}

/// Check for [`super::avro_to_jsonb`]
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

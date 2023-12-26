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

use std::sync::LazyLock;

use apache_avro::schema::{DecimalSchema, RecordSchema, Schema};
use itertools::Itertools;
use risingwave_common::log::LogSuppresser;
use risingwave_common::types::{DataType, Decimal};
use risingwave_pb::plan_common::{AdditionalColumnType, ColumnDesc, ColumnDescVersion};

pub fn avro_schema_to_column_descs(schema: &Schema) -> anyhow::Result<Vec<ColumnDesc>> {
    if let Schema::Record(RecordSchema { fields, .. }) = schema {
        let mut index = 0;
        let fields = fields
            .iter()
            .map(|field| avro_field_to_column_desc(&field.name, &field.schema, &mut index))
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(fields)
    } else {
        anyhow::bail!("schema invalid, record type required at top level of the schema.");
    }
}

const DBZ_VARIABLE_SCALE_DECIMAL_NAME: &str = "VariableScaleDecimal";
const DBZ_VARIABLE_SCALE_DECIMAL_NAMESPACE: &str = "io.debezium.data";

fn avro_field_to_column_desc(
    name: &str,
    schema: &Schema,
    index: &mut i32,
) -> anyhow::Result<ColumnDesc> {
    let data_type = avro_type_mapping(schema)?;
    match schema {
        Schema::Record(RecordSchema {
            name: schema_name,
            fields,
            ..
        }) => {
            let vec_column = fields
                .iter()
                .map(|f| avro_field_to_column_desc(&f.name, &f.schema, index))
                .collect::<anyhow::Result<Vec<_>>>()?;
            *index += 1;
            Ok(ColumnDesc {
                column_type: Some(data_type.to_protobuf()),
                column_id: *index,
                name: name.to_owned(),
                field_descs: vec_column,
                type_name: schema_name.to_string(),
                generated_or_default_column: None,
                description: None,
                additional_column_type: AdditionalColumnType::Normal as i32,
                version: ColumnDescVersion::Pr13707 as i32,
            })
        }
        _ => {
            *index += 1;
            Ok(ColumnDesc {
                column_type: Some(data_type.to_protobuf()),
                column_id: *index,
                name: name.to_owned(),
                additional_column_type: AdditionalColumnType::Normal as i32,
                version: ColumnDescVersion::Pr13707 as i32,
                ..Default::default()
            })
        }
    }
}

fn avro_type_mapping(schema: &Schema) -> anyhow::Result<DataType> {
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
                    "RisingWave supports decimal precision up to {}, but got {}. Will truncate. ({} suppressed)",
                    Decimal::MAX_PRECISION,
                    suppressed_count,
                    precision
                );
                }
            }
            DataType::Decimal
        }
        Schema::Date => DataType::Date,
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
                .map(|f| avro_type_mapping(&f.schema))
                .collect::<anyhow::Result<Vec<_>>>()?;
            let struct_names = fields.iter().map(|f| f.name.clone()).collect_vec();
            DataType::new_struct(struct_fields, struct_names)
        }
        Schema::Array(item_schema) => {
            let item_type = avro_type_mapping(item_schema.as_ref())?;
            DataType::List(Box::new(item_type))
        }
        Schema::Union(union_schema) => {
            let nested_schema = union_schema
                .variants()
                .iter()
                .find_or_first(|s| !matches!(s, Schema::Null))
                .ok_or_else(|| {
                    anyhow::format_err!("unsupported type in Avro: {:?}", union_schema)
                })?;

            avro_type_mapping(nested_schema)?
        }
        _ => {
            return Err(anyhow::format_err!(
                "unsupported type in Avro: {:?}",
                schema
            ));
        }
    };

    Ok(data_type)
}

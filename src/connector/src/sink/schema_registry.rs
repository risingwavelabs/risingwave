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
use risingwave_common::catalog::{Field, SchemaId};
use risingwave_common::types::DataType;
use url::Url;
use valico::json_schema::{self, Builder, PrimitiveType};

use crate::parser::{Client, SchemaRegistryAuth};
use crate::sink::{Result, SinkError};

// Currently rw does not support alter sink, so schema version won't change
pub const SCHEMA_REGISTRY_VERSION_PREFIX: &str = "00001";

/// A Schema type
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum SchemaType {
    Avro,
    Protobuf,
    Json,
}

/// Register a schema
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RegisterSchema {
    pub schema: String,
    pub schema_type: SchemaType,
}

// Should be in align with `datum_to_json_object`.
fn datatype_to_json_schema_type(data_type: &DataType) -> PrimitiveType {
    match data_type {
        DataType::Boolean => PrimitiveType::Boolean,
        DataType::Int16 => PrimitiveType::Integer,
        DataType::Int32 => PrimitiveType::Integer,
        DataType::Int64 => PrimitiveType::Number,
        DataType::Float32 => PrimitiveType::Number,
        DataType::Float64 => PrimitiveType::Number,
        DataType::Decimal => PrimitiveType::Number,
        DataType::Date => PrimitiveType::Integer,
        DataType::Varchar => PrimitiveType::String,
        DataType::Time => PrimitiveType::Number,
        // Currently rw only sinks with `TimestampHandlingMode::Milli`
        DataType::Timestamp => PrimitiveType::Number,
        DataType::Timestamptz => PrimitiveType::String,
        DataType::Interval => PrimitiveType::String,
        DataType::Struct(_) => PrimitiveType::Object,
        DataType::List(_) => PrimitiveType::Array,
        DataType::Bytea => PrimitiveType::String,
        DataType::Jsonb => PrimitiveType::String,
        DataType::Serial => PrimitiveType::String,
        DataType::Int256 => PrimitiveType::Number,
    }
}

fn apply_field_to_json_schema_builder(field: &Field, builder: &mut Builder) {
    builder.type_(datatype_to_json_schema_type(&field.data_type));
    match &field.data_type {
        DataType::List(data_type) => builder.items_schema(|item| {
            item.type_(datatype_to_json_schema_type(data_type));
        }),
        DataType::Struct(_) => {
            builder.properties(|props| {
                for sub_field in &field.sub_fields {
                    props.insert(&sub_field.name, |prop| {
                        apply_field_to_json_schema_builder(sub_field, prop);
                    })
                }
            });
        }
        _ => {}
    }
}

pub fn generate_json_schema(schema_name: &str, fields: &Vec<Field>) -> RegisterSchema {
    let schema = json_schema::builder::schema(|s| {
        // Valico is based on draft 7
        s.schema("https://json-schema.org/draft-07/schema");
        s.title(schema_name);
        s.type_(PrimitiveType::Object);
        s.properties(|props| {
            for field in fields {
                props.insert(&field.name, |prop| {
                    apply_field_to_json_schema_builder(field, prop);
                })
            }
        })
    })
    .into_json()
    .to_string();
    RegisterSchema {
        schema,
        schema_type: SchemaType::Json,
    }
}

pub async fn post_schema_to_schema_registry(
    url: &str,
    schema: &RegisterSchema,
    subject: &str,
    schema_registry_auth: &SchemaRegistryAuth,
) -> Result<SchemaId> {
    let url = Url::parse(url)
        .map_err(|e| SinkError::SchemaRegistry(anyhow!("failed to parse url ({}): {}", url, e,)))?;
    let client = Client::new(url, schema_registry_auth)?;
    client
        .post_schema(subject, schema)
        .await
        .map_err(SinkError::from)
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::Field;
    use risingwave_common::types::{DataType, StructType};

    use crate::sink::schema_registry::generate_json_schema;

    #[test]
    fn test_generate_json_schema() {
        let fields = vec![
            Field {
                data_type: DataType::Boolean,
                name: "v1".into(),
                sub_fields: vec![],
                type_name: "".into(),
            },
            Field {
                data_type: DataType::Varchar,
                name: "v2".into(),
                sub_fields: vec![],
                type_name: "".into(),
            },
            Field {
                data_type: DataType::List(Box::new(DataType::Varchar)),
                name: "v3".into(),
                sub_fields: vec![],
                type_name: "".into(),
            },
            Field {
                data_type: DataType::Struct(StructType::new(vec![
                    ("a", DataType::Int64),
                    ("b", DataType::Float64),
                ])),
                name: "v4".into(),
                sub_fields: vec![
                    Field {
                        data_type: DataType::Int64,
                        name: "a".into(),
                        sub_fields: vec![],
                        type_name: "".into(),
                    },
                    Field {
                        data_type: DataType::Float64,
                        name: "b".into(),
                        sub_fields: vec![],
                        type_name: "".into(),
                    },
                ],
                type_name: "".into(),
            },
        ];
        let json_schema = generate_json_schema("test", &fields).schema;
        let json_schema_ans = "{\"$schema\":\"https://json-schema.org/draft-07/schema\",\"properties\":{\"v1\":{\"type\":\"boolean\"},\"v2\":{\"type\":\"string\"},\"v3\":{\"items\":{\"type\":\"string\"},\"type\":\"array\"},\"v4\":{\"properties\":{\"a\":{\"type\":\"number\"},\"b\":{\"type\":\"number\"}},\"type\":\"object\"}},\"schemaType\":\"JSON\",\"title\":\"test\"}";
        assert_eq!(json_schema, json_schema_ans);
    }
}

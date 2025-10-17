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

use std::collections::HashMap;

use serde_json::Value;

use crate::common::kconnect::{Schema, SchemaBuilder};

#[derive(Debug, thiserror::Error, thiserror_ext::Macro)]
#[error("{0}")]
pub struct DataException(#[message] String);

const SCHEMA_TYPE_FIELD_NAME: &str = "type";
const ARRAY_ITEMS_FIELD_NAME: &str = "items";
const MAP_KEY_FIELD_NAME: &str = "keys";
const MAP_VALUE_FIELD_NAME: &str = "values";
const STRUCT_FIELDS_FIELD_NAME: &str = "fields";
const STRUCT_FIELD_NAME_FIELD_NAME: &str = "field";
const SCHEMA_OPTIONAL_FIELD_NAME: &str = "optional";
const SCHEMA_NAME_FIELD_NAME: &str = "name";
const SCHEMA_VERSION_FIELD_NAME: &str = "version";
const SCHEMA_DOC_FIELD_NAME: &str = "doc";
const SCHEMA_PARAMETERS_FIELD_NAME: &str = "parameters";
const SCHEMA_DEFAULT_FIELD_NAME: &str = "default";

const BOOLEAN_TYPE_NAME: &str = "boolean";
const INT8_TYPE_NAME: &str = "int8";
const INT16_TYPE_NAME: &str = "int16";
const INT32_TYPE_NAME: &str = "int32";
const INT64_TYPE_NAME: &str = "int64";
const FLOAT_TYPE_NAME: &str = "float";
const DOUBLE_TYPE_NAME: &str = "double";
const BYTES_TYPE_NAME: &str = "bytes";
const STRING_TYPE_NAME: &str = "string";
const ARRAY_TYPE_NAME: &str = "array";
const MAP_TYPE_NAME: &str = "map";
const STRUCT_TYPE_NAME: &str = "struct";

/// `JsonConverter::asConnectSchema`
pub fn decode_schema_from_json(j: &Value) -> Result<Option<Schema>, DataException> {
    if j.is_null() {
        return Ok(None);
    }

    let mut cache: HashMap<Value, Schema> = HashMap::new();
    if let Some(cached) = cache.get(j) {
        return Ok(Some(cached.clone()));
    }

    let Some(Value::String(type_)) = j.get(SCHEMA_TYPE_FIELD_NAME) else {
        bail_data_exception!("Schema must contain 'type' field");
    };

    let mut builder = match type_.as_str() {
        BOOLEAN_TYPE_NAME => SchemaBuilder::bool(),
        INT8_TYPE_NAME => SchemaBuilder::int8(),
        INT16_TYPE_NAME => SchemaBuilder::int16(),
        INT32_TYPE_NAME => SchemaBuilder::int32(),
        INT64_TYPE_NAME => SchemaBuilder::int64(),
        FLOAT_TYPE_NAME => SchemaBuilder::float32(),
        DOUBLE_TYPE_NAME => SchemaBuilder::float64(),
        BYTES_TYPE_NAME => SchemaBuilder::bytes(),
        STRING_TYPE_NAME => SchemaBuilder::string(),
        ARRAY_TYPE_NAME => {
            let Some(elem_schema) = j.get(ARRAY_ITEMS_FIELD_NAME) else {
                bail_data_exception!("Array schema did not specify the element type");
            };
            let Some(elem_schema) = decode_schema_from_json(elem_schema)? else {
                // from ArrayBuilder.array
                bail_data_exception!("element schema cannot be null.");
            };
            SchemaBuilder::array(elem_schema)
        }
        MAP_TYPE_NAME => {
            let Some(key_schema) = j.get(MAP_KEY_FIELD_NAME) else {
                bail_data_exception!("Map schema did not specify the key type");
            };
            let Some(key_schema) = decode_schema_from_json(key_schema)? else {
                bail_data_exception!("key schema cannot be null.");
            };
            let Some(value_schema) = j.get(MAP_VALUE_FIELD_NAME) else {
                bail_data_exception!("Map schema did not specify the value type");
            };
            let Some(value_schema) = decode_schema_from_json(value_schema)? else {
                bail_data_exception!("value schema cannot be null.");
            };
            SchemaBuilder::map(key_schema, value_schema)
        }
        STRUCT_TYPE_NAME => {
            let mut b = SchemaBuilder::struct_();
            let Some(Value::Array(fields)) = j.get(STRUCT_FIELDS_FIELD_NAME) else {
                bail_data_exception!("Struct schema's \"fields\" argument is not an array.",);
            };
            for field in fields {
                let Some(Value::String(field_name)) = field.get(STRUCT_FIELD_NAME_FIELD_NAME)
                else {
                    bail_data_exception!("Struct schema's field name not specified properly",);
                };
                let Some(field) = decode_schema_from_json(field)? else {
                    bail_data_exception!("field schema for field {field_name} cannot be null.");
                };
                b.field(field_name, field);
            }
            b
        }
        _ => {
            bail_data_exception!("Unknown schema type: {type_}");
        }
    };

    if let Some(Value::Bool(opt)) = j.get(SCHEMA_OPTIONAL_FIELD_NAME)
        && *opt
    {
        builder.optional();
    } else {
        builder.required();
    }

    if let Some(Value::String(name)) = j.get(SCHEMA_NAME_FIELD_NAME) {
        builder.name(name);
    }

    if let Some(Value::Number(version)) = j.get(SCHEMA_VERSION_FIELD_NAME)
        && let Some(v) = version.as_i64()
    {
        builder.version(v.try_into().unwrap());
    }

    if let Some(Value::String(doc)) = j.get(SCHEMA_DOC_FIELD_NAME) {
        builder.doc(doc);
    }

    if let Some(Value::Object(params)) = j.get(SCHEMA_PARAMETERS_FIELD_NAME) {
        for (k, v) in params {
            let Value::String(param_value) = v else {
                bail_data_exception!("Schema parameters must have string values.");
            };
            builder.parameters(k, param_value);
        }
    }

    if let Some(_default) = j.get(SCHEMA_DEFAULT_FIELD_NAME) {
        tracing::info!("Schema default value is not supported yet.");
    }

    let result = builder.build();
    cache.insert(j.clone(), result.clone());
    Ok(Some(result))
}

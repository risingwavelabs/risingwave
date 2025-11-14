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

use super::constants::{field_name, type_name};
use super::generic::{CacheKey, JsonRead};
use crate::kconnect::data::schema::ConnectSchema;
use crate::kconnect::errors::{DataException, bail_data_exception};

#[derive(Default, Debug)]
pub struct Cache<J> {
    last: Option<(J, ConnectSchema)>,
}

impl<'a, J: CacheKey<'a>> Cache<J>
where
    J::Borrowed: JsonRead,
{
    pub fn decode(&mut self, j: &J::Borrowed) -> Result<Option<ConnectSchema>, DataException> {
        if let Some((cached_j, cached_schema)) = &self.last
            && cached_j.eq_borrowed(j)
        {
            return Ok(Some(cached_schema.clone()));
        }
        let start = std::time::Instant::now();
        let schema = decode_schema_from_json(j)?;
        if let Some(ref s) = schema {
            self.last = Some((J::from_borrowed(j), s.clone()));
            tracing::info!(
                "kconnect schema parsed in {} seconds",
                start.elapsed().as_secs_f64()
            );
        }
        Ok(schema)
    }
}

/// `JsonConverter::asConnectSchema`
fn decode_schema_from_json(j: &impl JsonRead) -> Result<Option<ConnectSchema>, DataException> {
    if j.is_null() {
        return Ok(None);
    }

    // TODO: check cache

    let Some(type_) = j.get_str(field_name::SCHEMA_TYPE) else {
        bail_data_exception!("Schema must contain 'type' field");
    };

    let mut builder = match type_ {
        type_name::BOOLEAN => ConnectSchema::bool(),
        type_name::INT8 => ConnectSchema::int8(),
        type_name::INT16 => ConnectSchema::int16(),
        type_name::INT32 => ConnectSchema::int32(),
        type_name::INT64 => ConnectSchema::int64(),
        type_name::FLOAT => ConnectSchema::float32(),
        type_name::DOUBLE => ConnectSchema::float64(),
        type_name::BYTES => ConnectSchema::bytes(),
        type_name::STRING => ConnectSchema::string(),
        type_name::ARRAY => {
            let Some(elem_schema) = j.get(field_name::ARRAY_ITEMS) else {
                bail_data_exception!("Array schema did not specify the element type");
            };
            let Some(elem_schema) = decode_schema_from_json(elem_schema)? else {
                // from ArrayBuilder.array
                bail_data_exception!("element schema cannot be null.");
            };
            ConnectSchema::array(elem_schema)
        }
        type_name::MAP => {
            let Some(key_schema) = j.get(field_name::MAP_KEY) else {
                bail_data_exception!("Map schema did not specify the key type");
            };
            let Some(key_schema) = decode_schema_from_json(key_schema)? else {
                bail_data_exception!("key schema cannot be null.");
            };
            let Some(value_schema) = j.get(field_name::MAP_VALUE) else {
                bail_data_exception!("Map schema did not specify the value type");
            };
            let Some(value_schema) = decode_schema_from_json(value_schema)? else {
                bail_data_exception!("value schema cannot be null.");
            };
            ConnectSchema::map(key_schema, value_schema)
        }
        type_name::STRUCT => {
            let mut b = ConnectSchema::struct_builder();
            let Some(fields) = j.get_array(field_name::STRUCT_FIELDS) else {
                bail_data_exception!("Struct schema's \"fields\" argument is not an array.",);
            };
            for field in fields {
                let Some(field_name) = field.get_str(field_name::STRUCT_FIELD_NAME) else {
                    bail_data_exception!("Struct schema's field name not specified properly",);
                };
                let Some(field) = decode_schema_from_json(field)? else {
                    bail_data_exception!("field schema for field {field_name} cannot be null.");
                };
                b.add_field(field_name, field)?;
            }
            b.build()
        }
        _ => {
            bail_data_exception!("Unknown schema type: {type_}");
        }
    };

    if let Some(opt) = j.get_bool(field_name::SCHEMA_OPTIONAL) {
        builder.base_mut().optional = opt;
    }

    if let Some(name) = j.get_str(field_name::SCHEMA_NAME) {
        builder.base_mut().name = Some(name.into());
    }

    if let Some(version) = j.get_i32(field_name::SCHEMA_VERSION) {
        builder.base_mut().version = Some(version);
    }

    if let Some(doc) = j.get_str(field_name::SCHEMA_DOC) {
        builder.base_mut().doc = Some(doc.into());
    }

    if let Some(params) = j.get_kv_iter(field_name::SCHEMA_PARAMETERS) {
        for (k, v) in params {
            let Some(param_value) = v.as_str() else {
                bail_data_exception!("Schema parameters must have string values.");
            };
            builder
                .base_mut()
                .parameters
                .insert(k.into(), param_value.into());
        }
    }

    if let Some(_default) = j.get(field_name::SCHEMA_DEFAULT) {
        tracing::info!("Schema default value is not supported yet.");
    }

    let result = builder;
    // cache.insert(j.clone(), result.clone());
    Ok(Some(result))
}

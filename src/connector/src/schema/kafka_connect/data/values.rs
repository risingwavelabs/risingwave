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

use super::schema::{ConnectSchema, Value};
use crate::schema::kafka_connect::error::DataException;

fn convert_to(
    to_schema: &ConnectSchema,
    from_schema: &ConnectSchema,
    value: Option<&Value>,
) -> Result<Option<Value>, DataException> {
    let Some(value) = value else {
        return match to_schema.base().optional {
            true => Ok(None),
            false => Err(DataException::new(
                "Unable to convert a null value to a schema that requires a value",
            )),
        };
    };
    match to_schema.base().type_ {
        super::schema::SchemaType::Bytes => {
            if Some(super::decimal::LOGICAL_NAME) == to_schema.base().name.as_deref() {
                // BigDecimal from bytes | BigDecimal | Number->double | String
                todo!()
            }
            // bytes from bytes | BigDecimal
            todo!()
        }
        super::schema::SchemaType::String => todo!(),
        super::schema::SchemaType::Boolean => todo!(),
        super::schema::SchemaType::Int8 => todo!(),
        super::schema::SchemaType::Int16 => todo!(),
        super::schema::SchemaType::Int32 => todo!(),
        super::schema::SchemaType::Int64 => todo!(),
        super::schema::SchemaType::Float32 => todo!(),
        super::schema::SchemaType::Float64 => todo!(),
        super::schema::SchemaType::Array => todo!(),
        super::schema::SchemaType::Map => todo!(),
        super::schema::SchemaType::Struct => todo!(),
    }
}

// fn append()

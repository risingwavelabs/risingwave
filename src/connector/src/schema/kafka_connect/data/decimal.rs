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

use super::schema::{ConnectSchema, PrimitiveSchema, SchemaType};

pub const LOGICAL_NAME: &str = "org.apache.kafka.connect.data.Decimal";
pub const SCALE_FIELD: &str = "scale";

pub fn schema(scale: i32) -> ConnectSchema {
    let mut schema = ConnectSchema::Primitive(PrimitiveSchema::new(SchemaType::Bytes));
    let base_mut = schema.base_mut();
    base_mut.name = Some(LOGICAL_NAME.into());
    base_mut
        .parameters
        .insert(SCALE_FIELD.into(), scale.to_string());
    base_mut.version = Some(1);
    schema
}

pub struct BigDecimal;

impl BigDecimal {
    pub fn scale(&self) -> i32 {
        todo!()
    }

    pub fn unscaled_value_byte_array(&self) -> Vec<u8> {
        todo!()
    }

    pub fn new(_unscaled_bytes: &[u8], _scale: i32) -> Self {
        todo!()
    }
}

pub fn from_logical(schema: &ConnectSchema, value: BigDecimal) -> Result<Vec<u8>, String> {
    let schema_scale = scale(schema)?;
    if value.scale() != schema_scale {
        return Err(format!(
            "Decimal value has mismatching scale for given Decimal schema."
        ));
    }
    Ok(value.unscaled_value_byte_array())
}

pub fn to_logical(schema: &ConnectSchema, value: &[u8]) -> Result<BigDecimal, String> {
    Ok(BigDecimal::new(value, scale(schema)?))
}

fn scale(schema: &ConnectSchema) -> Result<i32, String> {
    let Some(scale_string) = schema.base().parameters.get(SCALE_FIELD) else {
        return Err(format!(
            "Invalid Decimal schema: scale parameter not found."
        ));
    };
    scale_string
        .parse()
        .map_err(|e| format!("Invalid scale parameter found in Decimal schema: {e}"))
}

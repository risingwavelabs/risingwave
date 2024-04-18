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

use super::schema::{Field, SchemaType, StructSchema, Value};
use crate::schema::kafka_connect::error::DataException;

#[derive(Debug, Clone)]
pub struct Struct {
    schema: Box<StructSchema>,
    values: Vec<Option<Value>>,
}

impl Struct {
    pub fn new(schema: StructSchema) -> Self {
        let len = schema.fields.len();
        Self {
            schema: schema.into(),
            values: vec![None; len],
        }
    }

    pub fn schema(&self) -> &StructSchema {
        &self.schema
    }

    pub fn get_by_name(&self, field_name: &str) -> Result<Option<&Value>, DataException> {
        let field = self.lookup_field(field_name)?;
        Ok(self.get_by_field(field))
    }

    pub fn get_by_field<'a>(&'a self, field: &'a Field) -> Option<&Value> {
        self.values[field.index]
            .as_ref()
            .or_else(|| field.schema.base().default.as_ref())
    }

    pub fn get_without_default(&self, field_name: &str) -> Result<Option<&Value>, DataException> {
        let field = self.lookup_field(field_name)?;
        Ok(self.values[field.index].as_ref())
    }

    // get_{int8,..,struct}

    pub fn put_by_name(
        &mut self,
        field_name: &str,
        val: Value,
    ) -> Result<&mut Self, DataException> {
        let field = self.lookup_field(field_name)?.clone(); // TODO: avoid clone
        self.put_by_field(&field, val)
    }

    pub fn put_by_field(&mut self, field: &Field, val: Value) -> Result<&mut Self, DataException> {
        field
            .schema
            .validate_value(Some(&val))
            .map_err(|e| DataException::new(format!("{}: {e}", field.name)))?;
        self.values[field.index].replace(val);
        Ok(self)
    }

    pub fn validate(&self) -> Result<(), String> {
        for field in &self.schema.fields {
            let field_schema = field.schema.base();
            let val = self.values[field.index].as_ref();
            if val.is_none() && (field_schema.optional || field_schema.default.is_some()) {
                continue;
            }
            field
                .schema
                .validate_value(val)
                .map_err(|e| format!("{}: {e}", field.name))?;
        }
        Ok(())
    }

    fn lookup_field(&self, field_name: &str) -> Result<&Field, DataException> {
        self.schema
            .field(field_name)
            .ok_or_else(|| DataException::new(format!("{field_name} is not a valid field name")))
    }

    fn get_check_type(
        &self,
        field_name: &str,
        type_: SchemaType,
    ) -> Result<Option<&Value>, DataException> {
        let field = self.lookup_field(field_name)?;
        if field.schema.base().type_ != type_ {
            return Err(DataException::new(format!(
                "Field '{field_name}' is not of type {type_}"
            )));
        }
        Ok(self.values[field.index].as_ref())
    }
}

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

use std::sync::Arc;

struct SchemaInner {
    name: Option<Box<str>>,
}

#[derive(Clone)]
pub struct Schema(Arc<SchemaInner>);
pub struct SchemaBuilder;

impl SchemaBuilder {
    pub fn bool() -> Self {
        SchemaBuilder
    }

    pub fn int8() -> Self {
        SchemaBuilder
    }

    pub fn int16() -> Self {
        SchemaBuilder
    }

    pub fn int32() -> Self {
        SchemaBuilder
    }

    pub fn int64() -> Self {
        SchemaBuilder
    }

    pub fn float32() -> Self {
        SchemaBuilder
    }

    pub fn float64() -> Self {
        SchemaBuilder
    }

    pub fn bytes() -> Self {
        SchemaBuilder
    }

    pub fn string() -> Self {
        SchemaBuilder
    }

    pub fn array(_items: Schema) -> Self {
        SchemaBuilder
    }

    pub fn map(_key: Schema, _value: Schema) -> Self {
        SchemaBuilder
    }

    pub fn struct_() -> Self {
        SchemaBuilder
    }

    pub fn field(&mut self, _name: &str, _schema: Schema) {}

    pub fn optional(&mut self) {}

    pub fn required(&mut self) {}

    pub fn name(&mut self, _name: &str) {}

    pub fn version(&mut self, _version: i32) {}

    pub fn doc(&mut self, _doc: &str) {}

    pub fn parameters(&mut self, _key: &str, _value: &str) {}

    pub fn default_value(&mut self) {}

    pub fn build(self) -> Schema {
        Schema(Arc::new(SchemaInner { name: None }))
    }
}

impl Schema {
    pub fn name(&self) -> Option<&str> {
        self.0.name.as_deref()
    }
}

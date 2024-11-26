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

use anyhow::Context;
use risingwave_pb::plan_common::ColumnDesc;

use super::avro::{avro_schema_to_column_descs, MapHandling};

impl crate::JsonSchema {
    /// FIXME: when the JSON schema is invalid, it will panic.
    ///
    /// ## Notes on type conversion
    /// Map will be used when an object doesn't have `properties` but has `additionalProperties`.
    /// When an object has `properties` and `additionalProperties`, the latter will be ignored.
    /// <https://github.com/mozilla/jsonschema-transpiler/blob/fb715c7147ebd52427e0aea09b2bba2d539850b1/src/jsonschema.rs#L228-L280>
    ///
    /// TODO: examine other stuff like `oneOf`, `patternProperties`, etc.
    pub fn json_schema_to_columns(&self) -> anyhow::Result<Vec<ColumnDesc>> {
        let avro_schema = jst::convert_avro(&self.0, jst::Context::default()).to_string();
        let schema =
            apache_avro::Schema::parse_str(&avro_schema).context("failed to parse avro schema")?;
        avro_schema_to_column_descs(&schema, Some(MapHandling::Jsonb)).map_err(Into::into)
    }
}

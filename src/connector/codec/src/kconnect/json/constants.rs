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

pub mod field_name {
    pub const SCHEMA_TYPE: &str = "type";
    pub const ARRAY_ITEMS: &str = "items";
    pub const MAP_KEY: &str = "keys";
    pub const MAP_VALUE: &str = "values";
    pub const STRUCT_FIELDS: &str = "fields";
    pub const STRUCT_FIELD_NAME: &str = "field";
    pub const SCHEMA_OPTIONAL: &str = "optional";
    pub const SCHEMA_NAME: &str = "name";
    pub const SCHEMA_VERSION: &str = "version";
    pub const SCHEMA_DOC: &str = "doc";
    pub const SCHEMA_PARAMETERS: &str = "parameters";
    pub const SCHEMA_DEFAULT: &str = "default";
}

pub mod type_name {
    pub const BOOLEAN: &str = "boolean";
    pub const INT8: &str = "int8";
    pub const INT16: &str = "int16";
    pub const INT32: &str = "int32";
    pub const INT64: &str = "int64";
    pub const FLOAT: &str = "float";
    pub const DOUBLE: &str = "double";
    pub const BYTES: &str = "bytes";
    pub const STRING: &str = "string";
    pub const ARRAY: &str = "array";
    pub const MAP: &str = "map";
    pub const STRUCT: &str = "struct";
}

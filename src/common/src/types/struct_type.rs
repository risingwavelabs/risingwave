// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::DataType;
use crate::array::ArrayMeta;

/// Details about a struct type. There are 2 cases for a struct:
/// 1. `field_names.len() == fields.len()`: it represents a struct with named fields, e.g.
///    `STRUCT<i INT, j VARCHAR>`.
/// 2. `field_names.len() == 0`: it represents a struct with unnamed fields, e.g.
///    `ROW(1, 2)`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StructType {
    pub fields: Vec<DataType>,
    pub field_names: Vec<String>,
}

impl StructType {
    pub fn new(named_fields: Vec<(DataType, String)>) -> Self {
        let mut fields = Vec::with_capacity(named_fields.len());
        let mut field_names = Vec::with_capacity(named_fields.len());
        for (d, s) in named_fields {
            fields.push(d);
            field_names.push(s);
        }
        Self {
            fields,
            field_names,
        }
    }

    pub fn to_array_meta(&self) -> ArrayMeta {
        ArrayMeta::Struct {
            children: self.fields.clone().into(),
        }
    }
}

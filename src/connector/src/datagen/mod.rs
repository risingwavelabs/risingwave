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

pub mod enumerator;
pub mod source;
pub mod split;

use std::collections::HashMap;

pub use enumerator::*;
use serde::Deserialize;
pub use source::*;
pub use split::*;

pub const DATAGEN_CONNECTOR: &str = "datagen";
#[derive(Clone, Debug, Deserialize)]
pub struct DatagenProperties {
    #[serde(
        rename = "datagen.max.chunk.size",
        default = "default_datagen_max_chunk_size"
    )]
    pub max_chunk_size: String,
    #[serde(
        rename = "datagen.rows.per.second",
        default = "default_rows_per_second"
    )]
    pub rows_per_second: String,
    // Some connector options of the datagen source's fields
    // for example: create datagen source with column v1 int, v2 float
    // 'fields.v1.kind'='sequence',
    // 'fields.v1.start'='1',
    // 'fields.v1.end'='1000',
    // 'fields.v1.kind'='random',
    // datagen will create v1 by self-incrementing from 1 to 1000
    // datagen will create v2 by randomly generating from default_min to default_max
    #[serde(flatten)]
    fields: HashMap<String, String>,
}

fn default_rows_per_second() -> String {
    "1".to_string()
}
fn default_datagen_max_chunk_size() -> String {
    "5".to_string()
}

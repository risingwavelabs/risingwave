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

pub mod enumerator;
pub mod source;
pub mod split;

use std::collections::HashMap;

pub use enumerator::*;
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
pub use source::*;
pub use split::*;

use crate::enforce_secret::EnforceSecret;
use crate::source::SourceProperties;

pub const DATAGEN_CONNECTOR: &str = "datagen";

#[serde_as]
#[derive(Clone, Debug, Deserialize, with_options::WithOptions)]
pub struct DatagenProperties {
    /// split_num means data source partition
    #[serde(rename = "datagen.split.num")]
    pub split_num: Option<String>,

    /// default_rows_per_second =10
    /// when the split_num = 3 and default_rows_per_second =10
    /// there will be three readers that generate respectively 4,3,3 message per second
    #[serde(
        rename = "datagen.rows.per.second",
        default = "default_rows_per_second"
    )]
    #[serde_as(as = "DisplayFromStr")]
    pub rows_per_second: u64,

    /// Some connector options of the datagen source's fields
    /// for example: create datagen source with column v1 int, v2 float
    /// 'fields.v1.kind'='sequence',
    /// 'fields.v1.start'='1',
    /// 'fields.v1.end'='1000',
    /// 'fields.v2.kind'='random',
    /// datagen will create v1 by self-incrementing from 1 to 1000
    /// datagen will create v2 by randomly generating from default_min to default_max
    #[serde(flatten)]
    pub fields: HashMap<String, String>,
}

impl EnforceSecret for DatagenProperties {}

impl SourceProperties for DatagenProperties {
    type Split = DatagenSplit;
    type SplitEnumerator = DatagenSplitEnumerator;
    type SplitReader = DatagenSplitReader;

    const SOURCE_NAME: &'static str = DATAGEN_CONNECTOR;
}

impl crate::source::UnknownFields for DatagenProperties {
    fn unknown_fields(&self) -> HashMap<String, String> {
        // FIXME: datagen does not handle unknown fields yet
        HashMap::new()
    }
}

fn default_rows_per_second() -> u64 {
    10
}

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

pub use enumerator::*;
use serde::Deserialize;
pub use source::*;
pub use split::*;

pub const CDC_CONNECTOR: &str = "cdc";

#[derive(Clone, Debug, Deserialize)]
pub struct CdcProperties {
    #[serde(default)]
    pub connector_node_addr: String,
    #[serde(default)]
    pub source_id: u32,
    #[serde(default)]
    pub start_offset: String,
    #[serde(default)]
    pub parititon: String,
    #[serde(rename = "database.name")]
    pub database_name: String,
    #[serde(rename = "table.name")]
    pub table_name: String,
    #[serde(rename = "database.hostname")]
    pub database_host: String,
    #[serde(rename = "database.port")]
    pub database_port: String,
    #[serde(rename = "database.user")]
    pub database_user: String,
    #[serde(rename = "database.password")]
    pub database_password: String,
}

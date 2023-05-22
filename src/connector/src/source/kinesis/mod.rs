// Copyright 2023 RisingWave Labs
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

pub mod config;
pub mod enumerator;
pub mod source;
pub mod split;

use serde::Deserialize;

use crate::common::KinesisCommon;

pub const KINESIS_CONNECTOR: &str = "kinesis";

#[derive(Clone, Debug, Deserialize)]
pub struct KinesisProperties {
    #[serde(rename = "scan.startup.mode", alias = "kinesis.scan.startup.mode")]
    // accepted values: "latest", "earliest", "sequence_number"
    pub scan_startup_mode: Option<String>,
    #[serde(
        rename = "scan.startup.sequence_number",
        alias = "kinesis.scan.startup.sequence_number"
    )]
    pub seq_offset: Option<String>,

    #[serde(
        rename = "enable.split.reduction",
        alias = "kinesis.enable.split.reduction"
    )]
    pub enable_split_reduction: Option<bool>,

    #[serde(flatten)]
    pub common: KinesisCommon,
}

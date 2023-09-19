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

pub mod enumerator;
pub mod source;
pub mod split;
pub mod topic;

pub use enumerator::*;
use serde::Deserialize;
pub use split::*;

use crate::common::PulsarCommon;
use crate::source::pulsar::source::reader::PulsarSplitReader;
use crate::source::SourceProperties;

pub const PULSAR_CONNECTOR: &str = "pulsar";

impl SourceProperties for PulsarProperties {
    type Split = PulsarSplit;
    type SplitEnumerator = PulsarSplitEnumerator;
    type SplitReader = PulsarSplitReader;

    const SOURCE_NAME: &'static str = PULSAR_CONNECTOR;
}

#[derive(Clone, Debug, Deserialize)]
pub struct PulsarProperties {
    #[serde(rename = "scan.startup.mode", alias = "pulsar.scan.startup.mode")]
    pub scan_startup_mode: Option<String>,

    #[serde(rename = "scan.startup.timestamp_millis", alias = "pulsar.time.offset")]
    pub time_offset: Option<String>,

    #[serde(flatten)]
    pub common: PulsarCommon,
}

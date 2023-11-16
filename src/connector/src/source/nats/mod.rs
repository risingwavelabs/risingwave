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

use serde::Deserialize;
use with_options::WithOptions;

use crate::common::NatsCommon;
use crate::source::nats::enumerator::NatsSplitEnumerator;
use crate::source::nats::source::{NatsSplit, NatsSplitReader};
use crate::source::SourceProperties;

pub const NATS_CONNECTOR: &str = "nats";

#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct NatsProperties {
    #[serde(flatten)]
    pub common: NatsCommon,

    #[serde(rename = "scan.startup.mode")]
    pub scan_startup_mode: Option<String>,

    #[serde(rename = "scan.startup.timestamp_millis")]
    pub start_time: Option<String>,

    #[serde(rename = "stream")]
    pub stream: String,
}

impl SourceProperties for NatsProperties {
    type Split = NatsSplit;
    type SplitEnumerator = NatsSplitEnumerator;
    type SplitReader = NatsSplitReader;

    const SOURCE_NAME: &'static str = NATS_CONNECTOR;
}

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

pub mod admin;
pub mod enumerator;
pub mod source;
pub mod split;
pub mod topic;

pub use enumerator::*;
use serde::Deserialize;
pub use split::*;

pub const PULSAR_CONNECTOR: &str = "pulsar";

#[derive(Clone, Debug, Deserialize)]
pub struct PulsarProperties {
    #[serde(rename = "topic", alias = "pulsar.topic")]
    pub topic: String,

    #[serde(rename = "admin.url", alias = "pulsar.admin.url")]
    pub admin_url: String,

    #[serde(rename = "service.url", alias = "pulsar.service.url")]
    pub service_url: String,

    #[serde(rename = "scan.startup.mode", alias = "pulsar.scan.startup.mode")]
    pub scan_startup_mode: Option<String>,

    #[serde(rename = "scan.startup.timestamp_millis", alias = "pulsar.time.offset")]
    pub time_offset: Option<String>,
}

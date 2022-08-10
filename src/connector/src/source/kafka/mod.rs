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

use std::time::Duration;

use serde::Deserialize;

pub mod enumerator;
pub mod source;
pub mod split;

pub use enumerator::*;
pub use source::*;
pub use split::*;

pub const KAFKA_CONNECTOR: &str = "kafka";

#[derive(Clone, Debug, Deserialize)]
pub struct KafkaProperties {
    #[serde(rename = "properties.bootstrap.server", alias = "kafka.brokers")]
    pub brokers: String,

    #[serde(rename = "topic", alias = "kafka.topic")]
    pub topic: String,

    #[serde(rename = "scan.startup.mode", alias = "kafka.scan.startup.mode")]
    pub scan_startup_mode: Option<String>,

    #[serde(rename = "scan.startup.timestamp_millis", alias = "kafka.time.offset")]
    pub time_offset: Option<String>,

    #[serde(rename = "properties.group.id", alias = "kafka.consumer.group")]
    pub consumer_group: Option<String>,
}

const KAFKA_SYNC_CALL_TIMEOUT: Duration = Duration::from_secs(1);

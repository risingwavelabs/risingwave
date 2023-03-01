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

use std::time::Duration;

use serde::Deserialize;

pub mod enumerator;
pub mod private_link;
pub mod source;
pub mod split;

pub use enumerator::*;
pub use private_link::*;
pub use source::*;
pub use split::*;

use crate::common::KafkaCommon;
pub const KAFKA_CONNECTOR: &str = "kafka";
pub const KAFKA_PROPS_BROKER_KEY: &str = "properties.bootstrap.server";
pub const KAFKA_PROPS_BROKER_KEY_ALIAS: &str = "kafka.brokers";

#[derive(Clone, Debug, Deserialize)]
pub struct KafkaProperties {
    /// This parameter is not intended to be exposed to users.
    /// This parameter specifies only for one parallelism. The parallelism of kafka source
    /// is equal to the parallelism passed into compute nodes. So users need to calculate
    /// how many bytes will be consumed in total across all the parallelism by themselves.
    #[serde(rename = "bytes.per.second", alias = "kafka.bytes.per.second")]
    pub bytes_per_second: Option<String>,

    /// This parameter is not intended to be exposed to users.
    /// This parameter specifies only for one parallelism. The parallelism of kafka source
    /// is equal to the parallelism passed into compute nodes. So users need to calculate
    /// how many messages will be consumed in total across all the parallelism by themselves.
    #[serde(rename = "max.num.messages", alias = "kafka.max.num.messages")]
    pub max_num_messages: Option<String>,

    #[serde(rename = "scan.startup.mode", alias = "kafka.scan.startup.mode")]
    pub scan_startup_mode: Option<String>,

    #[serde(rename = "scan.startup.timestamp_millis", alias = "kafka.time.offset")]
    pub time_offset: Option<String>,

    #[serde(rename = "properties.group.id", alias = "kafka.consumer.group")]
    pub consumer_group: Option<String>,

    /// This parameter is used to tell KafkaSplitReader to produce `UpsertMessage`s, which
    /// combine both key and value fields of the Kafka message.
    /// TODO: Currently, `Option<bool>` can not be parsed here.
    #[serde(rename = "upsert")]
    pub upsert: Option<String>,

    #[serde(flatten)]
    pub common: KafkaCommon,
}

const KAFKA_SYNC_CALL_TIMEOUT: Duration = Duration::from_secs(1);

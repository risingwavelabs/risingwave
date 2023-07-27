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
use serde_with::{serde_as, DisplayFromStr};

pub mod enumerator;
pub mod private_link;
pub mod source;
pub mod split;

pub use enumerator::*;
pub use private_link::*;
pub use source::*;
pub use split::*;

use crate::common::KafkaCommon;
use crate::deserialize_duration_from_string;
pub const KAFKA_CONNECTOR: &str = "kafka";
pub const KAFKA_PROPS_BROKER_KEY: &str = "properties.bootstrap.server";
pub const KAFKA_PROPS_BROKER_KEY_ALIAS: &str = "kafka.brokers";
pub const PRIVATELINK_CONNECTION: &str = "privatelink";

/// Properties for the rdkafka library. Leave a field as `None` to use the default value.
/// These properties are not intended to be exposed to users in the majority of cases.
///
/// See also <https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md>
#[serde_as]
#[derive(Clone, Debug, Deserialize)]
pub struct RdKafkaPropertiesConsumer {
    /// Minimum number of messages per topic+partition librdkafka tries to maintain in the local
    /// consumer queue.
    #[serde(rename = "properties.queued.min.messages")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub queued_min_messages: Option<usize>,

    #[serde(rename = "properties.queued.max.messages.kbytes")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub queued_max_messages_kbytes: Option<usize>,

    /// Maximum time the broker may wait to fill the Fetch response with `fetch.min.`bytes of
    /// messages.
    #[serde(rename = "properties.fetch.wait.max.ms")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub fetch_wait_max_ms: Option<usize>,

    /// How long to postpone the next fetch request for a topic+partition in case the current fetch
    /// queue thresholds (`queued.min.messages` or `queued.max.messages.kbytes`) have been
    /// exceeded. This property may need to be decreased if the queue thresholds are set low
    /// and the application is experiencing long (~1s) delays between messages. Low values may
    /// increase CPU utilization.
    // FIXME: need to upgrade rdkafka to v2.2.0 to use this property
    // #[serde(rename = "properties.fetch.queue.backoff.ms")]
    // #[serde_as(as = "Option<DisplayFromStr>")]
    // pub fetch_queue_backoff_ms: Option<usize>,

    /// Maximum amount of data the broker shall return for a Fetch request. Messages are fetched in
    /// batches by the consumer and if the first message batch in the first non-empty partition of
    /// the Fetch request is larger than this value, then the message batch will still be returned
    /// to ensure the consumer can make progress. The maximum message batch size accepted by the
    /// broker is defined via `message.max.bytes` (broker config) or `max.message.bytes` (broker
    /// topic config). `fetch.max.bytes` is automatically adjusted upwards to be at least
    /// `message.max.bytes` (consumer config).
    #[serde(rename = "properties.fetch.max.bytes")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub fetch_max_bytes: Option<usize>,

    /// Maximum Kafka protocol response message size. This serves as a safety precaution to avoid
    /// memory exhaustion in case of protocol hickups. This value must be at least
    /// `fetch.max.bytes` + 512 to allow for protocol overhead; the value is adjusted automatically
    /// unless the configuration property is explicitly set.
    #[serde(rename = "properties.receive.message.max.bytes")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub receive_message_max_bytes: Option<usize>,
}

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

    #[serde(
        rename = "properties.sync.call.timeout",
        deserialize_with = "deserialize_duration_from_string",
        default = "default_kafka_sync_call_timeout"
    )]
    pub sync_call_timeout: Duration,

    #[serde(flatten)]
    pub common: KafkaCommon,

    #[serde(flatten)]
    pub rdkafka_properties: RdKafkaPropertiesConsumer,
}

impl KafkaProperties {
    pub fn set_client(&self, c: &mut rdkafka::ClientConfig) {
        self.common.set_client(c);
        self.rdkafka_properties.set_client(c);

        tracing::info!("kafka client starts with: {:?}", c);
    }
}

const fn default_kafka_sync_call_timeout() -> Duration {
    Duration::from_secs(5)
}
const KAFKA_ISOLATION_LEVEL: &str = "read_committed";

impl RdKafkaPropertiesConsumer {
    pub fn set_client(&self, c: &mut rdkafka::ClientConfig) {
        if let Some(v) = &self.queued_min_messages {
            c.set("queued.min.messages", v.to_string());
        }
        if let Some(v) = &self.queued_max_messages_kbytes {
            c.set("queued.max.messages.kbytes", v.to_string());
        }
        if let Some(v) = &self.fetch_wait_max_ms {
            c.set("fetch.wait.max.ms", v.to_string());
        }
        // if let Some(v) = &self.fetch_queue_backoff_ms {
        //     c.set("fetch.queue.backoff.ms", v.to_string());
        // }
        if let Some(v) = &self.fetch_max_bytes {
            c.set("fetch.max.bytes", v.to_string());
        }
        if let Some(v) = &self.receive_message_max_bytes {
            c.set("receive.message.max.bytes", v.to_string());
        }
    }
}

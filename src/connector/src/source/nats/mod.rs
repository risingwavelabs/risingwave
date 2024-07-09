// Copyright 2024 RisingWave Labs
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

use serde::Deserialize;
use with_options::WithOptions;

use crate::connector_common::NatsCommon;
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

    #[serde(
        rename = "scan.startup.timestamp.millis",
        alias = "scan.startup.timestamp_millis"
    )]
    pub start_time: Option<String>,

    #[serde(rename = "stream")]
    pub stream: String,

    /// Setting `durable_name` to `Some(...)` will cause this consumer
    /// to be "durable". This may be a good choice for workloads that
    /// benefit from the `JetStream` server or cluster remembering the
    /// progress of consumers for fault tolerance purposes. If a consumer
    /// crashes, the `JetStream` server or cluster will remember which
    /// messages the consumer acknowledged. When the consumer recovers,
    /// this information will allow the consumer to resume processing
    /// where it left off. If you're unsure, set this to `Some(...)`.
    ///
    /// Setting `durable_name` to `None` will cause this consumer to
    /// be "ephemeral". This may be a good choice for workloads where
    /// you don't need the `JetStream` server to remember the consumer's
    /// progress in the case of a crash, such as certain "high churn"
    /// workloads or workloads where a crashed instance is not required
    /// to recover.
    #[serde(rename = "consumer.durable_name")]
    pub durable_name: Option<String>,

    /// A short description of the purpose of this consumer.
    #[serde(rename = "consumer.description")]
    pub description: Option<String>,

    /// How messages should be acknowledged
    #[serde(rename = "consumer.ack_policy")]
    pub ack_policy: Option<String>,

    /// How long to allow messages to remain un-acknowledged before attempting redelivery
    #[serde(rename = "consumer.ack_wait")]
    pub ack_wait: Option<String>,

    /// Maximum number of times a specific message will be delivered. Use this to avoid poison pill messages that repeatedly crash your consumer processes forever.
    #[serde(rename = "consumer.max_deliver")]
    pub max_deliver: Option<String>,

    /// When consuming from a Stream with many subjects, or wildcards, this selects only specific incoming subjects. Supports wildcards.
    #[serde(rename = "consumer.filter_subject")]
    pub filter_subject: Option<String>,

    /// Fulfills the same role as [Config::filter_subject], but allows filtering by many subjects.
    #[serde(rename = "consumer.filter_subjects")]
    pub filter_subjects: Option<String>,

    /// Whether messages are sent as quickly as possible or at the rate of receipt
    #[serde(rename = "consumer.replay_policy")]
    pub replay_policy: Option<String>,

    /// The rate of message delivery in bits per second
    #[serde(rename = "consumer.rate_limit")]
    pub rate_limit: Option<String>,

    /// What percentage of acknowledgments should be samples for observability, 0-100
    #[serde(rename = "consumer.sample_frequency")]
    pub sample_frequency: Option<String>,

    /// The maximum number of waiting consumers.
    #[serde(rename = "consumer.max_waiting")]
    pub max_waiting: Option<String>,

    /// The maximum number of unacknowledged messages that may be
    /// in-flight before pausing sending additional messages to
    /// this consumer.
    #[serde(rename = "consumer.max_ack_pending")]
    pub max_ack_pending: Option<String>,

    /// The maximum number of unacknowledged messages that may be
    /// in-flight before pausing sending additional messages to
    /// this consumer.
    #[serde(rename = "consumer.idle_heartbeat")]
    pub idle_heartbeat: Option<String>,

    /// Maximum size of a request batch
    #[serde(rename = "consumer.max_batch")]
    pub max_batch: Option<String>,

    // / Maximum value of request max_bytes
    #[serde(rename = "consumer.max_bytes")]
    pub max_bytes: Option<String>,

    /// Maximum value for request expiration
    #[serde(rename = "consumer.max_expires")]
    pub max_expires: Option<String>,

    /// Threshold for consumer inactivity
    #[serde(rename = "consumer.inactive_threshold")]
    pub inactive_threshold: Option<String>,

    /// Number of consumer replicas
    #[serde(rename = "consumer.num.replicas", alias = "consumer.num_replicas")]
    pub num_replicas: Option<String>,

    /// Force consumer to use memory storage.
    #[serde(rename = "consumer.memory_storage")]
    pub memory_storage: Option<String>,

    /// Custom backoff for missed acknowledgments.
    #[serde(rename = "consumer.backoff")]
    pub backoff: Option<String>,

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

impl SourceProperties for NatsProperties {
    type Split = NatsSplit;
    type SplitEnumerator = NatsSplitEnumerator;
    type SplitReader = NatsSplitReader;

    const SOURCE_NAME: &'static str = NATS_CONNECTOR;
}

impl crate::source::UnknownFields for NatsProperties {
    fn unknown_fields(&self) -> HashMap<String, String> {
        self.unknown_fields.clone()
    }
}

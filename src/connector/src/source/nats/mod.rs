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
use std::time::Duration;

use async_nats::jetstream::consumer::pull::Config;
use async_nats::jetstream::consumer::{AckPolicy, ReplayPolicy};
use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
use with_options::WithOptions;

use crate::connector_common::NatsCommon;
use crate::source::nats::enumerator::NatsSplitEnumerator;
use crate::source::nats::source::{NatsSplit, NatsSplitReader};
use crate::source::SourceProperties;
use crate::{
    deserialize_optional_string_seq_from_string, deserialize_optional_u64_seq_from_string,
};

pub const NATS_CONNECTOR: &str = "nats";

pub struct AckPolicyWrapper;

impl AckPolicyWrapper {
    pub fn parse_str(s: &str) -> Result<AckPolicy, String> {
        match s {
            "none" => Ok(AckPolicy::None),
            "all" => Ok(AckPolicy::All),
            "explicit" => Ok(AckPolicy::Explicit),
            _ => Err(format!("Invalid AckPolicy '{}'", s)),
        }
    }
}

pub struct ReplayPolicyWrapper;

impl ReplayPolicyWrapper {
    pub fn parse_str(s: &str) -> Result<ReplayPolicy, String> {
        match s {
            "instant" => Ok(ReplayPolicy::Instant),
            "original" => Ok(ReplayPolicy::Original),
            _ => Err(format!("Invalid ReplayPolicy '{}'", s)),
        }
    }
}

#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct NatsProperties {
    #[serde(flatten)]
    pub common: NatsCommon,

    #[serde(flatten)]
    pub nats_properties_consumer: NatsPropertiesConsumer,

    #[serde(rename = "scan.startup.mode")]
    pub scan_startup_mode: Option<String>,

    #[serde(
        rename = "scan.startup.timestamp.millis",
        alias = "scan.startup.timestamp_millis"
    )]
    pub start_time: Option<String>,

    #[serde(rename = "stream")]
    pub stream: String,

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

impl NatsProperties {
    pub fn set_config(&self, c: &mut Config) {
        self.nats_properties_consumer.set_config(c);
    }
}

/// Properties for the async-nats library.
/// See <https://docs.rs/async-nats/latest/async_nats/jetstream/consumer/struct.Config.html>
#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct NatsPropertiesConsumer {
    #[serde(rename = "consumer.deliver_subject")]
    pub deliver_subject: Option<String>,

    #[serde(rename = "consumer.durable_name")]
    pub durable_name: Option<String>,

    #[serde(rename = "consumer.name")]
    pub name: Option<String>,

    #[serde(rename = "consumer.description")]
    pub description: Option<String>,

    #[serde(rename = "consumer.deliver_policy")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub deliver_policy: Option<String>,

    #[serde(rename = "consumer.ack_policy")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub ack_policy: Option<String>,

    #[serde(rename = "consumer.ack_wait.sec")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub ack_wait: Option<u64>,

    #[serde(rename = "consumer.max_deliver")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub max_deliver: Option<i64>,

    #[serde(rename = "consumer.filter_subject")]
    pub filter_subject: Option<String>,

    #[serde(
        rename = "consumer.filter_subjects",
        default,
        deserialize_with = "deserialize_optional_string_seq_from_string"
    )]
    pub filter_subjects: Option<Vec<String>>,

    #[serde(rename = "consumer.replay_policy")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub replay_policy: Option<String>,

    #[serde(rename = "consumer.rate_limit")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub rate_limit: Option<u64>,

    #[serde(rename = "consumer.sample_frequency")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub sample_frequency: Option<u8>,

    #[serde(rename = "consumer.max_waiting")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub max_waiting: Option<i64>,

    #[serde(rename = "consumer.max_ack_pending")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub max_ack_pending: Option<i64>,

    #[serde(rename = "consumer.headers_only")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub headers_only: Option<bool>,

    #[serde(rename = "consumer.max_batch")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub max_batch: Option<i64>,

    #[serde(rename = "consumer.max_bytes")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub max_bytes: Option<i64>,

    #[serde(rename = "consumer.max_expires.sec")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub max_expires: Option<u64>,

    #[serde(rename = "consumer.inactive_threshold.sec")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub inactive_threshold: Option<u64>,

    #[serde(rename = "consumer.num.replicas", alias = "consumer.num_replicas")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub num_replicas: Option<usize>,

    #[serde(rename = "consumer.memory_storage")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub memory_storage: Option<bool>,

    #[serde(
        rename = "consumer.backoff.sec",
        default,
        deserialize_with = "deserialize_optional_u64_seq_from_string"
    )]
    pub backoff: Option<Vec<u64>>,
}

impl NatsPropertiesConsumer {
    pub fn set_config(&self, c: &mut Config) {
        if let Some(v) = &self.name {
            c.name = Some(v.clone())
        }
        if let Some(v) = &self.durable_name {
            c.durable_name = Some(v.clone())
        }
        if let Some(v) = &self.description {
            c.description = Some(v.clone())
        }
        if let Some(v) = &self.ack_policy {
            c.ack_policy = AckPolicyWrapper::parse_str(v).unwrap()
        }
        if let Some(v) = &self.ack_wait {
            c.ack_wait = Duration::from_secs(*v)
        }
        if let Some(v) = &self.max_deliver {
            c.max_deliver = *v
        }
        if let Some(v) = &self.filter_subject {
            c.filter_subject = v.clone()
        }
        if let Some(v) = &self.filter_subjects {
            c.filter_subjects = v.clone()
        }
        if let Some(v) = &self.replay_policy {
            c.replay_policy = ReplayPolicyWrapper::parse_str(v).unwrap()
        }
        if let Some(v) = &self.rate_limit {
            c.rate_limit = *v
        }
        if let Some(v) = &self.sample_frequency {
            c.sample_frequency = *v
        }
        if let Some(v) = &self.max_waiting {
            c.max_waiting = *v
        }
        if let Some(v) = &self.max_ack_pending {
            c.max_ack_pending = *v
        }
        if let Some(v) = &self.headers_only {
            c.headers_only = *v
        }
        if let Some(v) = &self.max_batch {
            c.max_batch = *v
        }
        if let Some(v) = &self.max_bytes {
            c.max_bytes = *v
        }
        if let Some(v) = &self.max_expires {
            c.max_expires = Duration::from_secs(*v)
        }
        if let Some(v) = &self.inactive_threshold {
            c.inactive_threshold = Duration::from_secs(*v)
        }
        if let Some(v) = &self.num_replicas {
            c.num_replicas = *v
        }
        if let Some(v) = &self.memory_storage {
            c.memory_storage = *v
        }
        if let Some(v) = &self.backoff {
            c.backoff = v.iter().map(|&x| Duration::from_secs(x)).collect()
        }
    }
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

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use maplit::btreemap;

    use super::*;

    #[test]
    fn test_parse_config_consumer() {
        let config: BTreeMap<String, String> = btreemap! {
            "stream".to_string() => "risingwave".to_string(),

            // NATS common
            "subject".to_string() => "subject1".to_string(),
            "server_url".to_string() => "nats-server:4222".to_string(),
            "connect_mode".to_string() => "plain".to_string(),
            "type".to_string() => "append-only".to_string(),

            // NATS properties consumer
            "consumer.name".to_string() => "foobar".to_string(),
            "consumer.durable_name".to_string() => "durable_foobar".to_string(),
            "consumer.description".to_string() => "A description".to_string(),
            "consumer.ack_policy".to_string() => "all".to_string(),
            "consumer.ack_wait.sec".to_string() => "10".to_string(),
            "consumer.max_deliver".to_string() => "10".to_string(),
            "consumer.filter_subject".to_string() => "subject".to_string(),
            "consumer.filter_subjects".to_string() => "subject1,subject2".to_string(),
            "consumer.replay_policy".to_string() => "instant".to_string(),
            "consumer.rate_limit".to_string() => "100".to_string(),
            "consumer.sample_frequency".to_string() => "1".to_string(),
            "consumer.max_waiting".to_string() => "5".to_string(),
            "consumer.max_ack_pending".to_string() => "100".to_string(),
            "consumer.headers_only".to_string() => "true".to_string(),
            "consumer.max_batch".to_string() => "10".to_string(),
            "consumer.max_bytes".to_string() => "1024".to_string(),
            "consumer.max_expires.sec".to_string() => "24".to_string(),
            "consumer.inactive_threshold.sec".to_string() => "10".to_string(),
            "consumer.num_replicas".to_string() => "3".to_string(),
            "consumer.memory_storage".to_string() => "true".to_string(),
            "consumer.backoff.sec".to_string() => "2,10,15".to_string(),

        };

        let props: NatsProperties =
            serde_json::from_value(serde_json::to_value(config).unwrap()).unwrap();

        assert_eq!(
            props.nats_properties_consumer.name,
            Some("foobar".to_string())
        );
        assert_eq!(
            props.nats_properties_consumer.durable_name,
            Some("durable_foobar".to_string())
        );
        assert_eq!(
            props.nats_properties_consumer.description,
            Some("A description".to_string())
        );
        assert_eq!(
            props.nats_properties_consumer.ack_policy,
            Some("all".to_string())
        );
        assert_eq!(props.nats_properties_consumer.ack_wait, Some(10));
        assert_eq!(
            props.nats_properties_consumer.filter_subjects,
            Some(vec!["subject1".to_string(), "subject2".to_string()])
        );
        assert_eq!(
            props.nats_properties_consumer.replay_policy,
            Some("instant".to_string())
        );
        assert_eq!(props.nats_properties_consumer.rate_limit, Some(100));
        assert_eq!(props.nats_properties_consumer.sample_frequency, Some(1));
        assert_eq!(props.nats_properties_consumer.max_waiting, Some(5));
        assert_eq!(props.nats_properties_consumer.max_ack_pending, Some(100));
        assert_eq!(props.nats_properties_consumer.headers_only, Some(true));
        assert_eq!(props.nats_properties_consumer.max_batch, Some(10));
        assert_eq!(props.nats_properties_consumer.max_bytes, Some(1024));
        assert_eq!(props.nats_properties_consumer.max_expires, Some(24));
        assert_eq!(props.nats_properties_consumer.inactive_threshold, Some(10));
        assert_eq!(props.nats_properties_consumer.num_replicas, Some(3));
        assert_eq!(props.nats_properties_consumer.memory_storage, Some(true));
        assert_eq!(
            props.nats_properties_consumer.backoff,
            Some(vec![2, 10, 15])
        );
    }
}

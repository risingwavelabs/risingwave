// Copyright 2025 RisingWave Labs
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
pub use enumerator::NatsSplitEnumerator;
pub mod source;
pub mod split;

use std::collections::HashMap;
use std::fmt::Display;
use std::time::Duration;

use async_nats::jetstream::consumer::pull::Config;
use async_nats::jetstream::consumer::{AckPolicy, ReplayPolicy};
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use thiserror::Error;
use with_options::WithOptions;

use crate::connector_common::NatsCommon;
use crate::enforce_secret_on_cloud::EnforceSecretOnCloud;
use crate::error::{ConnectorError, ConnectorResult};
use crate::source::SourceProperties;
use crate::source::nats::source::{NatsSplit, NatsSplitReader};
use crate::{
    deserialize_optional_string_seq_from_string, deserialize_optional_u64_seq_from_string,
};

#[derive(Debug, Clone, Error)]
pub struct NatsJetStreamError(String);

impl Display for NatsJetStreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub const NATS_CONNECTOR: &str = "nats";

pub struct AckPolicyWrapper;

impl AckPolicyWrapper {
    pub fn parse_str(s: &str) -> Result<AckPolicy, NatsJetStreamError> {
        match s {
            "none" => Ok(AckPolicy::None),
            "all" => Ok(AckPolicy::All),
            "explicit" => Ok(AckPolicy::Explicit),
            _ => Err(NatsJetStreamError(format!(
                "Invalid AckPolicy '{}', expect `none`, `all`, and `explicit`",
                s
            ))),
        }
    }
}

pub struct ReplayPolicyWrapper;

impl ReplayPolicyWrapper {
    pub fn parse_str(s: &str) -> Result<ReplayPolicy, NatsJetStreamError> {
        match s {
            "instant" => Ok(ReplayPolicy::Instant),
            "original" => Ok(ReplayPolicy::Original),
            _ => Err(NatsJetStreamError(format!(
                "Invalid ReplayPolicy '{}', expect `instant` and `original`",
                s
            ))),
        }
    }
}

#[serde_as]
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
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub start_timestamp_millis: Option<i64>,

    #[serde(rename = "stream")]
    pub stream: String,

    #[serde(rename = "consumer.durable_name")]
    pub durable_consumer_name: String,

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

impl EnforceSecretOnCloud for NatsProperties {
    fn enforce_secret_on_cloud<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> ConnectorResult<()> {
        for prop in prop_iter {
            NatsCommon::enforce_one(prop)?;
        }
        Ok(())
    }
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

    pub fn get_ack_policy(&self) -> ConnectorResult<AckPolicy> {
        match &self.ack_policy {
            Some(policy) => Ok(AckPolicyWrapper::parse_str(policy).map_err(ConnectorError::from)?),
            None => Ok(AckPolicy::None),
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
            "stream".to_owned() => "risingwave".to_owned(),

            // NATS common
            "subject".to_owned() => "subject1".to_owned(),
            "server_url".to_owned() => "nats-server:4222".to_owned(),
            "connect_mode".to_owned() => "plain".to_owned(),
            "type".to_owned() => "append-only".to_owned(),

            // NATS properties consumer
            "consumer.name".to_owned() => "foobar".to_owned(),
            "consumer.durable_name".to_owned() => "durable_foobar".to_owned(),
            "consumer.description".to_owned() => "A description".to_owned(),
            "consumer.ack_policy".to_owned() => "all".to_owned(),
            "consumer.ack_wait.sec".to_owned() => "10".to_owned(),
            "consumer.max_deliver".to_owned() => "10".to_owned(),
            "consumer.filter_subject".to_owned() => "subject".to_owned(),
            "consumer.filter_subjects".to_owned() => "subject1,subject2".to_owned(),
            "consumer.replay_policy".to_owned() => "instant".to_owned(),
            "consumer.rate_limit".to_owned() => "100".to_owned(),
            "consumer.sample_frequency".to_owned() => "1".to_owned(),
            "consumer.max_waiting".to_owned() => "5".to_owned(),
            "consumer.max_ack_pending".to_owned() => "100".to_owned(),
            "consumer.headers_only".to_owned() => "true".to_owned(),
            "consumer.max_batch".to_owned() => "10".to_owned(),
            "consumer.max_bytes".to_owned() => "1024".to_owned(),
            "consumer.max_expires.sec".to_owned() => "24".to_owned(),
            "consumer.inactive_threshold.sec".to_owned() => "10".to_owned(),
            "consumer.num_replicas".to_owned() => "3".to_owned(),
            "consumer.memory_storage".to_owned() => "true".to_owned(),
            "consumer.backoff.sec".to_owned() => "2,10,15".to_owned(),
            "durable_consumer_name".to_owned() => "test_durable_consumer".to_owned(),

        };

        let props: NatsProperties =
            serde_json::from_value(serde_json::to_value(config).unwrap()).unwrap();

        assert_eq!(
            props.nats_properties_consumer.name,
            Some("foobar".to_owned())
        );
        assert_eq!(props.durable_consumer_name, "durable_foobar".to_owned());
        assert_eq!(
            props.nats_properties_consumer.description,
            Some("A description".to_owned())
        );
        assert_eq!(
            props.nats_properties_consumer.ack_policy,
            Some("all".to_owned())
        );
        assert_eq!(props.nats_properties_consumer.ack_wait, Some(10));
        assert_eq!(
            props.nats_properties_consumer.filter_subjects,
            Some(vec!["subject1".to_owned(), "subject2".to_owned()])
        );
        assert_eq!(
            props.nats_properties_consumer.replay_policy,
            Some("instant".to_owned())
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

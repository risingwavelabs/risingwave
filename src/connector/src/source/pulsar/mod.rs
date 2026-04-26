// Copyright 2022 RisingWave Labs
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

use std::collections::HashMap;
use std::time::Duration;

pub use enumerator::*;
use pulsar::OperationRetryOptions;
use serde::{Deserialize, de};
use serde_with::serde_as;
pub use split::*;
use with_options::WithOptions;

use self::source::reader::PulsarSplitReader;
use crate::connector_common::{AwsAuthProps, PulsarCommon, PulsarOauthCommon};
use crate::enforce_secret::EnforceSecret;
use crate::error::ConnectorError;
use crate::source::SourceProperties;
use crate::{deserialize_optional_bool_from_string, deserialize_optional_duration_from_string};

pub const PULSAR_CONNECTOR: &str = "pulsar";

impl SourceProperties for PulsarProperties {
    type Split = PulsarSplit;
    type SplitEnumerator = PulsarSplitEnumerator;
    type SplitReader = PulsarSplitReader;

    const SOURCE_NAME: &'static str = PULSAR_CONNECTOR;
}

impl crate::source::UnknownFields for PulsarProperties {
    fn unknown_fields(&self) -> HashMap<String, String> {
        self.unknown_fields.clone()
    }
}

impl EnforceSecret for PulsarProperties {
    fn enforce_secret<'a>(prop_iter: impl Iterator<Item = &'a str>) -> Result<(), ConnectorError> {
        for prop in prop_iter {
            PulsarCommon::enforce_one(prop)?;
        }
        Ok(())
    }
}

impl EnforceSecret for PulsarConsumerOptions {}

#[derive(Clone, Debug, Deserialize, WithOptions)]
#[serde_as]
pub struct PulsarConsumerOptions {
    #[serde(
        rename = "pulsar.read_compacted",
        default,
        deserialize_with = "deserialize_optional_bool_from_string"
    )]
    pub read_compacted: Option<bool>,
}

#[derive(Clone, Debug, Default, Deserialize, WithOptions)]
#[serde_as]
pub struct PulsarSourceOperationRetry {
    #[serde(
        rename = "pulsar.operation.retry.max.retries",
        deserialize_with = "deserialize_optional_u32_from_string",
        default
    )]
    pub max_retries: Option<u32>,

    /// This controls Pulsar client operation retry delay for the source.
    /// It is distinct from `subscription.unacked.resend.delay`, which only affects
    /// unacked message redelivery, and from sink-side `properties.retry.*` settings.
    #[serde(
        rename = "pulsar.operation.retry.delay",
        deserialize_with = "deserialize_optional_duration_from_string",
        default
    )]
    pub retry_delay: Option<Duration>,
}

impl PulsarSourceOperationRetry {
    pub(crate) fn to_pulsar_options(&self) -> Option<OperationRetryOptions> {
        if self.max_retries.is_none() && self.retry_delay.is_none() {
            return None;
        }

        let mut options = OperationRetryOptions::default();
        options.max_retries = self.max_retries;
        if let Some(retry_delay) = self.retry_delay {
            options.retry_delay = retry_delay;
        }

        Some(options)
    }
}

fn deserialize_optional_u32_from_string<'de, D>(deserializer: D) -> Result<Option<u32>, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s: Option<String> = de::Deserialize::deserialize(deserializer)?;
    if let Some(s) = s {
        let parsed = s.parse().map_err(|_| {
            de::Error::invalid_value(
                de::Unexpected::Str(&s),
                &"integer greater than or equal to 0",
            )
        })?;
        Ok(Some(parsed))
    } else {
        Ok(None)
    }
}

#[doc(hidden)]
pub fn make_retryable_pulsar_connector_error(message: impl Into<String>) -> ConnectorError {
    pulsar::Error::Connection(pulsar::error::ConnectionError::Io(std::io::Error::new(
        std::io::ErrorKind::ConnectionReset,
        message.into(),
    )))
    .into()
}

#[derive(Clone, Debug, Deserialize, WithOptions)]
#[serde_as]
pub struct PulsarProperties {
    #[serde(rename = "scan.startup.mode", alias = "pulsar.scan.startup.mode")]
    pub scan_startup_mode: Option<String>,

    #[serde(
        rename = "scan.startup.timestamp.millis",
        alias = "pulsar.time.offset",
        alias = "scan.startup.timestamp_millis"
    )]
    pub time_offset: Option<String>,

    #[serde(flatten)]
    pub common: PulsarCommon,

    #[serde(flatten)]
    pub oauth: Option<PulsarOauthCommon>,

    #[serde(flatten)]
    pub aws_auth_props: AwsAuthProps,

    #[serde(rename = "iceberg.enabled")]
    #[serde_as(as = "DisplayFromStr")]
    pub iceberg_loader_enabled: Option<bool>,

    #[serde(rename = "iceberg.bucket", default)]
    pub iceberg_bucket: Option<String>,

    /// Specify a custom consumer group id prefix for the source.
    /// Defaults to `rw-consumer`.
    ///
    /// Notes:
    /// - Each job (materialized view) will have multiple subscriptions and
    ///   contains a generated suffix in the subscription name.
    ///   The subscription name will be `{subscription_name_prefix}-{fragment_id}-{actor_id}`.
    #[serde(rename = "subscription.name.prefix")]
    pub subscription_name_prefix: Option<String>,

    #[serde(
        rename = "subscription.unacked.resend.delay",
        deserialize_with = "deserialize_optional_duration_from_string",
        default
    )]
    pub subscription_unacked_resend_delay: Option<Duration>,

    #[serde(flatten)]
    pub operation_retry: PulsarSourceOperationRetry,

    #[serde(flatten)]
    pub consumer_options: PulsarConsumerOptions,

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn parse_pulsar_properties(extra: serde_json::Value) -> PulsarProperties {
        let mut value = json!({
            "topic": "persistent://public/default/test-topic",
            "service.url": "pulsar://localhost:6650",
        });

        value
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());

        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn test_operation_retry_override_uses_upstream_defaults_when_absent() {
        let props = parse_pulsar_properties(json!({}));
        assert!(props.operation_retry.to_pulsar_options().is_none());
    }

    #[test]
    fn test_operation_retry_override_sets_max_retries_only() {
        let props = parse_pulsar_properties(json!({"pulsar.operation.retry.max.retries": "7"}));

        let options = props.operation_retry.to_pulsar_options().unwrap();
        assert_eq!(options.max_retries, Some(7));
        assert_eq!(
            options.retry_delay,
            OperationRetryOptions::default().retry_delay
        );
    }

    #[test]
    fn test_operation_retry_override_sets_max_retries_and_delay() {
        let props = parse_pulsar_properties(json!({
            "pulsar.operation.retry.max.retries": "9",
            "pulsar.operation.retry.delay": "3s",
        }));

        let options = props.operation_retry.to_pulsar_options().unwrap();
        assert_eq!(options.max_retries, Some(9));
        assert_eq!(options.retry_delay, Duration::from_secs(3));
    }
}

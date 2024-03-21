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

use std::collections::HashMap;

use serde::Deserialize;

pub mod enumerator;
pub mod source;
pub mod split;

pub use enumerator::*;
pub use source::*;
pub use split::*;
use with_options::WithOptions;

use crate::source::SourceProperties;

pub const GOOGLE_PUBSUB_CONNECTOR: &str = "google_pubsub";

#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct PubsubProperties {
    /// pubsub subscription to consume messages from
    /// The subscription should be configured with the `retain-on-ack` property to enable
    /// message recovery within risingwave.
    #[serde(rename = "pubsub.subscription")]
    pub subscription: String,

    /// use the connector with a pubsub emulator
    /// <https://cloud.google.com/pubsub/docs/emulator>
    #[serde(rename = "pubsub.emulator_host")]
    pub emulator_host: Option<String>,

    /// `credentials` is a JSON string containing the service account credentials.
    /// See the [service-account credentials guide](https://developers.google.com/workspace/guides/create-credentials#create_credentials_for_a_service_account).
    /// The service account must have the `pubsub.subscriber` [role](https://cloud.google.com/pubsub/docs/access-control#roles).
    #[serde(rename = "pubsub.credentials")]
    pub credentials: Option<String>,

    /// `start_offset` is a numeric timestamp, ideally the publish timestamp of a message
    /// in the subscription. If present, the connector will attempt to seek the subscription
    /// to the timestamp and start consuming from there. Note that the seek operation is
    /// subject to limitations around the message retention policy of the subscription. See
    /// [Seeking to a timestamp](https://cloud.google.com/pubsub/docs/replay-overview#seeking_to_a_timestamp) for
    /// more details.
    #[serde(rename = "pubsub.start_offset.nanos")]
    pub start_offset: Option<String>,

    /// `start_snapshot` is a named pub/sub snapshot. If present, the connector will first seek
    /// to the snapshot before starting consumption. Snapshots are the preferred seeking mechanism
    /// in pub/sub because they guarantee retention of:
    /// - All unacknowledged messages at the time of their creation.
    /// - All messages created after their creation.
    /// Besides retention guarantees, timestamps are also more precise than timestamp-based seeks.
    /// See [Seeking to a snapshot](https://cloud.google.com/pubsub/docs/replay-overview#seeking_to_a_timestamp) for
    /// more details.
    #[serde(rename = "pubsub.start_snapshot")]
    pub start_snapshot: Option<String>,

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

impl SourceProperties for PubsubProperties {
    type Split = PubsubSplit;
    type SplitEnumerator = PubsubSplitEnumerator;
    type SplitReader = PubsubSplitReader;

    const SOURCE_NAME: &'static str = GOOGLE_PUBSUB_CONNECTOR;
}

impl crate::source::UnknownFields for PubsubProperties {
    fn unknown_fields(&self) -> HashMap<String, String> {
        self.unknown_fields.clone()
    }
}

impl PubsubProperties {
    /// `initialize_env` sets environment variables read by the `google-cloud-pubsub` crate
    pub(crate) fn initialize_env(&self) {
        tracing::debug!("setting pubsub environment variables");
        if let Some(emulator_host) = &self.emulator_host {
            std::env::set_var("PUBSUB_EMULATOR_HOST", emulator_host);
        }
        if let Some(credentials) = &self.credentials {
            std::env::set_var("GOOGLE_APPLICATION_CREDENTIALS_JSON", credentials);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ConnectorResult as Result;

    const EMULATOR_HOST: &str = "localhost:8081";
    const CREDENTIALS: &str = "{}";

    const PUBSUB_EMULATOR_HOST: &str = "PUBSUB_EMULATOR_HOST";
    const GOOGLE_APPLICATION_CREDENTIALS_JSON: &str = "GOOGLE_APPLICATION_CREDENTIALS_JSON";

    fn reset_env() {
        std::env::set_var(PUBSUB_EMULATOR_HOST, "");
        std::env::set_var(GOOGLE_APPLICATION_CREDENTIALS_JSON, "");
    }

    #[test]
    pub fn initialize_env() -> Result<()> {
        let default_properties = PubsubProperties {
            credentials: None,
            emulator_host: None,
            start_offset: None,
            start_snapshot: None,
            subscription: String::from("test-subscription"),

            unknown_fields: Default::default(),
        };

        let properties = PubsubProperties {
            emulator_host: Some(EMULATOR_HOST.into()),
            ..default_properties.clone()
        };

        reset_env();
        properties.initialize_env();
        assert_eq!(
            std::env::var(PUBSUB_EMULATOR_HOST)
                .unwrap_or_else(|_| panic!("{} not set in env", PUBSUB_EMULATOR_HOST))
                .as_str(),
            EMULATOR_HOST,
        );

        let properties = PubsubProperties {
            credentials: Some(CREDENTIALS.into()),
            ..default_properties
        };

        reset_env();
        properties.initialize_env();
        assert_eq!(
            std::env::var(GOOGLE_APPLICATION_CREDENTIALS_JSON)
                .unwrap_or_else(|_| panic!(
                    "{} not set in env",
                    GOOGLE_APPLICATION_CREDENTIALS_JSON
                ))
                .as_str(),
            CREDENTIALS
        );

        Ok(())
    }
}

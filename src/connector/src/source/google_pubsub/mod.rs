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

use std::collections::HashMap;

use anyhow::Context;
use google_cloud_gax::conn::Environment;
use google_cloud_pubsub::apiv1;
use google_cloud_pubsub::client::google_cloud_auth::credentials::CredentialsFile;
use google_cloud_pubsub::client::google_cloud_auth::project;
use google_cloud_pubsub::client::google_cloud_auth::token::DefaultTokenSourceProvider;
use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_pubsub::subscriber::SubscriberConfig;
use google_cloud_pubsub::subscription::Subscription;
use risingwave_common::bail;
use risingwave_common::util::env_var::env_var_is_true;
use serde::Deserialize;

pub mod enumerator;
pub mod source;
pub mod split;

pub use enumerator::*;
use phf::{Set, phf_set};
use serde_with::{DisplayFromStr, serde_as};
pub use source::*;
pub use split::*;
use with_options::WithOptions;

use crate::connector_common::{DISABLE_DEFAULT_CREDENTIAL, resolve_pubsub_project_id};
use crate::enforce_secret::EnforceSecret;
use crate::error::ConnectorResult;
use crate::source::SourceProperties;

pub const GOOGLE_PUBSUB_CONNECTOR: &str = "google_pubsub";

const DEFAULT_ACK_DEADLINE_SECONDS: i32 = 60;
// Pub/Sub messages are acknowledged only after a checkpoint. The upstream client default of 50
// can therefore stall each reader between checkpoints and severely limit throughput.
const DEFAULT_MAX_OUTSTANDING_MESSAGES: i64 = 1024;
const DEFAULT_MAX_OUTSTANDING_BYTES: i64 = 1_000_000_000;

/// # Implementation Notes
/// Pub/Sub does not rely on persisted state (`SplitImpl`) to start from a position.
/// It rely on Pub/Sub to load-balance messages between all Readers.
/// We `ack` received messages after checkpoint (see `WaitCheckpointWorker`) to achieve at-least-once delivery.
#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct PubsubProperties {
    /// The Google Pub/Sub project ID. If omitted, the connector uses the project ID from
    /// the credentials or Application Default Credentials.
    #[serde(rename = "pubsub.project_id")]
    pub project_id: Option<String>,

    /// Pub/Sub subscription to consume messages from.
    ///
    /// Note that we rely on Pub/Sub to load-balance messages between all Readers pulling from
    /// the same subscription. So one `subscription` (i.e., one `Source`) can only used for one MV
    /// (shared between the actors of its fragment).
    /// Otherwise, different MVs on the same Source will both receive part of the messages.
    /// TODO: check and enforce this on Meta.
    #[serde(rename = "pubsub.subscription")]
    pub subscription: String,

    /// use the connector with a pubsub emulator
    /// <https://cloud.google.com/pubsub/docs/emulator>
    #[serde(rename = "pubsub.emulator_host")]
    pub emulator_host: Option<String>,

    /// `credentials` is a JSON string containing the service account credentials. If omitted,
    /// the connector uses Google Application Default Credentials (ADC) when allowed by the
    /// deployment environment.
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
    /// Besides retention guarantees, snapshots are also more precise than timestamp-based seeks.
    /// See [Seeking to a snapshot](https://cloud.google.com/pubsub/docs/replay-overview#seeking_to_a_timestamp) for
    /// more details.
    #[serde(rename = "pubsub.start_snapshot")]
    pub start_snapshot: Option<String>,

    /// Deprecated: ignored since adaptive split mode was introduced.
    /// Split count now adapts automatically to the number of actors.
    /// Kept for backward compatibility with existing DDL.
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "pubsub.parallelism")]
    pub parallelism: Option<u32>,

    /// The ack deadline in seconds for the streaming pull subscriber.
    /// This is the maximum time the server will wait for an ack before redelivering the message.
    /// Must be between 10 and 600 seconds. Defaults to 60.
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "pubsub.ack_deadline_seconds")]
    #[with_option(allow_alter_on_fly)]
    pub ack_deadline_seconds: Option<i32>,

    /// The maximum number of unacknowledged messages delivered to each streaming pull reader.
    /// Pub/Sub pauses delivery to a reader when this limit is reached. Must be greater than 0.
    /// Defaults to 1024.
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "pubsub.max_outstanding_messages")]
    #[with_option(allow_alter_on_fly)]
    pub max_outstanding_messages: Option<i64>,

    /// The maximum total size of unacknowledged messages delivered to each streaming pull reader.
    /// Pub/Sub pauses delivery to a reader when this limit is reached. Must be greater than 0.
    /// Defaults to 1 GB.
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "pubsub.max_outstanding_bytes")]
    #[with_option(allow_alter_on_fly)]
    pub max_outstanding_bytes: Option<i64>,

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

impl EnforceSecret for PubsubProperties {
    const ENFORCE_SECRET_PROPERTIES: Set<&'static str> = phf_set! {
        "pubsub.credentials",
    };
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
    pub(crate) fn subscriber_config(&self) -> ConnectorResult<SubscriberConfig> {
        let stream_ack_deadline_seconds = self
            .ack_deadline_seconds
            .unwrap_or(DEFAULT_ACK_DEADLINE_SECONDS);
        if !(10..=600).contains(&stream_ack_deadline_seconds) {
            bail!("pubsub.ack_deadline_seconds must be between 10 and 600");
        }

        let max_outstanding_messages = self
            .max_outstanding_messages
            .unwrap_or(DEFAULT_MAX_OUTSTANDING_MESSAGES);
        if max_outstanding_messages <= 0 {
            bail!("pubsub.max_outstanding_messages must be greater than 0");
        }

        let max_outstanding_bytes = self
            .max_outstanding_bytes
            .unwrap_or(DEFAULT_MAX_OUTSTANDING_BYTES);
        if max_outstanding_bytes <= 0 {
            bail!("pubsub.max_outstanding_bytes must be greater than 0");
        }

        Ok(SubscriberConfig {
            stream_ack_deadline_seconds,
            max_outstanding_messages,
            max_outstanding_bytes,
            ..Default::default()
        })
    }

    pub(crate) async fn subscription_client(&self) -> ConnectorResult<Subscription> {
        let auth_config = project::Config::default()
            .with_audience(apiv1::conn_pool::AUDIENCE)
            .with_scopes(&apiv1::conn_pool::SCOPES);
        let (environment, detected_project_id) = if let Some(credentials) = &self.credentials {
            let credentials = CredentialsFile::new_from_str(credentials)
                .await
                .context("failed to parse Google Cloud Pub/Sub credentials")?;
            let provider = DefaultTokenSourceProvider::new_with_credentials(
                auth_config,
                Box::new(credentials),
            )
            .await
            .context("failed to initialize Google Cloud Pub/Sub token source")?;
            let project_id = provider.project_id.clone();
            (Environment::GoogleCloud(Box::new(provider)), project_id)
        } else if let Some(emulator_host) = &self.emulator_host {
            (Environment::Emulator(emulator_host.clone()), None)
        } else {
            if env_var_is_true(DISABLE_DEFAULT_CREDENTIAL) {
                bail!(
                    "Google Application Default Credentials are disabled; configure `pubsub.credentials` or `pubsub.emulator_host`"
                );
            }

            let provider = DefaultTokenSourceProvider::new(auth_config)
                .await
                .context(
                    "failed to initialize Google Cloud Pub/Sub ADC; provide `pubsub.credentials`, configure ADC, or use `pubsub.emulator_host`",
                )?;
            let project_id = provider.project_id.clone();
            (Environment::GoogleCloud(Box::new(provider)), project_id)
        };

        let project_id = resolve_pubsub_project_id(
            self.project_id.as_deref(),
            detected_project_id.as_deref(),
            matches!(&environment, Environment::Emulator(_)),
        )
        .context(
            "Google Cloud Pub/Sub project ID is unavailable; configure `pubsub.project_id` or provide credentials/ADC with a project ID",
        )?;
        let config = ClientConfig {
            environment,
            project_id: Some(project_id),
            ..Default::default()
        };
        let client = Client::new(config)
            .await
            .context("error initializing pubsub client")?;

        Ok(client.subscription(&self.subscription))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn parse_pubsub_properties(extra: serde_json::Value) -> PubsubProperties {
        let mut value = json!({
            "pubsub.subscription": "projects/test/subscriptions/test",
            "pubsub.emulator_host": "localhost:8900",
        });
        value
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn test_subscriber_config_defaults() {
        let config = parse_pubsub_properties(json!({}))
            .subscriber_config()
            .unwrap();

        assert_eq!(config.stream_ack_deadline_seconds, 60);
        assert_eq!(config.max_outstanding_messages, 1024);
        assert_eq!(config.max_outstanding_bytes, 1_000_000_000);
    }

    #[test]
    fn test_subscriber_config_overrides() {
        let config = parse_pubsub_properties(json!({
            "pubsub.ack_deadline_seconds": "120",
            "pubsub.max_outstanding_messages": "2048",
            "pubsub.max_outstanding_bytes": "1048576",
        }))
        .subscriber_config()
        .unwrap();

        assert_eq!(config.stream_ack_deadline_seconds, 120);
        assert_eq!(config.max_outstanding_messages, 2048);
        assert_eq!(config.max_outstanding_bytes, 1_048_576);
    }

    #[test]
    fn test_subscriber_config_validation() {
        let invalid_values = [
            (
                json!({"pubsub.ack_deadline_seconds": "9"}),
                "pubsub.ack_deadline_seconds must be between 10 and 600",
            ),
            (
                json!({"pubsub.max_outstanding_messages": "0"}),
                "pubsub.max_outstanding_messages must be greater than 0",
            ),
            (
                json!({"pubsub.max_outstanding_bytes": "0"}),
                "pubsub.max_outstanding_bytes must be greater than 0",
            ),
        ];

        for (value, expected_error) in invalid_values {
            let error = parse_pubsub_properties(value)
                .subscriber_config()
                .unwrap_err();
            assert!(error.to_string().contains(expected_error));
        }
    }
}

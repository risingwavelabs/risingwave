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

use std::collections::HashMap;

use anyhow::Context;
use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_pubsub::subscription::Subscription;
use serde::Deserialize;

pub mod enumerator;
pub mod source;
pub mod split;
pub use enumerator::*;
use serde_with::{DisplayFromStr, serde_as};
pub use source::*;
pub use split::*;
use with_options::WithOptions;

use crate::error::ConnectorResult;
use crate::source::SourceProperties;

pub const GOOGLE_PUBSUB_CONNECTOR: &str = "google_pubsub";

/// # Implementation Notes
/// Pub/Sub does not rely on persisted state (`SplitImpl`) to start from a position.
/// It rely on Pub/Sub to load-balance messages between all Readers.
/// We `ack` received messages after checkpoint (see `WaitCheckpointWorker`) to achieve at-least-once delivery.
#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct PubsubProperties {
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
    /// Besides retention guarantees, snapshots are also more precise than timestamp-based seeks.
    /// See [Seeking to a snapshot](https://cloud.google.com/pubsub/docs/replay-overview#seeking_to_a_timestamp) for
    /// more details.
    #[serde(rename = "pubsub.start_snapshot")]
    pub start_snapshot: Option<String>,

    /// `parallelism` is the number of parallel consumers to run for the subscription.
    /// TODO: use system parallelism if not set
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "pubsub.parallelism")]
    pub parallelism: Option<u32>,

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
    pub(crate) async fn subscription_client(&self) -> ConnectorResult<Subscription> {
        // initialize env
        {
            tracing::debug!("setting pubsub environment variables");
            if let Some(emulator_host) = &self.emulator_host {
                // safety: only read in the same thread below in with_auth
                unsafe { std::env::set_var("PUBSUB_EMULATOR_HOST", emulator_host) };
            }
            if let Some(credentials) = &self.credentials {
                // safety: only read in the same thread below in with_auth
                unsafe { std::env::set_var("GOOGLE_APPLICATION_CREDENTIALS_JSON", credentials) };
            }
        };

        // Validate config
        let config = ClientConfig::default().with_auth().await?;
        let client = Client::new(config)
            .await
            .context("error initializing pubsub client")?;

        Ok(client.subscription(&self.subscription))
    }
}

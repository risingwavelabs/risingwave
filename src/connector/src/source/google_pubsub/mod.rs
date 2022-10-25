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

use serde::{Deserialize, Serialize};

pub mod enumerator;
pub mod source;
pub mod split;

pub use enumerator::*;
use serde_with::{serde_as, DisplayFromStr};
pub use source::*;
pub use split::*;

pub const GOOGLE_PUBSUB_CONNECTOR: &str = "google_pubsub";

#[serde_as]
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Hash)]
pub struct PubsubProperties {
    #[serde_as(as = "DisplayFromStr")]
    #[serde(rename = "pubsub.split_count")]
    pub split_count: u32,

    /// pubsub subscription to consume messages from
    /// The subscription should be configured with the `retain-on-ack` property to enable
    /// message recovery within risingwave.
    #[serde(rename = "pubsub.subscription")]
    pub subscription: String,

    /// use the connector with a pubsub emulator
    /// https://cloud.google.com/pubsub/docs/emulator
    #[serde(rename = "pubsub.emulator_host")]
    pub emulator_host: Option<String>,

    /// credentials JSON object encoded with base64
    /// See the [service-account credentials guide](https://developers.google.com/workspace/guides/create-credentials#create_credentials_for_a_service_account).
    /// The service account must have the `pubsub.subscriber` [role](https://cloud.google.com/pubsub/docs/access-control#roles).
    #[serde(rename = "pubsub.credentials")]
    pub credentials: Option<String>,
    // TODO? endpoint override
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

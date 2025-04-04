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
pub mod source;
pub mod split;
pub mod topic;

use std::collections::HashMap;

pub use enumerator::*;
use serde::Deserialize;
use serde_with::serde_as;
pub use split::*;
use with_options::WithOptions;

use self::source::reader::PulsarSplitReader;
use crate::connector_common::{AwsAuthProps, PulsarCommon, PulsarOauthCommon};
use crate::enforce_secret_on_cloud::EnforceSecretOnCloud;
use crate::source::SourceProperties;
use crate::error::ConnectorError;

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

impl EnforceSecretOnCloud for PulsarProperties {
    fn enforce_secret_on_cloud<'a>(prop_iter: impl Iterator<Item = &'a str>) -> Result<(), ConnectorError> {
        for prop in prop_iter {
            PulsarCommon::enforce_one(prop)?;
        }
        Ok(())
    }
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

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

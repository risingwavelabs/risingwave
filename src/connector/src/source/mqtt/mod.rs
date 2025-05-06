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
pub use enumerator::MqttSplitEnumerator;
pub mod split;

use std::collections::HashMap;
use std::fmt::{Display, Formatter};

use serde_derive::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use thiserror::Error;
use with_options::WithOptions;

use crate::connector_common::{MqttCommon, MqttQualityOfService};
use crate::enforce_secret::EnforceSecret;
use crate::error::ConnectorResult;
use crate::source::SourceProperties;
use crate::source::mqtt::source::{MqttSplit, MqttSplitReader};

pub const MQTT_CONNECTOR: &str = "mqtt";

#[derive(Debug, Clone, Error)]
pub struct MqttError(String);

impl Display for MqttError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct MqttProperties {
    #[serde(flatten)]
    pub common: MqttCommon,

    /// The topic name to subscribe or publish to. When subscribing, it can be a wildcard topic. e.g /topic/#
    pub topic: String,

    /// The quality of service to use when publishing messages. Defaults to at_most_once.
    /// Could be at_most_once, at_least_once or exactly_once
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub qos: Option<MqttQualityOfService>,

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

impl EnforceSecret for MqttProperties {
    fn enforce_secret<'a>(prop_iter: impl Iterator<Item = &'a str>) -> ConnectorResult<()> {
        for prop in prop_iter {
            MqttCommon::enforce_one(prop)?;
        }
        Ok(())
    }
}

impl SourceProperties for MqttProperties {
    type Split = MqttSplit;
    type SplitEnumerator = MqttSplitEnumerator;
    type SplitReader = MqttSplitReader;

    const SOURCE_NAME: &'static str = MQTT_CONNECTOR;
}

impl crate::source::UnknownFields for MqttProperties {
    fn unknown_fields(&self) -> HashMap<String, String> {
        self.unknown_fields.clone()
    }
}

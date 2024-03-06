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

use serde_derive::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
use with_options::WithOptions;

use crate::mqtt_common::{MqttCommon, QualityOfService};
use crate::source::mqtt::enumerator::MqttSplitEnumerator;
use crate::source::mqtt::source::{MqttSplit, MqttSplitReader};
use crate::source::SourceProperties;

pub const MQTT_CONNECTOR: &str = "mqtt";

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct MqttProperties {
    #[serde(flatten)]
    pub common: MqttCommon,

    /// The quality of service to use when publishing messages. Defaults to at_most_once.
    /// Could be at_most_once, at_least_once or exactly_once
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub qos: Option<QualityOfService>,

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
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

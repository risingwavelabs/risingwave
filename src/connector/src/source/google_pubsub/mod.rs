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

use serde::Deserialize;

pub mod enumerator;
pub mod source;
pub mod split;

pub use enumerator::*;
pub use source::*;
pub use split::*;

pub const GOOGLE_PUBSUB_CONNECTOR: &str = "google_pubsub";

#[derive(Clone, Debug, Deserialize)]
pub struct PubsubProperties {
    #[serde(rename = "pubsub.split_count")]
    pub split_count: u32,

    #[serde(rename = "pubsub.subscription")]
    pub subscription: String,

    // use against the pubsub emulator
    #[serde(rename = "pubsub.emulator_host")]
    pub emulator_host: Option<String>,

    #[serde(rename = "pubsub.emulator_host")]
    pub credentials: Option<String>,
}

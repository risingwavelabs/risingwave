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
use phf::{Set, phf_set};
use risingwave_common::util::env_var::env_var_is_true;
use serde::Deserialize;

use crate::connector_common::DISABLE_DEFAULT_CREDENTIAL;
use crate::deserialize_optional_bool_from_string;
use crate::enforce_secret::EnforceSecret;
use crate::source::SourceProperties;
use crate::source::util::dummy::{
    DummyProperties, DummySourceReader, DummySplit, DummySplitEnumerator,
};

/// Refer to [`crate::source::OPENDAL_S3_CONNECTOR`].
pub const LEGACY_S3_CONNECTOR: &str = "s3";

/// These are supported by both `s3` and `s3_v2` (opendal) sources.
#[derive(Clone, Debug, Deserialize, PartialEq, with_options::WithOptions)]
pub struct S3PropertiesCommon {
    #[serde(rename = "s3.region_name")]
    pub region_name: String,
    #[serde(rename = "s3.bucket_name")]
    pub bucket_name: String,
    #[serde(rename = "match_pattern", default)]
    pub match_pattern: Option<String>,
    #[serde(rename = "s3.credentials.access", default)]
    pub access: Option<String>,
    #[serde(rename = "s3.credentials.secret", default)]
    pub secret: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_bool_from_string")]
    pub enable_config_load: Option<bool>,
    #[serde(rename = "s3.endpoint_url")]
    pub endpoint_url: Option<String>,
}

impl S3PropertiesCommon {
    pub fn enable_config_load(&self) -> bool {
        // If the env var is set to true, we disable the default config load. (Cloud environment)
        if env_var_is_true(DISABLE_DEFAULT_CREDENTIAL) {
            return false;
        }
        self.enable_config_load.unwrap_or(false)
    }
}

impl EnforceSecret for S3PropertiesCommon {
    const ENFORCE_SECRET_PROPERTIES: Set<&'static str> = phf_set! {
        "s3.credentials.access",
        "s3.credentials.secret",
    };
}

#[derive(Debug, Clone, PartialEq)]
pub struct LegacyS3;

/// Note: legacy s3 source is fully deprecated since v2.4.0.
/// The properties and enumerator are kept, so that meta can start normally.
pub type LegacyS3Properties = DummyProperties<LegacyS3>;

/// Note: legacy s3 source is fully deprecated since v2.4.0.
/// The properties and enumerator are kept, so that meta can start normally.
pub type LegacyS3SplitEnumerator = DummySplitEnumerator<LegacyS3>;

pub type LegacyFsSplit = DummySplit<LegacyS3>;

impl SourceProperties for LegacyS3Properties {
    type Split = LegacyFsSplit;
    type SplitEnumerator = LegacyS3SplitEnumerator;
    type SplitReader = DummySourceReader<LegacyS3>;

    const SOURCE_NAME: &'static str = LEGACY_S3_CONNECTOR;
}

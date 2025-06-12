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
use std::collections::HashMap;

pub use enumerator::LegacyS3SplitEnumerator;
use risingwave_common::util::env_var::env_var_is_true;

use crate::connector_common::DISABLE_DEFAULT_CREDENTIAL;
use crate::deserialize_optional_bool_from_string;
use crate::source::filesystem::LegacyFsSplit;
use crate::source::{SourceProperties, UnknownFields};
mod source;
use serde::Deserialize;
pub use source::LegacyS3FileReader;

use crate::connector_common::AwsAuthProps;

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

#[derive(Clone, Debug, Deserialize, PartialEq, with_options::WithOptions)]
pub struct LegacyS3Properties {
    #[serde(flatten)]
    pub common: S3PropertiesCommon,

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

impl From<S3PropertiesCommon> for LegacyS3Properties {
    fn from(common: S3PropertiesCommon) -> Self {
        Self {
            common,
            unknown_fields: HashMap::new(),
        }
    }
}

impl SourceProperties for LegacyS3Properties {
    type Split = LegacyFsSplit;
    type SplitEnumerator = LegacyS3SplitEnumerator;
    type SplitReader = LegacyS3FileReader;

    const SOURCE_NAME: &'static str = LEGACY_S3_CONNECTOR;
}

impl UnknownFields for LegacyS3Properties {
    fn unknown_fields(&self) -> HashMap<String, String> {
        self.unknown_fields.clone()
    }
}

impl From<&LegacyS3Properties> for AwsAuthProps {
    fn from(props: &LegacyS3Properties) -> Self {
        let props = &props.common;
        Self {
            region: Some(props.region_name.clone()),
            endpoint: props.endpoint_url.clone(),
            access_key: props.access.clone(),
            secret_key: props.secret.clone(),
            session_token: Default::default(),
            arn: Default::default(),
            external_id: Default::default(),
            profile: Default::default(),
            msk_signer_timeout_sec: Default::default(),
        }
    }
}

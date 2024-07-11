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
use std::collections::HashMap;

pub use enumerator::S3SplitEnumerator;

use crate::source::filesystem::file_common::CompressionFormat;
mod source;
use serde::Deserialize;
pub use source::S3FileReader;

use crate::connector_common::AwsAuthProps;
use crate::source::filesystem::FsSplit;
use crate::source::{SourceProperties, UnknownFields};

pub const S3_CONNECTOR: &str = "s3";

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
    #[serde(rename = "s3.endpoint_url")]
    pub endpoint_url: Option<String>,
    #[serde(rename = "compression_format", default = "Default::default")]
    pub compression_format: CompressionFormat,
}

#[derive(Clone, Debug, Deserialize, PartialEq, with_options::WithOptions)]
pub struct S3Properties {
    #[serde(flatten)]
    pub common: S3PropertiesCommon,

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

impl From<S3PropertiesCommon> for S3Properties {
    fn from(common: S3PropertiesCommon) -> Self {
        Self {
            common,
            unknown_fields: HashMap::new(),
        }
    }
}

impl SourceProperties for S3Properties {
    type Split = FsSplit;
    type SplitEnumerator = S3SplitEnumerator;
    type SplitReader = S3FileReader;

    const SOURCE_NAME: &'static str = S3_CONNECTOR;
}

impl UnknownFields for S3Properties {
    fn unknown_fields(&self) -> HashMap<String, String> {
        self.unknown_fields.clone()
    }
}

impl From<&S3Properties> for AwsAuthProps {
    fn from(props: &S3Properties) -> Self {
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
        }
    }
}

// Copyright 2023 RisingWave Labs
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

pub mod gcs_source;

pub use gcs_source::*;
pub mod s3_source;
pub use s3_source::*;
use serde::Deserialize;
pub mod opendal_enumerator;
pub mod opendal_reader;

use self::opendal_enumerator::OpendalEnumerator;
use self::opendal_reader::OpendalReader;
use super::{OpendalFsSplit, S3Properties};
use crate::source::SourceProperties;

pub const GCS_CONNECTOR: &str = "gcs";
// The new s3_v2 will use opendal.
pub const OPENDAL_S3_CONNECTOR: &str = "s3_v2";

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct GcsProperties {
    #[serde(rename = "gcs.bucket_name")]
    pub bucket_name: String,
    #[serde(rename = "gcs.credential")]
    pub credential: Option<String>,
    #[serde(rename = "gcs.service_account", default)]
    pub service_account: Option<String>,
    #[serde(rename = "match_pattern", default)]
    pub match_pattern: Option<String>,
}

impl SourceProperties for GcsProperties {
    type Split = OpendalFsSplit<OpendalGcs>;
    type SplitEnumerator = OpendalEnumerator<OpendalGcs>;
    type SplitReader = OpendalReader<OpendalGcs>;

    const SOURCE_NAME: &'static str = GCS_CONNECTOR;
}

pub trait OpendalSource: Send + Sync + 'static + Clone + PartialEq {
    type Properties: SourceProperties + Send + Sync;

    fn new_enumerator(properties: Self::Properties) -> anyhow::Result<OpendalEnumerator<Self>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpendalS3;

impl OpendalSource for OpendalS3 {
    type Properties = OpendalS3Properties;

    fn new_enumerator(properties: Self::Properties) -> anyhow::Result<OpendalEnumerator<Self>> {
        OpendalEnumerator::new_s3_source(properties.s3_properties, properties.assume_role)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpendalGcs;

impl OpendalSource for OpendalGcs {
    type Properties = GcsProperties;

    fn new_enumerator(properties: Self::Properties) -> anyhow::Result<OpendalEnumerator<Self>> {
        OpendalEnumerator::new_gcs_source(properties)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct OpendalS3Properties {
    pub s3_properties: S3Properties,
    #[serde(rename = "s3.assume_role", default)]
    pub assume_role: Option<String>,
}

impl SourceProperties for OpendalS3Properties {
    type Split = OpendalFsSplit<OpendalS3>;
    type SplitEnumerator = OpendalEnumerator<OpendalS3>;
    type SplitReader = OpendalReader<OpendalS3>;

    const SOURCE_NAME: &'static str = OPENDAL_S3_CONNECTOR;
}

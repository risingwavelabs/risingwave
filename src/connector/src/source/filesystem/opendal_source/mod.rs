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
pub const OPENDAL_S3_CONNECTOR: &str = "s3_v2";

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct GcsProperties {
    #[serde(rename = "gcs.bucket_name")]
    pub bucket_name: String,
    #[serde(rename = "gcs.credential")]
    pub credential: Option<String>,
    #[serde(rename = "match_pattern", default)]
    pub match_pattern: Option<String>,
}

impl SourceProperties for GcsProperties {
    type Split = OpendalFsSplit<GcsProperties>;
    type SplitEnumerator = OpendalEnumerator<GcsProperties>;
    type SplitReader = OpendalReader<GcsProperties>;

    const SOURCE_NAME: &'static str = GCS_CONNECTOR;

    fn init_from_pb_source(&mut self, _source: &risingwave_pb::catalog::PbSource) {}
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct OpendalS3Properties {
    pub s3_properties: S3Properties,
}

impl SourceProperties for OpendalS3Properties {
    type Split = OpendalFsSplit<OpendalS3Properties>;
    type SplitEnumerator = OpendalEnumerator<OpendalS3Properties>;
    type SplitReader = OpendalReader<OpendalS3Properties>;

    const SOURCE_NAME: &'static str = OPENDAL_S3_CONNECTOR;

    fn init_from_pb_source(&mut self, _source: &risingwave_pb::catalog::PbSource) {}
}

pub trait OpenDalProperties: Sized + Send + Clone + PartialEq + 'static + Sync {
    fn new_enumerator(properties: Self) -> anyhow::Result<OpendalEnumerator<Self>>;
}

impl OpenDalProperties for GcsProperties {
    fn new_enumerator(properties: Self) -> anyhow::Result<OpendalEnumerator<Self>> {
        OpendalEnumerator::new_gcs_source(properties)
    }
}

impl OpenDalProperties for OpendalS3Properties {
    fn new_enumerator(properties: Self) -> anyhow::Result<OpendalEnumerator<Self>> {
        OpendalEnumerator::new_s3_source(properties.s3_properties)
    }
}

/// Get the prefix from a glob
pub fn get_prefix(glob: &str) -> String {
    let mut escaped = false;
    let mut escaped_filter = false;
    glob.chars()
        .take_while(|c| match (c, &escaped) {
            ('*', false) => false,
            ('[', false) => false,
            ('{', false) => false,
            ('\\', false) => {
                escaped = true;
                true
            }
            (_, false) => true,
            (_, true) => {
                escaped = false;
                true
            }
        })
        .filter(|c| match (c, &escaped_filter) {
            (_, true) => {
                escaped_filter = false;
                true
            }
            ('\\', false) => {
                escaped_filter = true;
                false
            }
            (_, _) => true,
        })
        .collect()
}

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

use self::opendal_enumerator::OpenDALConnector;
use super::{GcsSplit, S3Properties, S3_CONNECTOR};
use crate::source::{SourceProperties, SplitImpl};

pub const GCS_CONNECTOR: &str = "gcs";

#[derive(Clone, Debug, Deserialize)]
pub struct GCSProperties {
    #[serde(rename = "gcs.bucket_name")]
    pub bucket_name: String,
}

impl SourceProperties for GCSProperties {
    type Split = GcsSplit;
    type SplitEnumerator = OpenDALConnector;
    type SplitReader = OpenDALConnector;

    const SOURCE_NAME: &'static str = GCS_CONNECTOR;

    fn init_from_pb_source(&mut self, _source: &risingwave_pb::catalog::PbSource) {}
}

#[derive(Clone, Debug, Deserialize)]
pub enum OpenDALProperties {
    GCSProperties(GCSProperties),
    S3Properties(S3Properties),
}

// impl SourceProperties for OpenDALProperties{

//     const SOURCE_NAME: &'static str = GCS_CONNECTOR;
//     type Split = GcsSplit;

//     type SplitEnumerator = OpenDALConnector;
//     type SplitReader = OpenDALConnector;

//     fn init_from_pb_source(&mut self, _source: &risingwave_pb::catalog::PbSource) {}
// }

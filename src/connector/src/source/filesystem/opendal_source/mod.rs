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

use std::collections::HashMap;

pub use opendal_enumerator::OpendalEnumerator;

pub mod azblob_source;
pub mod gcs_source;
pub mod posix_fs_source;
pub mod s3_source;

use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use with_options::WithOptions;
pub mod opendal_enumerator;
pub mod opendal_reader;
use std::collections::HashSet;

use once_cell::sync::Lazy;

use self::opendal_reader::OpendalReader;
use super::OpendalFsSplit;
use super::file_common::CompressionFormat;
pub use super::s3::S3PropertiesCommon;
use crate::enforce_secret_on_cloud::EnforceSecretOnCloud;
use crate::error::{ConnectorError, ConnectorResult};
use crate::source::{SourceProperties, UnknownFields};

pub const AZBLOB_CONNECTOR: &str = "azblob";
pub const GCS_CONNECTOR: &str = "gcs";
/// The new `s3_v2` will use opendal.
/// Note: user uses `connector='s3'`, which is converted to `connector='s3_v2'` in frontend (in `validate_compatibility`).
/// If user inputs `connector='s3_v2'`, it will be rejected.
pub const OPENDAL_S3_CONNECTOR: &str = "s3_v2";
pub const POSIX_FS_CONNECTOR: &str = "posix_fs";

pub const DEFAULT_REFRESH_INTERVAL_SEC: u64 = 60;

#[serde_as]
#[derive(Clone, Debug, Deserialize, PartialEq, WithOptions)]
pub struct FsSourceCommon {
    #[serde(rename = "refresh.interval.sec")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub refresh_interval_sec: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, WithOptions)]
pub struct GcsProperties {
    #[serde(rename = "gcs.bucket_name")]
    pub bucket_name: String,

    /// The base64 encoded credential key. If not set, ADC will be used.
    #[serde(rename = "gcs.credential")]
    pub credential: Option<String>,

    /// If credential/ADC is not set. The service account can be used to provide the credential info.
    #[serde(rename = "gcs.service_account", default)]
    pub service_account: Option<String>,

    #[serde(rename = "match_pattern", default)]
    pub match_pattern: Option<String>,

    #[serde(flatten)]
    pub fs_common: FsSourceCommon,

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,

    #[serde(rename = "compression_format", default = "Default::default")]
    pub compression_format: CompressionFormat,
}

impl EnforceSecretOnCloud for GcsProperties {
    const ENFORCE_SECRET_PROPERTIES_ON_CLOUD: Lazy<HashSet<&'static str>> =
        Lazy::new(|| HashSet::from(["gcs.credential", "gcs.service_account"]));
}

impl UnknownFields for GcsProperties {
    fn unknown_fields(&self) -> HashMap<String, String> {
        self.unknown_fields.clone()
    }
}

impl SourceProperties for GcsProperties {
    type Split = OpendalFsSplit<OpendalGcs>;
    type SplitEnumerator = OpendalEnumerator<OpendalGcs>;
    type SplitReader = OpendalReader<OpendalGcs>;

    const SOURCE_NAME: &'static str = GCS_CONNECTOR;
}

pub trait OpendalSource: Send + Sync + 'static + Clone + PartialEq {
    type Properties: SourceProperties + Send + Sync;

    fn new_enumerator(properties: Self::Properties) -> ConnectorResult<OpendalEnumerator<Self>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpendalS3;

impl OpendalSource for OpendalS3 {
    type Properties = OpendalS3Properties;

    fn new_enumerator(properties: Self::Properties) -> ConnectorResult<OpendalEnumerator<Self>> {
        OpendalEnumerator::new_s3_source(properties.s3_properties, properties.assume_role)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpendalGcs;

impl OpendalSource for OpendalGcs {
    type Properties = GcsProperties;

    fn new_enumerator(properties: Self::Properties) -> ConnectorResult<OpendalEnumerator<Self>> {
        OpendalEnumerator::new_gcs_source(properties)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpendalPosixFs;

impl OpendalSource for OpendalPosixFs {
    type Properties = PosixFsProperties;

    fn new_enumerator(properties: Self::Properties) -> ConnectorResult<OpendalEnumerator<Self>> {
        OpendalEnumerator::new_posix_fs_source(properties)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, with_options::WithOptions)]
pub struct OpendalS3Properties {
    #[serde(flatten)]
    pub s3_properties: S3PropertiesCommon,

    /// The following are only supported by `s3_v2` (opendal) source.
    #[serde(rename = "s3.assume_role", default)]
    pub assume_role: Option<String>,

    #[serde(flatten)]
    pub fs_common: FsSourceCommon,

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

impl EnforceSecretOnCloud for OpendalS3Properties {
    fn enforce_secret_on_cloud<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> Result<(), ConnectorError> {
        S3PropertiesCommon::enforce_secret_on_cloud(prop_iter)
    }
}

impl UnknownFields for OpendalS3Properties {
    fn unknown_fields(&self) -> HashMap<String, String> {
        self.unknown_fields.clone()
    }
}

impl SourceProperties for OpendalS3Properties {
    type Split = OpendalFsSplit<OpendalS3>;
    type SplitEnumerator = OpendalEnumerator<OpendalS3>;
    type SplitReader = OpendalReader<OpendalS3>;

    const SOURCE_NAME: &'static str = OPENDAL_S3_CONNECTOR;
}

#[derive(Clone, Debug, Deserialize, PartialEq, WithOptions)]
pub struct PosixFsProperties {
    /// The root directly of the files to search. The files will be searched recursively.
    #[serde(rename = "posix_fs.root")]
    pub root: String,

    /// The regex pattern to match files under root directory.
    #[serde(rename = "match_pattern", default)]
    pub match_pattern: Option<String>,

    #[serde(flatten)]
    pub fs_common: FsSourceCommon,

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
    #[serde(rename = "compression_format", default = "Default::default")]
    pub compression_format: CompressionFormat,
}

impl EnforceSecretOnCloud for PosixFsProperties {
    fn enforce_secret_on_cloud<'a>(
        _prop_iter: impl Iterator<Item = &'a str>,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }
}

impl UnknownFields for PosixFsProperties {
    fn unknown_fields(&self) -> HashMap<String, String> {
        self.unknown_fields.clone()
    }
}

impl SourceProperties for PosixFsProperties {
    type Split = OpendalFsSplit<OpendalPosixFs>;
    type SplitEnumerator = OpendalEnumerator<OpendalPosixFs>;
    type SplitReader = OpendalReader<OpendalPosixFs>;

    const SOURCE_NAME: &'static str = POSIX_FS_CONNECTOR;
}

#[derive(Clone, Debug, Deserialize, PartialEq, WithOptions)]
pub struct AzblobProperties {
    #[serde(rename = "azblob.container_name")]
    pub container_name: String,

    #[serde(rename = "azblob.credentials.account_name", default)]
    pub account_name: Option<String>,
    #[serde(rename = "azblob.credentials.account_key", default)]
    pub account_key: Option<String>,
    #[serde(rename = "azblob.endpoint_url")]
    pub endpoint_url: String,

    #[serde(rename = "match_pattern", default)]
    pub match_pattern: Option<String>,

    #[serde(flatten)]
    pub fs_common: FsSourceCommon,

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,

    #[serde(rename = "compression_format", default = "Default::default")]
    pub compression_format: CompressionFormat,
}

impl EnforceSecretOnCloud for AzblobProperties {
    const ENFORCE_SECRET_PROPERTIES_ON_CLOUD: Lazy<HashSet<&'static str>> = Lazy::new(|| {
        HashSet::from([
            "azblob.credentials.account_key",
            "azblob.credentials.account_name",
        ])
    });
}

impl UnknownFields for AzblobProperties {
    fn unknown_fields(&self) -> HashMap<String, String> {
        self.unknown_fields.clone()
    }
}

impl SourceProperties for AzblobProperties {
    type Split = OpendalFsSplit<OpendalAzblob>;
    type SplitEnumerator = OpendalEnumerator<OpendalAzblob>;
    type SplitReader = OpendalReader<OpendalAzblob>;

    const SOURCE_NAME: &'static str = AZBLOB_CONNECTOR;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpendalAzblob;

impl OpendalSource for OpendalAzblob {
    type Properties = AzblobProperties;

    fn new_enumerator(properties: Self::Properties) -> ConnectorResult<OpendalEnumerator<Self>> {
        OpendalEnumerator::new_azblob_source(properties)
    }
}

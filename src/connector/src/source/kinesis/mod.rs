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
pub mod split;

use std::collections::HashMap;

use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
pub use source::KinesisMeta;
use with_options::WithOptions;

use crate::connector_common::KinesisCommon;
use crate::source::kinesis::enumerator::client::KinesisSplitEnumerator;
use crate::source::kinesis::source::reader::KinesisSplitReader;
use crate::source::kinesis::split::KinesisSplit;
use crate::source::SourceProperties;

pub const KINESIS_CONNECTOR: &str = "kinesis";

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct KinesisProperties {
    #[serde(rename = "scan.startup.mode", alias = "kinesis.scan.startup.mode")]
    // accepted values: "latest", "earliest", "timestamp"
    pub scan_startup_mode: Option<String>,

    #[serde(rename = "scan.startup.timestamp.millis")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub start_timestamp_millis: Option<i64>,

    #[serde(flatten)]
    pub common: KinesisCommon,

    #[serde(flatten)]
    pub reader_config: KinesisReaderConfig,

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

const fn kinesis_reader_default_eof_retry_interval_ms() -> u64 {
    1000
}

const fn kinesis_reader_default_error_retry_interval_ms() -> u64 {
    1000
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct KinesisReaderConfig {
    #[serde(
        rename = "kinesis.reader.eof_retry_interval_ms",
        default = "kinesis_reader_default_eof_retry_interval_ms"
    )]
    pub eof_retry_interval_ms: u64,

    #[serde(
        rename = "kinesis.reader.error_retry_interval_ms",
        default = "kinesis_reader_default_error_retry_interval_ms"
    )]
    pub error_retry_interval_ms: u64,
}

impl Default for KinesisReaderConfig {
    fn default() -> Self {
        Self {
            eof_retry_interval_ms: kinesis_reader_default_eof_retry_interval_ms(),
            error_retry_interval_ms: kinesis_reader_default_error_retry_interval_ms(),
        }
    }
}
impl SourceProperties for KinesisProperties {
    type Split = KinesisSplit;
    type SplitEnumerator = KinesisSplitEnumerator;
    type SplitReader = KinesisSplitReader;

    const SOURCE_NAME: &'static str = KINESIS_CONNECTOR;
}

impl crate::source::UnknownFields for KinesisProperties {
    fn unknown_fields(&self) -> HashMap<String, String> {
        self.unknown_fields.clone()
    }
}

#[cfg(test)]
mod test {
    use maplit::{btreemap, hashmap};

    use super::*;
    use crate::source::ConnectorProperties;
    use crate::WithOptionsSecResolved;

    #[test]
    fn test_parse_kinesis_timestamp_offset() {
        let props: HashMap<String, String> = hashmap! {
            "stream".to_owned() => "sample_stream".to_owned(),
            "aws.region".to_owned() => "us-east-1".to_owned(),
            "scan_startup_mode".to_owned() => "timestamp".to_owned(),
            "scan.startup.timestamp.millis".to_owned() => "123456789".to_owned(),
        };

        let kinesis_props: KinesisProperties =
            serde_json::from_value(serde_json::to_value(props).unwrap()).unwrap();
        assert_eq!(kinesis_props.start_timestamp_millis, Some(123456789));
    }

    #[test]
    fn test_kinesis_props() {
        let with_options = btreemap! {
            "connector".to_owned() => "kinesis".to_owned(),
            "scan.startup.mode".to_owned() => "latest".to_owned(),
            "stream".to_owned() => "sample_stream".to_owned(),
            "aws.region".to_owned() => "us-east-1".to_owned(),
            "endpoint".to_owned() => "https://kinesis.us-east-1.amazonaws.com".to_owned(),
            "aws.credentials.access_key_id".to_owned() => "access_key".to_owned(),
            "aws.credentials.secret_access_key".to_owned() => "secret_key".to_owned(),
        };
        let connector_props = ConnectorProperties::extract(
            WithOptionsSecResolved::without_secrets(with_options),
            true,
        )
        .unwrap();
        let ConnectorProperties::Kinesis(kinesis_props) = connector_props else {
            panic!()
        };

        assert_eq!(kinesis_props.common.sdk_connect_timeout_ms, 10000);

        let with_options = btreemap! {
            "connector".to_owned() => "kinesis".to_owned(),
            "scan.startup.mode".to_owned() => "latest".to_owned(),
            "stream".to_owned() => "sample_stream".to_owned(),
            "aws.region".to_owned() => "us-east-1".to_owned(),
            "endpoint".to_owned() => "https://kinesis.us-east-1.amazonaws.com".to_owned(),
            "aws.credentials.access_key_id".to_owned() => "access_key".to_owned(),
            "aws.credentials.secret_access_key".to_owned() => "secret_key".to_owned(),
            "kinesis.sdk.connect_timeout_ms".to_owned() => "20000".to_owned(),
        };
        let connector_props = ConnectorProperties::extract(
            WithOptionsSecResolved::without_secrets(with_options),
            true,
        )
        .unwrap();
        let ConnectorProperties::Kinesis(kinesis_props) = connector_props else {
            panic!()
        };

        assert_eq!(kinesis_props.common.sdk_connect_timeout_ms, 20000);
    }
}

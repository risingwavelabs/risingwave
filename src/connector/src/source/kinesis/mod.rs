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
pub use enumerator::client::KinesisSplitEnumerator;
pub mod source;
pub mod split;

use std::collections::HashMap;

use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
pub use source::KinesisMeta;
use with_options::WithOptions;

use crate::connector_common::KinesisCommon;
use crate::enforce_secret::EnforceSecret;
use crate::error::ConnectorError;
use crate::source::SourceProperties;
use crate::source::kinesis::source::reader::KinesisSplitReader;
use crate::source::kinesis::split::KinesisSplit;

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
    200
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

impl EnforceSecret for KinesisProperties {
    fn enforce_secret<'a>(prop_iter: impl Iterator<Item = &'a str>) -> Result<(), ConnectorError> {
        for prop in prop_iter {
            KinesisCommon::enforce_one(prop)?;
        }
        Ok(())
    }
}

impl crate::source::UnknownFields for KinesisProperties {
    fn unknown_fields(&self) -> HashMap<String, String> {
        self.unknown_fields.clone()
    }
}

#[cfg(test)]
mod test {
    use maplit::hashmap;

    use super::*;

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
}

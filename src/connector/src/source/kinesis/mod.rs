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

pub mod enumerator;
pub mod source;
pub mod split;

use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
use with_options::WithOptions;

use crate::common::KinesisCommon;
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
    pub timestamp_offset: Option<i64>,

    #[serde(flatten)]
    pub common: KinesisCommon,
}

impl SourceProperties for KinesisProperties {
    type Split = KinesisSplit;
    type SplitEnumerator = KinesisSplitEnumerator;
    type SplitReader = KinesisSplitReader;

    const SOURCE_NAME: &'static str = KINESIS_CONNECTOR;
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use maplit::hashmap;

    use super::*;

    #[test]
    fn test_parse_kinesis_timestamp_offset() {
        let props: HashMap<String, String> = hashmap! {
            "stream".to_string() => "sample_stream".to_string(),
            "aws.region".to_string() => "us-east-1".to_string(),
            "scan_startup_mode".to_string() => "timestamp".to_string(),
            "scan.startup.timestamp.millis".to_string() => "123456789".to_string(),
        };

        let kinesis_props: KinesisProperties =
            serde_json::from_value(serde_json::to_value(props).unwrap()).unwrap();
        assert_eq!(kinesis_props.timestamp_offset, Some(123456789));
    }
}

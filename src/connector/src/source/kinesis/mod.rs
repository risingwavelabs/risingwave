// Copyright 2023 Singularity Data
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

pub mod config;
pub mod enumerator;
pub mod source;
pub mod split;

use std::collections::HashMap;

use anyhow::{anyhow, Result};
use serde::Deserialize;
use serde_json::Value;

use crate::common::KinesisCommon;

pub const KINESIS_CONNECTOR: &str = "kinesis";

#[derive(Clone, Debug, Deserialize)]
pub struct KinesisProperties {
    #[serde(rename = "scan.startup.mode", alias = "kinesis.scan.startup.mode")]
    // accepted values: "latest", "earliest", "sequence_number"
    pub scan_startup_mode: Option<String>,
    #[serde(
        rename = "scan.startup.sequence_number",
        alias = "kinesis.scan.startup.sequence_number"
    )]
    pub seq_offset: Option<String>,

    pub common: Option<KinesisCommon>,
    #[serde(flatten)]
    extra: Option<HashMap<String, Value>>,
}

impl KinesisProperties {
    fn extract_common(&mut self) -> Result<()> {
        self.common = Some(
            serde_json::from_value::<KinesisCommon>(
                serde_json::to_value(self.extra.take().unwrap()).map_err(|e| anyhow!(e))?,
            )
            .map_err(|e| anyhow!(e))?,
        );
        Ok(())
    }
}

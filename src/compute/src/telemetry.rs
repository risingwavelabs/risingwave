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

use std::time::SystemTime;

use anyhow::Result;
use risingwave_common::telemetry::{
    SystemData, TelemetryNodeType, TelemetryReport, TelemetryReportBase,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ComputeTelemetryReport {
    #[serde(flatten)]
    base: TelemetryReportBase,
}

impl TelemetryReport for ComputeTelemetryReport {
    fn to_json(&self) -> Result<String> {
        let json = serde_json::to_string(self)?;
        Ok(json)
    }
}

pub(crate) fn create_telemetry_report(
    tracking_id: String,
    session_id: String,
    up_time: u64,
) -> ComputeTelemetryReport {
    ComputeTelemetryReport {
        base: TelemetryReportBase {
            tracking_id,
            session_id,
            up_time,
            system_data: SystemData::new(),
            time_stamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("Clock might go backward")
                .as_secs(),
            node_type: TelemetryNodeType::Compute,
        },
    }
}

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

use anyhow::Result;
use risingwave_common::telemetry::report::TelemetryReportCreator;
use risingwave_common::telemetry::{
    current_timestamp, SystemData, TelemetryNodeType, TelemetryReport, TelemetryReportBase,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy)]
pub(crate) struct ComputeTelemetryCreator {}

impl ComputeTelemetryCreator {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

impl TelemetryReportCreator for ComputeTelemetryCreator {
    fn create_report(
        &self,
        tracking_id: String,
        session_id: String,
        up_time: u64,
    ) -> Result<ComputeTelemetryReport> {
        Ok(ComputeTelemetryReport::new(
            tracking_id,
            session_id,
            up_time,
        ))
    }

    fn report_type(&self) -> &str {
        "compute"
    }
}

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

impl ComputeTelemetryReport {
    pub(crate) fn new(tracking_id: String, session_id: String, up_time: u64) -> Self {
        Self {
            base: TelemetryReportBase {
                tracking_id,
                session_id,
                up_time,
                system_data: SystemData::new(),
                time_stamp: current_timestamp(),
                node_type: TelemetryNodeType::Compute,
            },
        }
    }
}

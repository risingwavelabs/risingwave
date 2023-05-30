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

use risingwave_common::telemetry::report::TelemetryReportCreator;
use risingwave_common::telemetry::{
    current_timestamp, SystemData, TelemetryNodeType, TelemetryReport, TelemetryReportBase,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy)]
pub(crate) struct FrontendTelemetryCreator {}

impl FrontendTelemetryCreator {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl TelemetryReportCreator for FrontendTelemetryCreator {
    async fn create_report(
        &self,
        tracking_id: String,
        session_id: String,
        up_time: u64,
    ) -> anyhow::Result<FrontendTelemetryReport> {
        Ok(FrontendTelemetryReport::new(
            tracking_id,
            session_id,
            up_time,
        ))
    }

    fn report_type(&self) -> &str {
        "frontend"
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct FrontendTelemetryReport {
    #[serde(flatten)]
    base: TelemetryReportBase,
}

impl TelemetryReport for FrontendTelemetryReport {
    fn to_json(&self) -> anyhow::Result<String> {
        let json = serde_json::to_string(self)?;
        Ok(json)
    }
}

impl FrontendTelemetryReport {
    pub(crate) fn new(tracking_id: String, session_id: String, up_time: u64) -> Self {
        Self {
            base: TelemetryReportBase {
                tracking_id,
                session_id,
                system_data: SystemData::new(),
                up_time,
                time_stamp: current_timestamp(),
                node_type: TelemetryNodeType::Frontend,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let report =
            FrontendTelemetryReport::new("tracking_id".to_owned(), "session_id".to_owned(), 0);

        assert_eq!(report.base.tracking_id, "tracking_id");
        assert_eq!(report.base.session_id, "session_id");
        assert_eq!(report.base.up_time, 0);
        assert_eq!(report.base.node_type, TelemetryNodeType::Frontend);
    }
}

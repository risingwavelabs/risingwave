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

use prost::Message;
use risingwave_common::telemetry::pb_compatible::TelemetryToProtobuf;
use risingwave_common::telemetry::report::TelemetryReportCreator;
use risingwave_common::telemetry::{
    SystemData, TelemetryNodeType, TelemetryReportBase, TelemetryResult, current_timestamp,
};
use serde::{Deserialize, Serialize};

const TELEMETRY_COMPUTE_REPORT_TYPE: &str = "compute";

#[derive(Clone, Copy)]
pub(crate) struct ComputeTelemetryCreator {}

impl ComputeTelemetryCreator {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl TelemetryReportCreator for ComputeTelemetryCreator {
    #[allow(refining_impl_trait)]
    async fn create_report(
        &self,
        tracking_id: String,
        session_id: String,
        up_time: u64,
    ) -> TelemetryResult<ComputeTelemetryReport> {
        Ok(ComputeTelemetryReport::new(
            tracking_id,
            session_id,
            up_time,
        ))
    }

    fn report_type(&self) -> &str {
        TELEMETRY_COMPUTE_REPORT_TYPE
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ComputeTelemetryReport {
    #[serde(flatten)]
    base: TelemetryReportBase,
}

impl TelemetryToProtobuf for ComputeTelemetryReport {
    fn to_pb_bytes(self) -> Vec<u8> {
        let pb_report = risingwave_pb::telemetry::ComputeReport {
            base: Some(self.base.into()),
        };
        pb_report.encode_to_vec()
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
                is_test: false,
            },
        }
    }
}

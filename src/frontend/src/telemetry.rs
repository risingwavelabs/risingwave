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
    report_event_common,
};
use risingwave_pb::telemetry::{PbTelemetryDatabaseObject, PbTelemetryEventStage};
use serde::{Deserialize, Serialize};

const TELEMETRY_FRONTEND_REPORT_TYPE: &str = "frontend";

pub(crate) fn report_event(
    event_stage: PbTelemetryEventStage,
    event_name: &str,
    catalog_id: i64,
    connector_name: Option<String>,
    component: Option<PbTelemetryDatabaseObject>,
    attributes: Option<jsonbb::Value>, // any json string
) {
    report_event_common(
        event_stage,
        event_name,
        catalog_id,
        connector_name,
        component,
        attributes,
        TELEMETRY_FRONTEND_REPORT_TYPE.to_owned(),
    );
}

#[derive(Clone, Copy)]
pub(crate) struct FrontendTelemetryCreator {}

impl FrontendTelemetryCreator {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl TelemetryReportCreator for FrontendTelemetryCreator {
    #[allow(refining_impl_trait)]
    async fn create_report(
        &self,
        tracking_id: String,
        session_id: String,
        up_time: u64,
    ) -> TelemetryResult<FrontendTelemetryReport> {
        Ok(FrontendTelemetryReport::new(
            tracking_id,
            session_id,
            up_time,
        ))
    }

    fn report_type(&self) -> &str {
        TELEMETRY_FRONTEND_REPORT_TYPE
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct FrontendTelemetryReport {
    #[serde(flatten)]
    base: TelemetryReportBase,
}

impl TelemetryToProtobuf for FrontendTelemetryReport {
    fn to_pb_bytes(self) -> Vec<u8> {
        let pb_report = risingwave_pb::telemetry::FrontendReport {
            base: Some(self.base.into()),
        };
        pb_report.encode_to_vec()
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
                is_test: false,
            },
        }
    }
}

// Copyright 2024 RisingWave Labs
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

use std::sync::OnceLock;

use prost::Message;
use risingwave_common::telemetry::pb_compatible::TelemetryToProtobuf;
use risingwave_common::telemetry::report::{report_event_common, TelemetryReportCreator};
use risingwave_common::telemetry::{
    current_timestamp, SystemData, TelemetryNodeType, TelemetryReportBase, TelemetryResult,
};
use risingwave_pb::telemetry::PbTelemetryEventStage;
use serde::{Deserialize, Serialize};

const TELEMETRY_COMPACTOR_REPORT_TYPE: &str = "compactor";

static COMPACTOR_TELEMETRY_SESSION_ID: OnceLock<String> = OnceLock::new();
static COMPACTOR_TELEMETRY_TRACKING_ID: OnceLock<String> = OnceLock::new();

pub(crate) fn set_compactor_telemetry_tracking_id_and_session_id(
    tracking_id: String,
    session_id: String,
) {
    COMPACTOR_TELEMETRY_TRACKING_ID.set(tracking_id).unwrap();
    COMPACTOR_TELEMETRY_SESSION_ID.set(session_id).unwrap();
}

fn get_compactor_telemetry_tracking_id_and_session_id() -> (Option<String>, Option<String>) {
    (
        COMPACTOR_TELEMETRY_TRACKING_ID.get().cloned(),
        COMPACTOR_TELEMETRY_SESSION_ID.get().cloned(),
    )
}

pub(crate) fn report_event(
    event_stage: PbTelemetryEventStage,
    feature_name: String,
    catalog_id: i64,
    connector_name: Option<String>,
    attributes: Option<String>, // any json string
) {
    report_event_common(
        Box::new(get_compactor_telemetry_tracking_id_and_session_id),
        event_stage,
        feature_name,
        catalog_id,
        connector_name,
        attributes,
        TELEMETRY_COMPACTOR_REPORT_TYPE.to_string(),
    );
}

#[derive(Clone, Copy)]
pub(crate) struct CompactorTelemetryCreator {}

impl CompactorTelemetryCreator {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl TelemetryReportCreator for CompactorTelemetryCreator {
    #[allow(refining_impl_trait)]
    async fn create_report(
        &self,
        tracking_id: String,
        session_id: String,
        up_time: u64,
    ) -> TelemetryResult<CompactorTelemetryReport> {
        Ok(CompactorTelemetryReport::new(
            tracking_id,
            session_id,
            up_time,
        ))
    }

    fn report_type(&self) -> &str {
        TELEMETRY_COMPACTOR_REPORT_TYPE
    }
}

impl TelemetryToProtobuf for CompactorTelemetryReport {
    fn to_pb_bytes(self) -> Vec<u8> {
        let pb_report = risingwave_pb::telemetry::CompactorReport {
            base: Some(self.base.into()),
        };
        pb_report.encode_to_vec()
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct CompactorTelemetryReport {
    #[serde(flatten)]
    base: TelemetryReportBase,
}

impl CompactorTelemetryReport {
    pub(crate) fn new(tracking_id: String, session_id: String, up_time: u64) -> Self {
        Self {
            base: TelemetryReportBase {
                tracking_id,
                session_id,
                system_data: SystemData::new(),
                up_time,
                time_stamp: current_timestamp(),
                node_type: TelemetryNodeType::Compactor,
                is_test: false,
            },
        }
    }
}

#[cfg(test)]
mod test {
    use risingwave_common::telemetry::pb_compatible::TelemetryToProtobuf;
    use risingwave_common::telemetry::{post_telemetry_report_pb, TELEMETRY_REPORT_URL};

    use crate::telemetry::{CompactorTelemetryReport, TELEMETRY_COMPACTOR_REPORT_TYPE};

    // It is ok to use `TELEMETRY_REPORT_URL` here because we mark it as test and will not write to the database.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn test_compactor_telemetry_report() {
        let mut report = CompactorTelemetryReport::new(
            "7d45669c-08c7-4571-ae3d-d3a3e70a2f7e".to_string(),
            "7d45669c-08c7-4571-ae3d-d3a3e70a2f7e".to_string(),
            100,
        );
        report.base.is_test = true;

        let pb_report = report.to_pb_bytes();
        let url =
            (TELEMETRY_REPORT_URL.to_owned() + "/" + TELEMETRY_COMPACTOR_REPORT_TYPE).to_owned();
        let post_res = post_telemetry_report_pb(&url, pb_report).await;
        assert!(post_res.is_ok());
    }
}

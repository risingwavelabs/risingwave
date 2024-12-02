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

use prost::Message;
use risingwave_common::telemetry::pb_compatible::TelemetryToProtobuf;
use risingwave_common::telemetry::report::TelemetryReportCreator;
use risingwave_common::telemetry::{
    current_timestamp, report_event_common, SystemData, TelemetryNodeType, TelemetryReportBase,
    TelemetryResult,
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
        TELEMETRY_FRONTEND_REPORT_TYPE.to_string(),
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

    use risingwave_common::telemetry::{post_telemetry_report_pb, TELEMETRY_REPORT_URL};

    // It is ok to
    // use `TELEMETRY_REPORT_URL` here because we mark it as test and will not write to the database.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn test_frontend_telemetry_report() {
        let mut report = super::FrontendTelemetryReport::new(
            "7d45669c-08c7-4571-ae3d-d3a3e70a2f7e".to_string(),
            "7d45669c-08c7-4571-ae3d-d3a3e70a2f7e".to_string(),
            100,
        );
        report.base.is_test = true;

        let pb_report = report.to_pb_bytes();
        let url =
            (TELEMETRY_REPORT_URL.to_owned() + "/" + TELEMETRY_FRONTEND_REPORT_TYPE).to_owned();
        let post_res = post_telemetry_report_pb(&url, pb_report).await;
        assert!(post_res.is_ok());
    }
}

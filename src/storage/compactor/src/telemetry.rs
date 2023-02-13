use risingwave_common::telemetry::report::TelemetryReportCreator;
use risingwave_common::telemetry::{
    current_timestamp, SystemData, TelemetryNodeType, TelemetryReport, TelemetryReportBase,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy)]
pub(crate) struct CompactorTelemetryCreator {}

impl CompactorTelemetryCreator {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

impl TelemetryReportCreator for CompactorTelemetryCreator {
    fn create_report(
        &self,
        tracking_id: String,
        session_id: String,
        up_time: u64,
    ) -> anyhow::Result<CompactorTelemetryReport> {
        Ok(CompactorTelemetryReport::new(
            tracking_id,
            session_id,
            up_time,
        ))
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct CompactorTelemetryReport {
    #[serde(flatten)]
    base: TelemetryReportBase,
}

impl TelemetryReport for CompactorTelemetryReport {
    fn to_json(&self) -> anyhow::Result<String> {
        let json = serde_json::to_string(self)?;
        Ok(json)
    }
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
            },
        }
    }
}

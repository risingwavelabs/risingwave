use std::time::SystemTime;

use risingwave_common::telemetry::{
    SystemData, TelemetryNodeType, TelemetryReport, TelemetryReportBase,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub(crate) struct FrontendTelemetryReport {
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
        tracing::info!("create frontend tel report");
        Self {
            base: TelemetryReportBase {
                tracking_id,
                session_id,
                system_data: SystemData::new(),
                up_time,
                time_stamp: SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .expect("Clock might go backward")
                    .as_secs(),
                node_type: TelemetryNodeType::Frontend,
            },
        }
    }
}

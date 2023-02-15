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

impl TelemetryReportCreator for FrontendTelemetryCreator {
    fn create_report(
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

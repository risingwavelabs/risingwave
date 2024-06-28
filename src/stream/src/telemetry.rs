use std::sync::OnceLock;

use risingwave_common::telemetry::report::report_event_common;
use risingwave_pb::telemetry::{PbTelemetryConnectorDirection, PbTelemetryEventStage};

const TELEMETRY_COMPUTE_REPORT_TYPE: &str = "compute";
static COMPUTE_TELEMETRY_SESSION_ID: OnceLock<String> = OnceLock::new();
static COMPUTE_TELEMETRY_TRACKING_ID: OnceLock<String> = OnceLock::new();

pub fn set_compute_telemetry_tracking_id_and_session_id(tracking_id: String, session_id: String) {
    COMPUTE_TELEMETRY_TRACKING_ID.set(tracking_id).unwrap();
    COMPUTE_TELEMETRY_SESSION_ID.set(session_id).unwrap();
}

pub(crate) fn get_compute_telemetry_tracking_id_and_session_id() -> (Option<String>, Option<String>)
{
    (
        COMPUTE_TELEMETRY_TRACKING_ID.get().cloned(),
        COMPUTE_TELEMETRY_SESSION_ID.get().cloned(),
    )
}

pub fn report_event(
    event_stage: PbTelemetryEventStage,
    feature_name: String,
    catalog_id: i64,
    connector_name: Option<String>,
    source_or_sink: Option<PbTelemetryConnectorDirection>,
    attributes: Option<String>, // any json string
) {
    report_event_common(
        Box::new(get_compute_telemetry_tracking_id_and_session_id),
        event_stage,
        feature_name,
        catalog_id,
        connector_name,
        source_or_sink,
        attributes,
        TELEMETRY_COMPUTE_REPORT_TYPE.to_string(),
    );
}

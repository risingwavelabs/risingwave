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

use risingwave_common::telemetry::report::report_event_common;
use risingwave_pb::telemetry::{PbTelemetryDatabaseComponents, PbTelemetryEventStage};

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
    component: Option<PbTelemetryDatabaseComponents>,
    attributes: Option<String>, // any json string
) {
    report_event_common(
        Box::new(get_compute_telemetry_tracking_id_and_session_id),
        event_stage,
        feature_name,
        catalog_id,
        connector_name,
        component,
        attributes,
        TELEMETRY_COMPUTE_REPORT_TYPE.to_string(),
    );
}

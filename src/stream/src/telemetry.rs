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

use risingwave_common::telemetry::report_event_common;
use risingwave_pb::telemetry::{PbTelemetryDatabaseObject, PbTelemetryEventStage};

const TELEMETRY_COMPUTE_REPORT_TYPE: &str = "compute";

pub fn report_event(
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
        TELEMETRY_COMPUTE_REPORT_TYPE.to_owned(),
    );
}

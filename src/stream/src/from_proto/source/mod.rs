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

mod trad_source;

use std::collections::BTreeMap;

use serde_json::json;
pub use trad_source::{create_source_desc_builder, SourceExecutorBuilder};
mod fs_fetch;
pub use fs_fetch::FsFetchExecutorBuilder;
use risingwave_common::catalog::TableId;
use risingwave_connector::source::UPSTREAM_SOURCE_KEY;
use risingwave_pb::catalog::PbStreamSourceInfo;
use risingwave_pb::telemetry::{PbTelemetryConnectorDirection, PbTelemetryEventStage};

use super::*;
use crate::telemetry::report_event;

fn get_connector_name(with_props: &BTreeMap<String, String>) -> String {
    with_props
        .get(UPSTREAM_SOURCE_KEY)
        .map(|s| s.to_lowercase())
        .unwrap_or(String::default())
}

fn telemetry_source_build(
    source_type: &str, // "source" or "source backfill"
    source_id: &TableId,
    source_info: &PbStreamSourceInfo,
    with_props: &BTreeMap<String, String>,
) {
    let attr = json!({"format": source_info.format().as_str_name(), "encode": source_info.row_encode().as_str_name()}).to_string();
    report_event(
        PbTelemetryEventStage::CreateStreamJob,
        source_type.to_string(),
        0,
        Some(get_connector_name(with_props)),
        Some(PbTelemetryConnectorDirection::Source),
        Some(attr),
    )
}

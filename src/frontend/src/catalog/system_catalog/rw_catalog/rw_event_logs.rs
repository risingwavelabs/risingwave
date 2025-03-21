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

use itertools::Itertools;
use risingwave_common::types::{Fields, JsonbVal, Timestamptz};
use risingwave_frontend_macro::system_catalog;
use risingwave_pb::meta::event_log::Event;
use serde_json::json;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

#[derive(Fields)]
struct RwEventLog {
    #[primary_key]
    unique_id: String,
    timestamp: Timestamptz,
    event_type: String,
    info: JsonbVal,
}

#[system_catalog(table, "rw_catalog.rw_event_logs")]
async fn read(reader: &SysCatalogReaderImpl) -> Result<Vec<RwEventLog>> {
    let configs = reader
        .meta_client
        .list_event_log()
        .await?
        .into_iter()
        .sorted_by(|a, b| a.timestamp.cmp(&b.timestamp))
        .map(|mut e| {
            let id = e.unique_id.take().unwrap();
            let ts = Timestamptz::from_millis(e.timestamp.take().unwrap() as i64).unwrap();
            let event_type = event_type(e.event.as_ref().unwrap());
            RwEventLog {
                unique_id: id,
                timestamp: ts,
                event_type: event_type.clone(),
                info: json!(e).into(),
            }
        })
        .collect();
    Ok(configs)
}

fn event_type(e: &Event) -> String {
    match e {
        Event::CreateStreamJobFail(_) => "CREATE_STREAM_JOB_FAIL",
        Event::DirtyStreamJobClear(_) => "DIRTY_STREAM_JOB_CLEAR",
        Event::MetaNodeStart(_) => "META_NODE_START",
        Event::BarrierComplete(_) => "BARRIER_COMPLETE",
        Event::InjectBarrierFail(_) => "INJECT_BARRIER_FAIL",
        Event::CollectBarrierFail(_) => "COLLECT_BARRIER_FAIL",
        Event::WorkerNodePanic(_) => "WORKER_NODE_PANIC",
        Event::AutoSchemaChangeFail(_) => "AUTO_SCHEMA_CHANGE_FAIL",
        Event::SinkFail(_) => "SINK_FAIL",
        Event::Recovery(e) => e.event_type(),
    }
    .into()
}

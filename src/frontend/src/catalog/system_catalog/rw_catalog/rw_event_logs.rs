// Copyright 2023 RisingWave Labs
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
use risingwave_common::catalog::RW_CATALOG_SCHEMA_NAME;
use risingwave_common::error::Result;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, ScalarImpl, Timestamptz};
use risingwave_pb::meta::event_log::Event;
use serde_json::json;

use crate::catalog::system_catalog::{BuiltinTable, SysCatalogReaderImpl};

pub const RW_EVENT_LOGS: BuiltinTable = BuiltinTable {
    name: "rw_event_logs",
    schema: RW_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Varchar, "unique_id"),
        (DataType::Timestamptz, "timestamp"),
        (DataType::Varchar, "event_type"),
        (DataType::Jsonb, "info"),
    ],
    pk: &[0],
};

impl SysCatalogReaderImpl {
    pub async fn read_event_logs(&self) -> Result<Vec<OwnedRow>> {
        let configs = self
            .meta_client
            .list_event_log()
            .await?
            .into_iter()
            .sorted_by(|a, b| a.timestamp.cmp(&b.timestamp))
            .map(|mut e| {
                let id = e.unique_id.take().unwrap().into();
                let ts = Timestamptz::from_millis(e.timestamp.take().unwrap() as i64).unwrap();
                let event_type = event_type(e.event.as_ref().unwrap());
                OwnedRow::new(vec![
                    Some(ScalarImpl::Utf8(id)),
                    Some(ScalarImpl::Timestamptz(ts)),
                    Some(ScalarImpl::Utf8(event_type.into())),
                    Some(ScalarImpl::Jsonb(json!(e).into())),
                ])
            })
            .collect_vec();
        Ok(configs)
    }
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
    }
    .into()
}

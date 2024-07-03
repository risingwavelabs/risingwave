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

use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

#[derive(Fields)]
#[primary_key(worker_id, slot_id)]
struct RwWorkerSlot {
    slot_id: i32,
    worker_id: i32,
}

#[system_catalog(table, "rw_catalog.rw_worker_slots")]
fn read_rw_worker_slots(reader: &SysCatalogReaderImpl) -> Result<Vec<RwWorkerSlot>> {
    let workers = reader.worker_node_manager.list_worker_nodes();
    Ok(workers
        .into_iter()
        .flat_map(|worker| {
            (0..worker.parallelism).map(move |slot_id| RwWorkerSlot {
                slot_id: slot_id as _,
                worker_id: worker.id as _,
            })
        })
        .collect())
}

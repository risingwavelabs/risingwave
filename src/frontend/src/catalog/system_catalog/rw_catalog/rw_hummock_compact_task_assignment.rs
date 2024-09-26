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

use risingwave_common::types::{Fields, JsonbVal};
use risingwave_frontend_macro::system_catalog;
use serde_json::json;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

#[derive(Fields)]
struct RwHummockCompactTaskAssignment {
    #[primary_key]
    compaction_group_id: i64,
    task_id: i64,
    select_level: i32,
    target_level: i32,
    task_type: i32,
    task_status: i32,
    base_level: i32,
    gc_delete_keys: bool,
    target_file_size: i64,
    target_sub_level_id: i64,
    compression_algorithm: i32,
    table_ids: JsonbVal,
}

#[system_catalog(table, "rw_catalog.rw_hummock_compact_task_assignment")]
async fn read(reader: &SysCatalogReaderImpl) -> Result<Vec<RwHummockCompactTaskAssignment>> {
    // The naming of compact_task_assignment is due to versioning; now compact_task_assignment only records the state of the compact task
    let compact_task_assignments = reader.meta_client.list_compact_task_assignment().await?;

    let mut rows = vec![];
    for compact_task_assignment in compact_task_assignments {
        let compact_task = compact_task_assignment.compact_task.unwrap();

        let select_level = compact_task.input_ssts[0].level_idx;

        rows.push(RwHummockCompactTaskAssignment {
            compaction_group_id: compact_task.compaction_group_id as _,
            task_id: compact_task.task_id as _,
            select_level: select_level as _,
            target_level: compact_task.target_level as _,
            task_type: compact_task.task_type as _,
            task_status: compact_task.task_status as _,
            base_level: compact_task.base_level as _,
            gc_delete_keys: compact_task.gc_delete_keys as _,
            target_file_size: compact_task.target_file_size as _,
            target_sub_level_id: compact_task.target_sub_level_id as _,
            compression_algorithm: compact_task.compression_algorithm as _,
            table_ids: json!(compact_task.existing_table_ids).into(),
        });
    }

    Ok(rows)
}

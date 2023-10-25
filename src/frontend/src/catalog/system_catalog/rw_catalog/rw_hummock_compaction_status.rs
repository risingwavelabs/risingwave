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

use risingwave_common::catalog::RW_CATALOG_SCHEMA_NAME;
use risingwave_common::error::Result;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_pb::hummock::CompactTaskAssignment;
use serde_json::json;

use crate::catalog::system_catalog::{BuiltinTable, SysCatalogReaderImpl};

pub const RW_HUMMOCK_COMPACTION_STATUS: BuiltinTable = BuiltinTable {
    name: "rw_hummock_compaction_status",
    schema: RW_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Int64, "compaction_group_id"),
        (DataType::Int64, "task_id"),
        (DataType::Int32, "select_level"),
        (DataType::Int32, "target_level"),
        (DataType::Int32, "task_type"),
        (DataType::Int32, "task_status"),
        (DataType::Int64, "watermark"),
        (DataType::Int32, "base_level"),
        (DataType::Boolean, "gc_delete_keys"),
        (DataType::Int64, "target_file_size"),
        (DataType::Int64, "target_sub_level_id"),
        (DataType::Jsonb, "compact_task"),
    ],
    pk: &[0],
};

impl SysCatalogReaderImpl {
    pub async fn read_hummock_compaction_status(&self) -> Result<Vec<OwnedRow>> {
        let (_compaction_status, compaction_assignment, _compaction_progress) =
            self.meta_client.list_compaction_status().await?;
        Ok(assignments_to_rows(compaction_assignment))
    }
}

fn assignments_to_rows(assignments: Vec<CompactTaskAssignment>) -> Vec<OwnedRow> {
    let mut rows = vec![];
    for assignment in assignments {
        let compact_task = assignment.compact_task.unwrap();

        let select_level = compact_task.input_ssts[0].level_idx;

        rows.push(OwnedRow::new(vec![
            Some(ScalarImpl::Int64(compact_task.compaction_group_id as _)),
            Some(ScalarImpl::Int64(compact_task.task_id as _)),
            Some(ScalarImpl::Int32(select_level as _)),
            Some(ScalarImpl::Int32(compact_task.target_level as _)),
            Some(ScalarImpl::Int32(compact_task.task_type as _)),
            Some(ScalarImpl::Int32(compact_task.task_status as _)),
            Some(ScalarImpl::Int64(compact_task.watermark as _)),
            Some(ScalarImpl::Int32(compact_task.base_level as _)),
            Some(ScalarImpl::Bool(compact_task.gc_delete_keys as _)),
            Some(ScalarImpl::Int64(compact_task.target_file_size as _)),
            Some(ScalarImpl::Int64(compact_task.target_sub_level_id as _)),
            Some(ScalarImpl::Jsonb(json!(compact_task).into())),
        ]));
    }

    rows
}

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
use risingwave_pb::hummock::CompactTaskProgress;

use crate::catalog::system_catalog::{BuiltinTable, SysCatalogReaderImpl};

pub const RW_HUMMOCK_COMPACT_TASK_PROGRESS: BuiltinTable = BuiltinTable {
    name: "rw_hummock_compact_task_progress",
    schema: RW_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Int64, "compaction_group_id"),
        (DataType::Int64, "task_id"),
        (DataType::Int32, "num_ssts_sealed"),
        (DataType::Int32, "num_ssts_uploaded"),
        (DataType::Int64, "num_progress_key"),
        (DataType::Int64, "num_pending_read_io"),
        (DataType::Int64, "num_pending_write_io"),
    ],
    pk: &[0],
};

impl SysCatalogReaderImpl {
    pub async fn read_hummock_compact_task_progress(&self) -> Result<Vec<OwnedRow>> {
        let compact_task_progress = self.meta_client.list_compact_task_progress().await?;
        Ok(compact_task_progress_to_rows(compact_task_progress))
    }
}

fn compact_task_progress_to_rows(
    compact_task_progress_vec: Vec<CompactTaskProgress>,
) -> Vec<OwnedRow> {
    let mut rows = vec![];
    for compact_task_progress in compact_task_progress_vec {
        rows.push(OwnedRow::new(vec![
            Some(ScalarImpl::Int64(
                compact_task_progress
                    .compaction_group_id
                    .unwrap_or_default() as _,
            )),
            Some(ScalarImpl::Int64(compact_task_progress.task_id as _)),
            Some(ScalarImpl::Int32(
                compact_task_progress.num_ssts_sealed as _,
            )),
            Some(ScalarImpl::Int32(
                compact_task_progress.num_ssts_uploaded as _,
            )),
            Some(ScalarImpl::Int64(
                compact_task_progress.num_progress_key as _,
            )),
            Some(ScalarImpl::Int64(
                compact_task_progress.num_pending_read_io as _,
            )),
            Some(ScalarImpl::Int64(
                compact_task_progress.num_pending_write_io as _,
            )),
        ]));
    }

    rows
}

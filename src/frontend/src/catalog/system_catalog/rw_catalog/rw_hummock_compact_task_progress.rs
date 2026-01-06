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

use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

#[derive(Fields)]
struct RwHummockCompactTaskProgress {
    #[primary_key]
    compaction_group_id: i64,
    task_id: i64,
    num_ssts_sealed: i32,
    num_ssts_uploaded: i32,
    num_progress_key: i64,
    num_pending_read_io: i64,
    num_pending_write_io: i64,
}

#[system_catalog(table, "rw_catalog.rw_hummock_compact_task_progress")]
async fn read(reader: &SysCatalogReaderImpl) -> Result<Vec<RwHummockCompactTaskProgress>> {
    let compact_task_progress = reader.meta_client.list_compact_task_progress().await?;

    let mut rows = vec![];
    for p in compact_task_progress {
        rows.push(RwHummockCompactTaskProgress {
            compaction_group_id: p.compaction_group_id.unwrap_or_default() as _,
            task_id: p.task_id as _,
            num_ssts_sealed: p.num_ssts_sealed as _,
            num_ssts_uploaded: p.num_ssts_uploaded as _,
            num_progress_key: p.num_progress_key as _,
            num_pending_read_io: p.num_pending_read_io as _,
            num_pending_write_io: p.num_pending_write_io as _,
        });
    }
    Ok(rows)
}

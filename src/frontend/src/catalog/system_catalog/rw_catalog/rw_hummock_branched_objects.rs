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
#[primary_key(object_id, compaction_group_id)]
struct RwHummockBranchedObject {
    object_id: i64,
    compaction_group_id: i64,
    sst_id: Vec<i64>,
}

#[system_catalog(table, "rw_catalog.rw_hummock_branched_objects")]
async fn read(reader: &SysCatalogReaderImpl) -> Result<Vec<RwHummockBranchedObject>> {
    let branched_objects = reader.meta_client.list_branched_objects().await?;
    let rows = branched_objects
        .into_iter()
        .map(|o| RwHummockBranchedObject {
            object_id: o.object_id as _,
            sst_id: o.sst_id.into_iter().map(|id| id as _).collect(),
            compaction_group_id: o.compaction_group_id as _,
        })
        .collect();
    Ok(rows)
}

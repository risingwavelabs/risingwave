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

use risingwave_common::types::{Fields, JsonbVal};
use risingwave_frontend_macro::system_catalog;
use serde_json::json;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

#[derive(Fields)]
struct RwMetaSnapshot {
    #[primary_key]
    meta_snapshot_id: i64,
    hummock_version_id: i64,
    remarks: Option<String>,
    state_table_info: Option<JsonbVal>,
    rw_version: Option<String>,
}

#[system_catalog(table, "rw_catalog.rw_meta_snapshot")]
async fn read_meta_snapshot(reader: &SysCatalogReaderImpl) -> Result<Vec<RwMetaSnapshot>> {
    let meta_snapshots = reader
        .meta_client
        .list_meta_snapshots()
        .await?
        .into_iter()
        .map(|s| RwMetaSnapshot {
            meta_snapshot_id: s.id as _,
            hummock_version_id: s.hummock_version_id as _,
            remarks: s.remarks,
            state_table_info: Some(json!(s.state_table_info).into()),
            rw_version: s.rw_version,
        })
        .collect();
    Ok(meta_snapshots)
}

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
use risingwave_common::types::{Fields, JsonbVal};
use risingwave_frontend_macro::system_catalog;
use serde_json::json;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

#[derive(Fields)]
struct RwHummockCompactionGroupConfig {
    #[primary_key]
    id: i64,
    parent_id: Option<i64>,
    member_tables: Option<JsonbVal>,
    compaction_config: Option<JsonbVal>,
    active_write_limit: Option<JsonbVal>,
}

#[system_catalog(table, "rw_catalog.rw_hummock_compaction_group_configs")]
async fn read(reader: &SysCatalogReaderImpl) -> Result<Vec<RwHummockCompactionGroupConfig>> {
    let info = reader
        .meta_client
        .list_hummock_compaction_group_configs()
        .await?;
    let mut write_limits = reader
        .meta_client
        .list_hummock_active_write_limits()
        .await?;
    let mut rows = info
        .into_iter()
        .map(|i| RwHummockCompactionGroupConfig {
            id: i.id.as_raw_id() as _,
            parent_id: Some(i.parent_id.as_raw_id() as _),
            member_tables: Some(json!(i.member_table_ids).into()),
            compaction_config: Some(json!(i.compaction_config).into()),
            active_write_limit: write_limits.remove(&i.id).map(|w| json!(w).into()),
        })
        .collect_vec();
    // As compaction group configs and active write limits are fetched via two RPCs, it's possible there's inconsistency.
    // Just leave unknown field blank.
    rows.extend(
        write_limits
            .into_iter()
            .map(|(cg, w)| RwHummockCompactionGroupConfig {
                id: cg.as_raw_id() as _,
                parent_id: None,
                member_tables: None,
                compaction_config: None,
                active_write_limit: Some(json!(w).into()),
            }),
    );
    Ok(rows)
}

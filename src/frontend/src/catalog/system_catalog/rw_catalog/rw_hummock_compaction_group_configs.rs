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
use risingwave_common::types::{DataType, ScalarImpl};
use serde_json::json;

use crate::catalog::system_catalog::{BuiltinTable, SysCatalogReaderImpl};

pub const RW_HUMMOCK_COMPACTION_GROUP_CONFIGS: BuiltinTable = BuiltinTable {
    name: "rw_hummock_compaction_group_configs",
    schema: RW_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Int64, "id"),
        (DataType::Int64, "parent_id"),
        (DataType::Jsonb, "member_tables"),
        (DataType::Jsonb, "compaction_config"),
        (DataType::Jsonb, "active_write_limit"),
    ],
    pk: &[0],
};

impl SysCatalogReaderImpl {
    pub async fn read_hummock_compaction_group_configs(&self) -> Result<Vec<OwnedRow>> {
        let info = self
            .meta_client
            .list_hummock_compaction_group_configs()
            .await?;
        let mut write_limits = self.meta_client.list_hummock_active_write_limits().await?;
        let mut rows = info
            .into_iter()
            .map(|i| {
                let active_write_limit = write_limits
                    .remove(&i.id)
                    .map(|w| ScalarImpl::Jsonb(json!(w).into()));
                OwnedRow::new(vec![
                    Some(ScalarImpl::Int64(i.id as _)),
                    Some(ScalarImpl::Int64(i.parent_id as _)),
                    Some(ScalarImpl::Jsonb(json!(i.member_table_ids).into())),
                    Some(ScalarImpl::Jsonb(json!(i.compaction_config).into())),
                    active_write_limit,
                ])
            })
            .collect_vec();
        // As compaction group configs and active write limits are fetched via two RPCs, it's possible there's inconsistency.
        // Just leave unknown field blank.
        rows.extend(write_limits.into_iter().map(|(cg, w)| {
            OwnedRow::new(vec![
                Some(ScalarImpl::Int64(cg as _)),
                None,
                None,
                None,
                Some(ScalarImpl::Jsonb(json!(w).into())),
            ])
        }));
        Ok(rows)
    }
}

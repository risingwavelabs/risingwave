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
use serde_json::json;

use crate::catalog::system_catalog::{BuiltinTable, SysCatalogReaderImpl};

pub const RW_HUMMOCK_VERSION_DELTAS: BuiltinTable = BuiltinTable {
    name: "rw_hummock_version_deltas",
    schema: RW_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Int64, "id"),
        (DataType::Int64, "prev_id"),
        (DataType::Int64, "max_committed_epoch"),
        (DataType::Int64, "safe_epoch"),
        (DataType::Boolean, "trivial_move"),
        (DataType::Jsonb, "gc_object_ids"),
        (DataType::Jsonb, "group_deltas"),
    ],
    pk: &[0],
};

impl SysCatalogReaderImpl {
    pub async fn read_hummock_version_deltas(&self) -> Result<Vec<OwnedRow>> {
        let deltas = self.meta_client.list_version_deltas().await?;
        let rows = deltas
            .into_iter()
            .map(|d| {
                OwnedRow::new(vec![
                    Some(ScalarImpl::Int64(d.id as _)),
                    Some(ScalarImpl::Int64(d.prev_id as _)),
                    Some(ScalarImpl::Int64(d.max_committed_epoch as _)),
                    Some(ScalarImpl::Int64(d.safe_epoch as _)),
                    Some(ScalarImpl::Bool(d.trivial_move)),
                    Some(ScalarImpl::Jsonb(json!(d.gc_object_ids).into())),
                    Some(ScalarImpl::Jsonb(json!(d.group_deltas).into())),
                ])
            })
            .collect();
        Ok(rows)
    }
}

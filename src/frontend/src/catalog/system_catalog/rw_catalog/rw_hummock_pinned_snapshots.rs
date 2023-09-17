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

use crate::catalog::system_catalog::{BuiltinTable, SysCatalogReaderImpl};

pub const RW_HUMMOCK_PINNED_SNAPSHOTS: BuiltinTable = BuiltinTable {
    name: "rw_hummock_pinned_snapshots",
    schema: RW_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Int32, "worker_node_id"),
        (DataType::Int64, "min_pinned_snapshot_id"),
    ],
    pk: &[],
};

impl SysCatalogReaderImpl {
    pub async fn read_hummock_pinned_snapshots(&self) -> Result<Vec<OwnedRow>> {
        let pinned_snapshots = self
            .meta_client
            .list_hummock_pinned_snapshots()
            .await?
            .into_iter()
            .map(|s| {
                OwnedRow::new(vec![
                    Some(ScalarImpl::Int32(s.0 as i32)),
                    Some(ScalarImpl::Int64(s.1 as i64)),
                ])
            })
            .collect_vec();
        Ok(pinned_snapshots)
    }
}

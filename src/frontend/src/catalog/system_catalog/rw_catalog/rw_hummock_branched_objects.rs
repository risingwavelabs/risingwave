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

use crate::catalog::system_catalog::{BuiltinTable, SysCatalogReaderImpl};

pub const RW_HUMMOCK_BRANCHED_OBJECTS: BuiltinTable = BuiltinTable {
    name: "rw_hummock_branched_objects",
    schema: RW_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Int64, "object_id"),
        (DataType::Int64, "sst_id"),
        (DataType::Int64, "compaction_group_id"),
    ],
    pk: &[],
};

impl SysCatalogReaderImpl {
    pub async fn read_hummock_branched_objects(&self) -> Result<Vec<OwnedRow>> {
        let branched_objects = self.meta_client.list_branched_objects().await?;
        let rows = branched_objects
            .into_iter()
            .map(|o| {
                OwnedRow::new(vec![
                    Some(ScalarImpl::Int64(o.object_id as _)),
                    Some(ScalarImpl::Int64(o.sst_id as _)),
                    Some(ScalarImpl::Int64(o.compaction_group_id as _)),
                ])
            })
            .collect();
        Ok(rows)
    }
}

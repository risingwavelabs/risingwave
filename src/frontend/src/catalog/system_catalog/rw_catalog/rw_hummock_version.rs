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
use risingwave_pb::hummock::HummockVersion;
use serde_json::json;

use crate::catalog::system_catalog::{BuiltinTable, SysCatalogReaderImpl};

pub const RW_HUMMOCK_CURRENT_VERSION: BuiltinTable = BuiltinTable {
    name: "rw_hummock_current_version",
    schema: RW_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Int64, "version_id"),
        (DataType::Int64, "max_committed_epoch"),
        (DataType::Int64, "safe_epoch"),
        (DataType::Jsonb, "compaction_group"),
    ],
    pk: &[],
};

pub const RW_HUMMOCK_CHECKPOINT_VERSION: BuiltinTable = BuiltinTable {
    name: "rw_hummock_checkpoint_version",
    schema: RW_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Int64, "version_id"),
        (DataType::Int64, "max_committed_epoch"),
        (DataType::Int64, "safe_epoch"),
        (DataType::Jsonb, "compaction_group"),
    ],
    pk: &[],
};

impl SysCatalogReaderImpl {
    pub async fn read_hummock_current_version(&self) -> Result<Vec<OwnedRow>> {
        let version = self.meta_client.get_hummock_current_version().await?;
        Ok(version_to_rows(&version))
    }

    pub async fn read_hummock_checkpoint_version(&self) -> Result<Vec<OwnedRow>> {
        let version = self.meta_client.get_hummock_checkpoint_version().await?;
        Ok(version_to_rows(&version))
    }
}

fn version_to_rows(version: &HummockVersion) -> Vec<OwnedRow> {
    version
        .levels
        .values()
        .map(|cg| {
            OwnedRow::new(vec![
                Some(ScalarImpl::Int64(version.id as _)),
                Some(ScalarImpl::Int64(version.max_committed_epoch as _)),
                Some(ScalarImpl::Int64(version.safe_epoch as _)),
                // FIXME #8612: The byte array key_range is encoded to a string by serde_json. We need disable this behavior as it makes it harder to understand the key range.
                Some(ScalarImpl::Jsonb(json!(cg).into())),
            ])
        })
        .collect()
}

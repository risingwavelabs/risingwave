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

pub const RW_HUMMOCK_SSTABLES: BuiltinTable = BuiltinTable {
    name: "rw_hummock_sstables",
    schema: RW_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Int64, "sstable_id"),
        (DataType::Int64, "object_id"),
        (DataType::Int64, "compaction_group_id"),
        (DataType::Int32, "level_id"),
        (DataType::Int64, "sub_level_id"),
        (DataType::Int32, "level_type"),
        (DataType::Bytea, "key_range_left"),
        (DataType::Bytea, "key_range_right"),
        (DataType::Boolean, "right_exclusive"),
        (DataType::Int64, "file_size"),
        (DataType::Int64, "meta_offset"),
        (DataType::Int64, "stale_key_count"),
        (DataType::Int64, "total_key_count"),
        (DataType::Int64, "min_epoch"),
        (DataType::Int64, "max_epoch"),
        (DataType::Int64, "uncompressed_file_size"),
        (DataType::Int64, "range_tombstone_count"),
        (DataType::Int32, "bloom_filter_kind"),
        (DataType::Jsonb, "table_ids"),
    ],
    pk: &[0],
};

impl SysCatalogReaderImpl {
    pub async fn read_hummock_current_version(&self) -> Result<Vec<OwnedRow>> {
        let version = self.meta_client.get_hummock_current_version().await?;
        Ok(version_to_compaction_group_rows(
            &remove_key_range_from_version(version),
        ))
    }

    pub async fn read_hummock_checkpoint_version(&self) -> Result<Vec<OwnedRow>> {
        let version = self.meta_client.get_hummock_checkpoint_version().await?;
        Ok(version_to_compaction_group_rows(
            &remove_key_range_from_version(version),
        ))
    }

    pub async fn read_hummock_sstables(&self) -> Result<Vec<OwnedRow>> {
        let version = self.meta_client.get_hummock_current_version().await?;
        Ok(version_to_sstable_rows(version))
    }
}

fn remove_key_range_from_version(mut version: HummockVersion) -> HummockVersion {
    // Because key range is too verbose for manual analysis, just don't expose it.
    for cg in version.levels.values_mut() {
        for level in cg
            .levels
            .iter_mut()
            .chain(cg.l0.as_mut().unwrap().sub_levels.iter_mut())
        {
            for sst in &mut level.table_infos {
                sst.key_range.take();
            }
        }
    }
    version
}

fn version_to_compaction_group_rows(version: &HummockVersion) -> Vec<OwnedRow> {
    version
        .levels
        .values()
        .map(|cg| {
            OwnedRow::new(vec![
                Some(ScalarImpl::Int64(version.id as _)),
                Some(ScalarImpl::Int64(version.max_committed_epoch as _)),
                Some(ScalarImpl::Int64(version.safe_epoch as _)),
                Some(ScalarImpl::Jsonb(json!(cg).into())),
            ])
        })
        .collect()
}

fn version_to_sstable_rows(version: HummockVersion) -> Vec<OwnedRow> {
    let mut sstables = vec![];
    for cg in version.levels.into_values() {
        for level in cg.levels.into_iter().chain(cg.l0.unwrap().sub_levels) {
            for sst in level.table_infos {
                let key_range = sst.key_range.unwrap();
                let sub_level_id = if level.level_idx > 0 {
                    None
                } else {
                    Some(ScalarImpl::Int64(level.sub_level_id as _))
                };
                sstables.push(OwnedRow::new(vec![
                    Some(ScalarImpl::Int64(sst.sst_id as _)),
                    Some(ScalarImpl::Int64(sst.object_id as _)),
                    Some(ScalarImpl::Int64(cg.group_id as _)),
                    Some(ScalarImpl::Int32(level.level_idx as _)),
                    sub_level_id,
                    Some(ScalarImpl::Int32(level.level_type as _)),
                    Some(ScalarImpl::Bytea(key_range.left.into())),
                    Some(ScalarImpl::Bytea(key_range.right.into())),
                    Some(ScalarImpl::Bool(key_range.right_exclusive)),
                    Some(ScalarImpl::Int64(sst.file_size as _)),
                    Some(ScalarImpl::Int64(sst.meta_offset as _)),
                    Some(ScalarImpl::Int64(sst.stale_key_count as _)),
                    Some(ScalarImpl::Int64(sst.total_key_count as _)),
                    Some(ScalarImpl::Int64(sst.min_epoch as _)),
                    Some(ScalarImpl::Int64(sst.max_epoch as _)),
                    Some(ScalarImpl::Int64(sst.uncompressed_file_size as _)),
                    Some(ScalarImpl::Int64(sst.range_tombstone_count as _)),
                    Some(ScalarImpl::Int32(sst.bloom_filter_kind as _)),
                    Some(ScalarImpl::Jsonb(json!(sst.table_ids).into())),
                ]));
            }
        }
    }
    sstables
}

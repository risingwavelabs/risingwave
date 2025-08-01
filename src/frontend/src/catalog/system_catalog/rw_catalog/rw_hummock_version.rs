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

use std::collections::HashMap;
use std::collections::hash_map::Entry;

use itertools::Itertools;
use risingwave_common::types::{Fields, JsonbVal};
use risingwave_frontend_macro::system_catalog;
use risingwave_hummock_sdk::version::HummockVersion;
use serde_json::json;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

#[derive(Fields)]
struct RwHummockVersion {
    #[primary_key]
    version_id: i64,
    compaction_group: JsonbVal,
}

#[derive(Fields)]
struct RwHummockSstable {
    #[primary_key]
    sstable_id: i64,
    object_id: i64,
    compaction_group_id: i64,
    level_id: i32,
    sub_level_id: Option<i64>,
    level_type: i32,
    key_range_left: Vec<u8>,
    key_range_right: Vec<u8>,
    right_exclusive: bool,
    file_size: i64,
    meta_offset: i64,
    stale_key_count: i64,
    total_key_count: i64,
    min_epoch: i64,
    max_epoch: i64,
    uncompressed_file_size: i64,
    range_tombstone_count: i64,
    bloom_filter_kind: i32,
    table_ids: JsonbVal,
    sst_size: i64,
}

#[system_catalog(table, "rw_catalog.rw_hummock_current_version")]
async fn read_hummock_current_version(
    reader: &SysCatalogReaderImpl,
) -> Result<Vec<RwHummockVersion>> {
    let version = reader.meta_client.get_hummock_current_version().await?;
    Ok(version_to_compaction_group_rows(
        &remove_key_range_from_version(version),
    ))
}

#[system_catalog(table, "rw_catalog.rw_hummock_checkpoint_version")]
async fn read_hummock_checkpoint_version(
    reader: &SysCatalogReaderImpl,
) -> Result<Vec<RwHummockVersion>> {
    let version = reader.meta_client.get_hummock_checkpoint_version().await?;
    Ok(version_to_compaction_group_rows(
        &remove_key_range_from_version(version),
    ))
}

#[system_catalog(table, "rw_catalog.rw_hummock_sstables")]
async fn read_hummock_sstables(reader: &SysCatalogReaderImpl) -> Result<Vec<RwHummockSstable>> {
    let version = reader.meta_client.get_hummock_current_version().await?;
    Ok(version_to_sstable_rows(version))
}

fn remove_key_range_from_version(mut version: HummockVersion) -> HummockVersion {
    // Because key range is too verbose for manual analysis, just don't expose it.
    for cg in version.levels.values_mut() {
        for level in cg.levels.iter_mut().chain(cg.l0.sub_levels.iter_mut()) {
            for sst in &mut level.table_infos {
                sst.remove_key_range();
            }
        }
    }
    version
}

fn version_to_compaction_group_rows(version: &HummockVersion) -> Vec<RwHummockVersion> {
    version
        .levels
        .values()
        .map(|cg| RwHummockVersion {
            version_id: version.id.to_u64() as _,
            compaction_group: json!(cg.to_protobuf()).into(),
        })
        .collect()
}

fn version_to_sstable_rows(version: HummockVersion) -> Vec<RwHummockSstable> {
    let mut sstables = vec![];
    for cg in version.levels.into_values() {
        for level in cg.levels.into_iter().chain(cg.l0.sub_levels) {
            for sst in level.table_infos {
                let key_range = sst.key_range.clone();
                sstables.push(RwHummockSstable {
                    sstable_id: sst.sst_id.inner() as _,
                    object_id: sst.object_id.inner() as _,
                    compaction_group_id: cg.group_id as _,
                    level_id: level.level_idx as _,
                    sub_level_id: (level.level_idx == 0).then_some(level.sub_level_id as _),
                    level_type: level.level_type as _,
                    key_range_left: key_range.left.to_vec(),
                    key_range_right: key_range.right.to_vec(),
                    right_exclusive: key_range.right_exclusive,
                    file_size: sst.file_size as _,
                    meta_offset: sst.meta_offset as _,
                    stale_key_count: sst.stale_key_count as _,
                    total_key_count: sst.total_key_count as _,
                    min_epoch: sst.min_epoch as _,
                    max_epoch: sst.max_epoch as _,
                    uncompressed_file_size: sst.uncompressed_file_size as _,
                    range_tombstone_count: sst.range_tombstone_count as _,
                    bloom_filter_kind: sst.bloom_filter_kind as _,
                    table_ids: json!(sst.table_ids).into(),
                    sst_size: sst.sst_size as _,
                });
            }
        }
    }
    sstables
}

#[derive(Fields)]
struct RwHummockTableWatermark {
    #[primary_key]
    table_id: i32,
    #[primary_key]
    vnode_id: i16,
    epoch: i64,
    watermark: Vec<u8>,
    direction: String,
}

#[system_catalog(table, "rw_catalog.rw_hummock_table_watermark")]
async fn read_hummock_table_watermarks(
    reader: &SysCatalogReaderImpl,
) -> Result<Vec<RwHummockTableWatermark>> {
    let version = reader.meta_client.get_hummock_current_version().await?;
    Ok(version
        .table_watermarks
        .into_iter()
        .flat_map(|(table_id, table_watermarks)| {
            let mut vnode_watermark_map = HashMap::new();
            for (vnode, epoch, watermark) in
                table_watermarks
                    .watermarks
                    .iter()
                    .flat_map(move |(epoch, watermarks)| {
                        watermarks.iter().flat_map(move |vnode_watermark| {
                            let watermark = vnode_watermark.watermark().clone();
                            let vnodes = vnode_watermark.vnode_bitmap().iter_ones().collect_vec();
                            vnodes
                                .into_iter()
                                .map(move |vnode| (vnode, *epoch, Vec::from(watermark.as_ref())))
                        })
                    })
            {
                match vnode_watermark_map.entry(vnode) {
                    Entry::Occupied(mut entry) => {
                        let (prev_epoch, prev_watermark) = entry.get_mut();
                        if epoch > *prev_epoch {
                            *prev_watermark = watermark;
                        }
                    }
                    Entry::Vacant(entry) => {
                        entry.insert((epoch, watermark));
                    }
                }
            }
            vnode_watermark_map
                .into_iter()
                .map(move |(vnode, (epoch, watermark))| RwHummockTableWatermark {
                    table_id: table_id.table_id as _,
                    vnode_id: vnode as _,
                    epoch: epoch as _,
                    watermark,
                    direction: table_watermarks.direction.to_string(),
                })
        })
        .collect())
}

#[derive(Fields)]
struct RwHummockSnapshot {
    #[primary_key]
    table_id: i32,
    committed_epoch: i64,
}

#[system_catalog(table, "rw_catalog.rw_hummock_snapshot")]
async fn read_hummock_snapshot_groups(
    reader: &SysCatalogReaderImpl,
) -> Result<Vec<RwHummockSnapshot>> {
    let version = reader.meta_client.get_hummock_current_version().await?;
    Ok(version
        .state_table_info
        .info()
        .iter()
        .map(|(table_id, info)| RwHummockSnapshot {
            table_id: table_id.table_id as _,
            committed_epoch: info.committed_epoch as _,
        })
        .collect())
}

#[derive(Fields)]
struct RwHummockTableChangeLog {
    #[primary_key]
    table_id: i32,
    change_log: JsonbVal,
}

#[system_catalog(table, "rw_catalog.rw_hummock_table_change_log")]
async fn read_hummock_table_change_log(
    reader: &SysCatalogReaderImpl,
) -> Result<
    Vec<crate::catalog::system_catalog::rw_catalog::rw_hummock_version::RwHummockTableChangeLog>,
> {
    let version = reader.meta_client.get_hummock_current_version().await?;
    Ok(version
        .table_change_log
        .iter()
        .map(|(table_id, change_log)| RwHummockTableChangeLog {
            table_id: table_id.table_id as i32,
            change_log: json!(change_log.to_protobuf()).into(),
        })
        .collect())
}

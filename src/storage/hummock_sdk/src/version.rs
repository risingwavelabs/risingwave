// Copyright 2024 RisingWave Labs
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

use std::collections::{BTreeMap, HashMap};
use std::mem::size_of;

use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_pb::hummock::compact_task::{PbTaskStatus, PbTaskType, TaskStatus, TaskType};
use risingwave_pb::hummock::hummock_version::PbLevels;
use risingwave_pb::hummock::hummock_version_delta::PbGroupDeltas;
use risingwave_pb::hummock::{
    BloomFilterType, LevelType, PbCompactTask, PbHummockVersion, PbHummockVersionDelta,
    PbInputLevel, PbKeyRange, PbLevel, PbLevelType, PbOverlappingLevel, PbSstableInfo,
    PbValidationTask, TableOption, TableSchema,
};
use serde::Serialize;

use crate::key_range::KeyRange;
use crate::table_watermark::TableWatermarks;
use crate::{CompactionGroupId, HummockSstableObjectId, ProtoSerializeSizeEstimatedExt};

#[derive(Debug, Clone, PartialEq, Default, Serialize)]
pub struct OverlappingLevel {
    pub sub_levels: Vec<Level>,
    pub total_file_size: u64,
    pub uncompressed_file_size: u64,
}

impl From<&PbOverlappingLevel> for OverlappingLevel {
    fn from(pb_overlapping_level: &PbOverlappingLevel) -> Self {
        Self {
            sub_levels: pb_overlapping_level
                .sub_levels
                .iter()
                .map(Level::from)
                .collect_vec(),
            total_file_size: pb_overlapping_level.total_file_size,
            uncompressed_file_size: pb_overlapping_level.uncompressed_file_size,
        }
    }
}

impl From<&OverlappingLevel> for PbOverlappingLevel {
    fn from(overlapping_level: &OverlappingLevel) -> Self {
        Self {
            sub_levels: overlapping_level
                .sub_levels
                .iter()
                .map(|pb_level| pb_level.into())
                .collect_vec(),
            total_file_size: overlapping_level.total_file_size,
            uncompressed_file_size: overlapping_level.uncompressed_file_size,
        }
    }
}

impl From<OverlappingLevel> for PbOverlappingLevel {
    fn from(overlapping_level: OverlappingLevel) -> Self {
        Self {
            sub_levels: overlapping_level
                .sub_levels
                .into_iter()
                .map(|pb_level| pb_level.into())
                .collect_vec(),
            total_file_size: overlapping_level.total_file_size,
            uncompressed_file_size: overlapping_level.uncompressed_file_size,
        }
    }
}

impl From<PbOverlappingLevel> for OverlappingLevel {
    fn from(pb_overlapping_level: PbOverlappingLevel) -> Self {
        Self {
            sub_levels: pb_overlapping_level
                .sub_levels
                .into_iter()
                .map(Level::from)
                .collect_vec(),
            total_file_size: pb_overlapping_level.total_file_size,
            uncompressed_file_size: pb_overlapping_level.uncompressed_file_size,
        }
    }
}

impl ProtoSerializeSizeEstimatedExt for OverlappingLevel {
    fn estimated_encode_len(&self) -> usize {
        self.sub_levels
            .iter()
            .map(|level| level.estimated_encode_len())
            .sum::<usize>()
            + size_of::<u64>()
            + size_of::<u64>()
    }
}

impl OverlappingLevel {
    pub fn get_sub_levels(&self) -> &Vec<Level> {
        &self.sub_levels
    }
}

#[derive(Debug, Clone, PartialEq, Default, Serialize)]
pub struct Level {
    pub level_idx: u32,
    pub level_type: LevelType,
    pub table_infos: Vec<SstableInfo>,
    pub total_file_size: u64,
    pub sub_level_id: u64,
    pub uncompressed_file_size: u64,
    pub vnode_partition_count: u32,
}

impl From<&PbLevel> for Level {
    fn from(pb_level: &PbLevel) -> Self {
        Self {
            level_idx: pb_level.level_idx,
            level_type: LevelType::try_from(pb_level.level_type).unwrap(),
            table_infos: pb_level
                .table_infos
                .iter()
                .map(SstableInfo::from)
                .collect_vec(),
            total_file_size: pb_level.total_file_size,
            sub_level_id: pb_level.sub_level_id,
            uncompressed_file_size: pb_level.uncompressed_file_size,
            vnode_partition_count: pb_level.vnode_partition_count,
        }
    }
}

impl From<&Level> for PbLevel {
    fn from(level: &Level) -> Self {
        Self {
            level_idx: level.level_idx,
            level_type: level.level_type.into(),
            table_infos: level
                .table_infos
                .iter()
                .map(PbSstableInfo::from)
                .collect_vec(),
            total_file_size: level.total_file_size,
            sub_level_id: level.sub_level_id,
            uncompressed_file_size: level.uncompressed_file_size,
            vnode_partition_count: level.vnode_partition_count,
        }
    }
}

impl From<Level> for PbLevel {
    fn from(level: Level) -> Self {
        Self {
            level_idx: level.level_idx,
            level_type: level.level_type.into(),
            table_infos: level
                .table_infos
                .into_iter()
                .map(PbSstableInfo::from)
                .collect_vec(),
            total_file_size: level.total_file_size,
            sub_level_id: level.sub_level_id,
            uncompressed_file_size: level.uncompressed_file_size,
            vnode_partition_count: level.vnode_partition_count,
        }
    }
}

impl From<PbLevel> for Level {
    fn from(pb_level: PbLevel) -> Self {
        Self {
            level_idx: pb_level.level_idx,
            level_type: LevelType::try_from(pb_level.level_type).unwrap(),
            table_infos: pb_level
                .table_infos
                .into_iter()
                .map(SstableInfo::from)
                .collect_vec(),
            total_file_size: pb_level.total_file_size,
            sub_level_id: pb_level.sub_level_id,
            uncompressed_file_size: pb_level.uncompressed_file_size,
            vnode_partition_count: pb_level.vnode_partition_count,
        }
    }
}

impl ProtoSerializeSizeEstimatedExt for Level {
    fn estimated_encode_len(&self) -> usize {
        size_of::<u32>()
            + size_of::<u32>()
            + self
                .table_infos
                .iter()
                .map(|sst| sst.estimated_encode_len())
                .sum::<usize>()
            + size_of::<u64>()
            + size_of::<u64>()
            + size_of::<u64>()
            + size_of::<u32>()
    }
}

impl Level {
    pub fn get_table_infos(&self) -> &Vec<SstableInfo> {
        &self.table_infos
    }

    pub fn level_type(&self) -> LevelType {
        self.level_type
    }

    pub fn get_sub_level_id(&self) -> u64 {
        self.sub_level_id
    }

    pub fn get_level_idx(&self) -> u32 {
        self.level_idx
    }

    pub fn get_total_file_size(&self) -> u64 {
        self.total_file_size
    }

    pub fn get_uncompressed_file_size(&self) -> u64 {
        self.uncompressed_file_size
    }
}

#[derive(Debug, Clone, PartialEq, Default, Serialize)]
pub struct Levels {
    pub levels: Vec<Level>,
    pub l0: Option<OverlappingLevel>,
    pub group_id: u64,
    pub parent_group_id: u64,
    pub member_table_ids: Vec<u32>,
}

impl Levels {
    pub fn get_levels(&self) -> &Vec<Level> {
        &self.levels
    }
}

impl ProtoSerializeSizeEstimatedExt for Levels {
    fn estimated_encode_len(&self) -> usize {
        let mut basic = self
            .levels
            .iter()
            .map(|level| level.estimated_encode_len())
            .sum::<usize>()
            + size_of::<u64>()
            + size_of::<u64>()
            + size_of::<u32>();
        if let Some(l0) = self.l0.as_ref() {
            basic += l0.estimated_encode_len();
        }

        basic
    }
}

impl From<&PbLevels> for Levels {
    fn from(pb_levels: &PbLevels) -> Self {
        Self {
            l0: if pb_levels.l0.is_some() {
                Some(OverlappingLevel::from(pb_levels.l0.as_ref().unwrap()))
            } else {
                None
            },
            levels: pb_levels.levels.iter().map(Level::from).collect_vec(),
            group_id: pb_levels.group_id,
            parent_group_id: pb_levels.parent_group_id,
            member_table_ids: pb_levels.member_table_ids.clone(),
        }
    }
}

impl From<&Levels> for PbLevels {
    fn from(levels: &Levels) -> Self {
        Self {
            l0: if levels.l0.is_some() {
                Some(levels.l0.as_ref().unwrap().into())
            } else {
                None
            },
            levels: levels.levels.iter().map(PbLevel::from).collect_vec(),
            group_id: levels.group_id,
            parent_group_id: levels.parent_group_id,
            member_table_ids: levels.member_table_ids.clone(),
        }
    }
}

impl From<PbLevels> for Levels {
    fn from(pb_levels: PbLevels) -> Self {
        Self {
            l0: if pb_levels.l0.is_some() {
                Some(OverlappingLevel::from(pb_levels.l0.unwrap()))
            } else {
                None
            },
            levels: pb_levels.levels.into_iter().map(Level::from).collect_vec(),
            group_id: pb_levels.group_id,
            parent_group_id: pb_levels.parent_group_id,
            member_table_ids: pb_levels.member_table_ids.clone(),
        }
    }
}

impl From<Levels> for PbLevels {
    fn from(levels: Levels) -> Self {
        Self {
            l0: if levels.l0.is_some() {
                Some(levels.l0.unwrap().into())
            } else {
                None
            },
            levels: levels.levels.into_iter().map(PbLevel::from).collect_vec(),
            group_id: levels.group_id,
            parent_group_id: levels.parent_group_id,
            member_table_ids: levels.member_table_ids,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct HummockVersion {
    pub id: u64,
    pub levels: HashMap<CompactionGroupId, Levels>,
    pub max_committed_epoch: u64,
    pub safe_epoch: u64,
    pub table_watermarks: HashMap<TableId, TableWatermarks>,
}

impl Default for HummockVersion {
    fn default() -> Self {
        HummockVersion::from(&PbHummockVersion::default())
    }
}

impl HummockVersion {
    /// Convert the `PbHummockVersion` received from rpc to `HummockVersion`. No need to
    /// maintain backward compatibility.
    pub fn from_rpc_protobuf(pb_version: &PbHummockVersion) -> Self {
        HummockVersion::from(pb_version)
    }

    /// Convert the `PbHummockVersion` deserialized from persisted state to `HummockVersion`.
    /// We should maintain backward compatibility.
    pub fn from_persisted_protobuf(pb_version: &PbHummockVersion) -> Self {
        HummockVersion::from(pb_version)
    }
}

impl ProtoSerializeSizeEstimatedExt for HummockVersion {
    fn estimated_encode_len(&self) -> usize {
        self.levels.len() * size_of::<CompactionGroupId>()
            + self
                .levels
                .values()
                .map(|level| level.estimated_encode_len())
                .sum::<usize>()
            + self.table_watermarks.len() * size_of::<u32>()
            + self
                .table_watermarks
                .values()
                .map(|table_watermark| table_watermark.estimated_encode_len())
                .sum::<usize>()
    }
}

impl From<&PbHummockVersion> for HummockVersion {
    fn from(pb_version: &PbHummockVersion) -> Self {
        Self {
            id: pb_version.id,
            levels: pb_version
                .levels
                .iter()
                .map(|(group_id, levels)| (*group_id as CompactionGroupId, Levels::from(levels)))
                .collect(),
            max_committed_epoch: pb_version.max_committed_epoch,
            safe_epoch: pb_version.safe_epoch,
            table_watermarks: pb_version
                .table_watermarks
                .iter()
                .map(|(table_id, table_watermark)| {
                    (
                        TableId::new(*table_id),
                        TableWatermarks::from(table_watermark),
                    )
                })
                .collect(),
        }
    }
}

impl From<&HummockVersion> for PbHummockVersion {
    fn from(version: &HummockVersion) -> Self {
        Self {
            id: version.id,
            levels: version
                .levels
                .iter()
                .map(|(group_id, levels)| (*group_id as _, levels.into()))
                .collect(),
            max_committed_epoch: version.max_committed_epoch,
            safe_epoch: version.safe_epoch,
            table_watermarks: version
                .table_watermarks
                .iter()
                .map(|(table_id, watermark)| (table_id.table_id, watermark.into()))
                .collect(),
        }
    }
}

impl From<HummockVersion> for PbHummockVersion {
    fn from(version: HummockVersion) -> Self {
        Self {
            id: version.id,
            levels: version
                .levels
                .into_iter()
                .map(|(group_id, levels)| (group_id as _, levels.into()))
                .collect(),
            max_committed_epoch: version.max_committed_epoch,
            safe_epoch: version.safe_epoch,
            table_watermarks: version
                .table_watermarks
                .into_iter()
                .map(|(table_id, watermark)| (table_id.table_id, watermark.into()))
                .collect(),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct HummockVersionDelta {
    pub id: u64,
    pub prev_id: u64,
    pub group_deltas: HashMap<CompactionGroupId, PbGroupDeltas>,
    pub max_committed_epoch: u64,
    pub safe_epoch: u64,
    pub trivial_move: bool,
    pub gc_object_ids: Vec<HummockSstableObjectId>,
    pub new_table_watermarks: HashMap<TableId, TableWatermarks>,
    pub removed_table_ids: Vec<TableId>,
}

impl Default for HummockVersionDelta {
    fn default() -> Self {
        HummockVersionDelta::from(&PbHummockVersionDelta::default())
    }
}

impl HummockVersionDelta {
    /// Convert the `PbHummockVersionDelta` deserialized from persisted state to `HummockVersionDelta`.
    /// We should maintain backward compatibility.
    pub fn from_persisted_protobuf(delta: &PbHummockVersionDelta) -> Self {
        Self::from(delta)
    }

    /// Convert the `PbHummockVersionDelta` received from rpc to `HummockVersionDelta`. No need to
    /// maintain backward compatibility.
    pub fn from_rpc_protobuf(delta: &PbHummockVersionDelta) -> Self {
        Self::from(delta)
    }
}

impl From<&PbHummockVersionDelta> for HummockVersionDelta {
    fn from(pb_version_delta: &PbHummockVersionDelta) -> Self {
        Self {
            id: pb_version_delta.id,
            prev_id: pb_version_delta.prev_id,
            group_deltas: pb_version_delta.group_deltas.clone(),
            max_committed_epoch: pb_version_delta.max_committed_epoch,
            safe_epoch: pb_version_delta.safe_epoch,
            trivial_move: pb_version_delta.trivial_move,
            gc_object_ids: pb_version_delta.gc_object_ids.clone(),
            new_table_watermarks: pb_version_delta
                .new_table_watermarks
                .iter()
                .map(|(table_id, watermarks)| {
                    (TableId::new(*table_id), TableWatermarks::from(watermarks))
                })
                .collect(),
            removed_table_ids: pb_version_delta
                .removed_table_ids
                .iter()
                .map(|table_id| TableId::new(*table_id))
                .collect(),
        }
    }
}

impl From<&HummockVersionDelta> for PbHummockVersionDelta {
    fn from(version_delta: &HummockVersionDelta) -> Self {
        Self {
            id: version_delta.id,
            prev_id: version_delta.prev_id,
            group_deltas: version_delta.group_deltas.clone(),
            max_committed_epoch: version_delta.max_committed_epoch,
            safe_epoch: version_delta.safe_epoch,
            trivial_move: version_delta.trivial_move,
            gc_object_ids: version_delta.gc_object_ids.clone(),
            new_table_watermarks: version_delta
                .new_table_watermarks
                .iter()
                .map(|(table_id, watermarks)| (table_id.table_id, watermarks.into()))
                .collect(),
            removed_table_ids: version_delta
                .removed_table_ids
                .iter()
                .map(|table_id| table_id.table_id)
                .collect(),
        }
    }
}

impl From<HummockVersionDelta> for PbHummockVersionDelta {
    fn from(version_delta: HummockVersionDelta) -> Self {
        Self {
            id: version_delta.id,
            prev_id: version_delta.prev_id,
            group_deltas: version_delta.group_deltas,
            max_committed_epoch: version_delta.max_committed_epoch,
            safe_epoch: version_delta.safe_epoch,
            trivial_move: version_delta.trivial_move,
            gc_object_ids: version_delta.gc_object_ids,
            new_table_watermarks: version_delta
                .new_table_watermarks
                .into_iter()
                .map(|(table_id, watermarks)| (table_id.table_id, watermarks.into()))
                .collect(),
            removed_table_ids: version_delta
                .removed_table_ids
                .into_iter()
                .map(|table_id| table_id.table_id)
                .collect(),
        }
    }
}

impl From<PbHummockVersionDelta> for HummockVersionDelta {
    fn from(pb_version_delta: PbHummockVersionDelta) -> Self {
        Self {
            id: pb_version_delta.id,
            prev_id: pb_version_delta.prev_id,
            group_deltas: pb_version_delta.group_deltas,
            max_committed_epoch: pb_version_delta.max_committed_epoch,
            safe_epoch: pb_version_delta.safe_epoch,
            trivial_move: pb_version_delta.trivial_move,
            gc_object_ids: pb_version_delta.gc_object_ids,
            new_table_watermarks: pb_version_delta
                .new_table_watermarks
                .into_iter()
                .map(|(table_id, watermarks)| (TableId::new(table_id), watermarks.into()))
                .collect(),
            removed_table_ids: pb_version_delta
                .removed_table_ids
                .into_iter()
                .map(TableId::new)
                .collect(),
        }
    }
}

#[derive(Debug, PartialEq, Clone, Default, Serialize)]
pub struct SstableInfo {
    pub object_id: u64,
    pub sst_id: u64,
    pub key_range: Option<KeyRange>,
    pub file_size: u64,
    pub table_ids: Vec<u32>,
    pub meta_offset: u64,
    pub stale_key_count: u64,
    pub total_key_count: u64,
    pub min_epoch: u64,
    pub max_epoch: u64,
    pub uncompressed_file_size: u64,
    pub range_tombstone_count: u64,
    pub bloom_filter_kind: BloomFilterType,
}

impl ProtoSerializeSizeEstimatedExt for SstableInfo {
    fn estimated_encode_len(&self) -> usize {
        let mut basic = size_of::<u64>() // object_id
            + size_of::<u64>() // sstable_id
            + size_of::<u64>() // file_size
            + self.table_ids.len() * size_of::<u32>() // table_ids
            + size_of::<u64>() // meta_offset
            + size_of::<u64>() // stale_key_count
            + size_of::<u64>() // total_key_count
            + size_of::<u64>() // min_epoch
            + size_of::<u64>() // max_epoch
            + size_of::<u64>() // uncompressed_file_size
            + size_of::<u64>() // range_tombstone_count
            + size_of::<u32>(); // bloom_filter_kind

        if let Some(key_range) = &self.key_range {
            basic += key_range.left.len() + key_range.right.len() + size_of::<bool>();
        }

        basic
    }
}

impl From<PbSstableInfo> for SstableInfo {
    fn from(pb_sstable_info: PbSstableInfo) -> Self {
        Self {
            object_id: pb_sstable_info.object_id,
            sst_id: pb_sstable_info.sst_id,
            key_range: if pb_sstable_info.key_range.is_some() {
                let pb_keyrange = pb_sstable_info.key_range.unwrap();
                let key_range = KeyRange {
                    left: Bytes::from(pb_keyrange.left),
                    right: Bytes::from(pb_keyrange.right),
                    right_exclusive: pb_keyrange.right_exclusive,
                };
                Some(key_range)
            } else {
                None
            },

            file_size: pb_sstable_info.file_size,
            table_ids: pb_sstable_info.table_ids.clone(),
            meta_offset: pb_sstable_info.meta_offset,
            stale_key_count: pb_sstable_info.stale_key_count,
            total_key_count: pb_sstable_info.total_key_count,
            min_epoch: pb_sstable_info.min_epoch,
            max_epoch: pb_sstable_info.max_epoch,
            uncompressed_file_size: pb_sstable_info.uncompressed_file_size,
            range_tombstone_count: pb_sstable_info.range_tombstone_count,
            bloom_filter_kind: BloomFilterType::try_from(pb_sstable_info.bloom_filter_kind)
                .unwrap(),
        }
    }
}

impl From<&PbSstableInfo> for SstableInfo {
    fn from(pb_sstable_info: &PbSstableInfo) -> Self {
        Self {
            object_id: pb_sstable_info.object_id,
            sst_id: pb_sstable_info.sst_id,
            key_range: if pb_sstable_info.key_range.is_some() {
                let pb_keyrange = pb_sstable_info.key_range.as_ref().unwrap();
                let key_range = KeyRange {
                    left: Bytes::from(pb_keyrange.left.clone()),
                    right: Bytes::from(pb_keyrange.right.clone()),
                    right_exclusive: pb_keyrange.right_exclusive,
                };
                Some(key_range)
            } else {
                None
            },

            file_size: pb_sstable_info.file_size,
            table_ids: pb_sstable_info.table_ids.clone(),
            meta_offset: pb_sstable_info.meta_offset,
            stale_key_count: pb_sstable_info.stale_key_count,
            total_key_count: pb_sstable_info.total_key_count,
            min_epoch: pb_sstable_info.min_epoch,
            max_epoch: pb_sstable_info.max_epoch,
            uncompressed_file_size: pb_sstable_info.uncompressed_file_size,
            range_tombstone_count: pb_sstable_info.range_tombstone_count,
            bloom_filter_kind: BloomFilterType::try_from(pb_sstable_info.bloom_filter_kind)
                .unwrap(),
        }
    }
}

impl From<SstableInfo> for PbSstableInfo {
    fn from(sstable_info: SstableInfo) -> Self {
        PbSstableInfo {
            object_id: sstable_info.object_id,
            sst_id: sstable_info.sst_id,
            key_range: if sstable_info.key_range.is_some() {
                let keyrange = sstable_info.key_range.unwrap();
                let pb_key_range = PbKeyRange {
                    left: keyrange.left.into(),
                    right: keyrange.right.into(),
                    right_exclusive: keyrange.right_exclusive,
                };
                Some(pb_key_range)
            } else {
                None
            },

            file_size: sstable_info.file_size,
            table_ids: sstable_info.table_ids.clone(),
            meta_offset: sstable_info.meta_offset,
            stale_key_count: sstable_info.stale_key_count,
            total_key_count: sstable_info.total_key_count,
            min_epoch: sstable_info.min_epoch,
            max_epoch: sstable_info.max_epoch,
            uncompressed_file_size: sstable_info.uncompressed_file_size,
            range_tombstone_count: sstable_info.range_tombstone_count,
            bloom_filter_kind: sstable_info.bloom_filter_kind.into(),
        }
    }
}

impl From<&SstableInfo> for PbSstableInfo {
    fn from(sstable_info: &SstableInfo) -> Self {
        PbSstableInfo {
            object_id: sstable_info.object_id,
            sst_id: sstable_info.sst_id,
            key_range: if sstable_info.key_range.is_some() {
                let keyrange = sstable_info.key_range.as_ref().unwrap();
                let pb_key_range = PbKeyRange {
                    left: keyrange.left.to_vec(),
                    right: keyrange.right.to_vec(),
                    right_exclusive: keyrange.right_exclusive,
                };
                Some(pb_key_range)
            } else {
                None
            },

            file_size: sstable_info.file_size,
            table_ids: sstable_info.table_ids.clone(),
            meta_offset: sstable_info.meta_offset,
            stale_key_count: sstable_info.stale_key_count,
            total_key_count: sstable_info.total_key_count,
            min_epoch: sstable_info.min_epoch,
            max_epoch: sstable_info.max_epoch,
            uncompressed_file_size: sstable_info.uncompressed_file_size,
            range_tombstone_count: sstable_info.range_tombstone_count,
            bloom_filter_kind: sstable_info.bloom_filter_kind.into(),
        }
    }
}

impl SstableInfo {
    pub fn get_sst_id(&self) -> u64 {
        self.sst_id
    }

    pub fn get_object_id(&self) -> u64 {
        self.sst_id
    }

    pub fn get_file_size(&self) -> u64 {
        self.file_size
    }
}

#[derive(Clone, PartialEq, Default, Debug, Serialize)]
pub struct InputLevel {
    pub level_idx: u32,
    pub level_type: LevelType,
    pub table_infos: Vec<SstableInfo>,
}

impl ProtoSerializeSizeEstimatedExt for InputLevel {
    fn estimated_encode_len(&self) -> usize {
        size_of::<u32>()
            + size_of::<i32>()
            + self
                .table_infos
                .iter()
                .map(|sst| sst.estimated_encode_len())
                .sum::<usize>()
    }
}

impl From<PbInputLevel> for InputLevel {
    fn from(pb_input_level: PbInputLevel) -> Self {
        Self {
            level_idx: pb_input_level.level_idx,
            level_type: LevelType::try_from(pb_input_level.level_type).unwrap(),
            table_infos: pb_input_level
                .table_infos
                .into_iter()
                .map(SstableInfo::from)
                .collect_vec(),
        }
    }
}

impl From<&PbInputLevel> for InputLevel {
    fn from(pb_input_level: &PbInputLevel) -> Self {
        Self {
            level_idx: pb_input_level.level_idx,
            level_type: LevelType::try_from(pb_input_level.level_type).unwrap(),
            table_infos: pb_input_level
                .table_infos
                .iter()
                .map(SstableInfo::from)
                .collect_vec(),
        }
    }
}

impl From<InputLevel> for PbInputLevel {
    fn from(input_level: InputLevel) -> Self {
        Self {
            level_idx: input_level.level_idx,
            level_type: input_level.level_type.into(),
            table_infos: input_level
                .table_infos
                .into_iter()
                .map(|sst| sst.into())
                .collect_vec(),
        }
    }
}

impl From<&InputLevel> for PbInputLevel {
    fn from(input_level: &InputLevel) -> Self {
        Self {
            level_idx: input_level.level_idx,
            level_type: input_level.level_type.into(),
            table_infos: input_level
                .table_infos
                .iter()
                .map(|sst| sst.into())
                .collect_vec(),
        }
    }
}

impl InputLevel {
    pub fn get_table_infos(&self) -> &Vec<SstableInfo> {
        &self.table_infos
    }

    pub fn level_type(&self) -> PbLevelType {
        self.level_type
    }

    pub fn get_level_idx(&self) -> u32 {
        self.level_idx
    }
}

#[derive(Clone, PartialEq, Default)]
pub struct CompactTask {
    /// SSTs to be compacted, which will be removed from LSM after compaction
    pub input_ssts: Vec<InputLevel>,
    /// In ideal case, the compaction will generate splits.len() tables which have key range
    /// corresponding to that in \[splits\], respectively
    pub splits: Vec<KeyRange>,
    /// low watermark in 'ts-aware compaction'
    pub watermark: u64,
    /// compaction output, which will be added to \[target_level\] of LSM after compaction
    pub sorted_output_ssts: Vec<SstableInfo>,
    /// task id assigned by hummock storage service
    pub task_id: u64,
    /// compaction output will be added to \[target_level\] of LSM after compaction
    pub target_level: u32,
    pub gc_delete_keys: bool,
    /// Lbase in LSM
    pub base_level: u32,
    pub task_status: TaskStatus,
    /// compaction group the task belongs to
    pub compaction_group_id: u64,
    /// existing_table_ids for compaction drop key
    pub existing_table_ids: Vec<u32>,
    pub compression_algorithm: u32,
    pub target_file_size: u64,
    pub compaction_filter_mask: u32,
    pub table_options: BTreeMap<u32, TableOption>,
    pub current_epoch_time: u64,
    pub target_sub_level_id: u64,
    /// Identifies whether the task is space_reclaim, if the compact_task_type increases, it will be refactored to enum
    pub task_type: TaskType,
    /// Deprecated. use table_vnode_partition instead;
    pub split_by_state_table: bool,
    /// Compaction needs to cut the state table every time 1/weight of vnodes in the table have been processed.
    /// Deprecated. use table_vnode_partition instead;
    pub split_weight_by_vnode: u32,
    pub table_vnode_partition: BTreeMap<u32, u32>,
    /// The table watermark of any table id. In compaction we only use the table watermarks on safe epoch,
    /// so we only need to include the table watermarks on safe epoch to reduce the size of metadata.
    pub table_watermarks: BTreeMap<u32, TableWatermarks>,

    pub table_schemas: BTreeMap<u32, TableSchema>,
}

impl ProtoSerializeSizeEstimatedExt for CompactTask {
    fn estimated_encode_len(&self) -> usize {
        self.input_ssts
            .iter()
            .map(|input_level| input_level.estimated_encode_len())
            .sum::<usize>()
            + self
                .splits
                .iter()
                .map(|split| split.left.len() + split.right.len() + size_of::<bool>())
                .sum::<usize>()
            + size_of::<u64>()
            + self
                .sorted_output_ssts
                .iter()
                .map(|sst| sst.estimated_encode_len())
                .sum::<usize>()
            + size_of::<u64>()
            + size_of::<u32>()
            + size_of::<bool>()
            + size_of::<u32>()
            + size_of::<i32>()
            + size_of::<u64>()
            + self.existing_table_ids.len() * size_of::<u32>()
            + size_of::<u32>()
            + size_of::<u64>()
            + size_of::<u32>()
            + self.table_options.len() * size_of::<u64>()
            + size_of::<u64>()
            + size_of::<u64>()
            + size_of::<i32>()
            + size_of::<bool>()
            + size_of::<u32>()
            + self.table_vnode_partition.len() * size_of::<u64>()
            + self
                .table_watermarks
                .values()
                .map(|table_watermark| size_of::<u32>() + table_watermark.estimated_encode_len())
                .sum::<usize>()
    }
}

impl From<PbCompactTask> for CompactTask {
    fn from(pb_compact_task: PbCompactTask) -> Self {
        Self {
            input_ssts: pb_compact_task
                .input_ssts
                .into_iter()
                .map(InputLevel::from)
                .collect_vec(),
            splits: pb_compact_task
                .splits
                .into_iter()
                .map(|pb_keyrange| KeyRange {
                    left: Bytes::from(pb_keyrange.left),
                    right: Bytes::from(pb_keyrange.right),
                    right_exclusive: pb_keyrange.right_exclusive,
                })
                .collect_vec(),
            watermark: pb_compact_task.watermark,
            sorted_output_ssts: pb_compact_task
                .sorted_output_ssts
                .into_iter()
                .map(SstableInfo::from)
                .collect_vec(),
            task_id: pb_compact_task.task_id,
            target_level: pb_compact_task.target_level,
            gc_delete_keys: pb_compact_task.gc_delete_keys,
            base_level: pb_compact_task.base_level,
            task_status: TaskStatus::try_from(pb_compact_task.task_status).unwrap(),
            compaction_group_id: pb_compact_task.compaction_group_id,
            existing_table_ids: pb_compact_task.existing_table_ids.clone(),
            compression_algorithm: pb_compact_task.compression_algorithm,
            target_file_size: pb_compact_task.target_file_size,
            compaction_filter_mask: pb_compact_task.compaction_filter_mask,
            table_options: pb_compact_task.table_options.clone(),
            current_epoch_time: pb_compact_task.current_epoch_time,
            target_sub_level_id: pb_compact_task.target_sub_level_id,
            task_type: TaskType::try_from(pb_compact_task.task_type).unwrap(),
            #[allow(deprecated)]
            split_by_state_table: pb_compact_task.split_by_state_table,
            split_weight_by_vnode: pb_compact_task.split_weight_by_vnode,
            table_vnode_partition: pb_compact_task.table_vnode_partition.clone(),
            table_watermarks: pb_compact_task
                .table_watermarks
                .into_iter()
                .map(|(table_id, pb_table_watermark)| {
                    (table_id, TableWatermarks::from(pb_table_watermark))
                })
                .collect(),
            table_schemas: pb_compact_task.table_schemas,
        }
    }
}

impl From<&PbCompactTask> for CompactTask {
    fn from(pb_compact_task: &PbCompactTask) -> Self {
        Self {
            input_ssts: pb_compact_task
                .input_ssts
                .iter()
                .map(InputLevel::from)
                .collect_vec(),
            splits: pb_compact_task
                .splits
                .iter()
                .map(|pb_keyrange| KeyRange {
                    left: Bytes::from(pb_keyrange.left.clone()),
                    right: Bytes::from(pb_keyrange.right.clone()),
                    right_exclusive: pb_keyrange.right_exclusive,
                })
                .collect_vec(),
            watermark: pb_compact_task.watermark,
            sorted_output_ssts: pb_compact_task
                .sorted_output_ssts
                .iter()
                .map(SstableInfo::from)
                .collect_vec(),
            task_id: pb_compact_task.task_id,
            target_level: pb_compact_task.target_level,
            gc_delete_keys: pb_compact_task.gc_delete_keys,
            base_level: pb_compact_task.base_level,
            task_status: TaskStatus::try_from(pb_compact_task.task_status).unwrap(),
            compaction_group_id: pb_compact_task.compaction_group_id,
            existing_table_ids: pb_compact_task.existing_table_ids.clone(),
            compression_algorithm: pb_compact_task.compression_algorithm,
            target_file_size: pb_compact_task.target_file_size,
            compaction_filter_mask: pb_compact_task.compaction_filter_mask,
            table_options: pb_compact_task.table_options.clone(),
            current_epoch_time: pb_compact_task.current_epoch_time,
            target_sub_level_id: pb_compact_task.target_sub_level_id,
            task_type: TaskType::try_from(pb_compact_task.task_type).unwrap(),
            #[allow(deprecated)]
            split_by_state_table: pb_compact_task.split_by_state_table,
            split_weight_by_vnode: pb_compact_task.split_weight_by_vnode,
            table_vnode_partition: pb_compact_task.table_vnode_partition.clone(),
            table_watermarks: pb_compact_task
                .table_watermarks
                .iter()
                .map(|(table_id, pb_table_watermark)| {
                    (*table_id, TableWatermarks::from(pb_table_watermark))
                })
                .collect(),
            table_schemas: pb_compact_task.table_schemas.clone(),
        }
    }
}

impl From<CompactTask> for PbCompactTask {
    fn from(compact_task: CompactTask) -> Self {
        Self {
            input_ssts: compact_task
                .input_ssts
                .into_iter()
                .map(|input_level| input_level.into())
                .collect_vec(),
            splits: compact_task
                .splits
                .into_iter()
                .map(|keyrange| PbKeyRange {
                    left: keyrange.left.into(),
                    right: keyrange.right.into(),
                    right_exclusive: keyrange.right_exclusive,
                })
                .collect_vec(),
            watermark: compact_task.watermark,
            sorted_output_ssts: compact_task
                .sorted_output_ssts
                .into_iter()
                .map(|sst| sst.into())
                .collect_vec(),
            task_id: compact_task.task_id,
            target_level: compact_task.target_level,
            gc_delete_keys: compact_task.gc_delete_keys,
            base_level: compact_task.base_level,
            task_status: compact_task.task_status.into(),
            compaction_group_id: compact_task.compaction_group_id,
            existing_table_ids: compact_task.existing_table_ids.clone(),
            compression_algorithm: compact_task.compression_algorithm,
            target_file_size: compact_task.target_file_size,
            compaction_filter_mask: compact_task.compaction_filter_mask,
            table_options: compact_task.table_options.clone(),
            current_epoch_time: compact_task.current_epoch_time,
            target_sub_level_id: compact_task.target_sub_level_id,
            task_type: compact_task.task_type.into(),
            //#[allow(deprecated)] split_by_state_table: self.split_by_state_table,
            split_weight_by_vnode: compact_task.split_weight_by_vnode,
            table_vnode_partition: compact_task.table_vnode_partition.clone(),
            table_watermarks: compact_task
                .table_watermarks
                .into_iter()
                .map(|(table_id, table_watermark)| (table_id, table_watermark.into()))
                .collect(),
            ..Default::default()
        }
    }
}

impl From<&CompactTask> for PbCompactTask {
    fn from(compact_task: &CompactTask) -> Self {
        Self {
            input_ssts: compact_task
                .input_ssts
                .iter()
                .map(|input_level| input_level.into())
                .collect_vec(),
            splits: compact_task
                .splits
                .iter()
                .map(|keyrange| PbKeyRange {
                    left: keyrange.left.to_vec(),
                    right: keyrange.right.to_vec(),
                    right_exclusive: keyrange.right_exclusive,
                })
                .collect_vec(),
            watermark: compact_task.watermark,
            sorted_output_ssts: compact_task
                .sorted_output_ssts
                .iter()
                .map(|sst| sst.into())
                .collect_vec(),
            task_id: compact_task.task_id,
            target_level: compact_task.target_level,
            gc_delete_keys: compact_task.gc_delete_keys,
            base_level: compact_task.base_level,
            task_status: compact_task.task_status.into(),
            compaction_group_id: compact_task.compaction_group_id,
            existing_table_ids: compact_task.existing_table_ids.clone(),
            compression_algorithm: compact_task.compression_algorithm,
            target_file_size: compact_task.target_file_size,
            compaction_filter_mask: compact_task.compaction_filter_mask,
            table_options: compact_task.table_options.clone(),
            current_epoch_time: compact_task.current_epoch_time,
            target_sub_level_id: compact_task.target_sub_level_id,
            task_type: compact_task.task_type.into(),
            //#[allow(deprecated)] split_by_state_table: self.split_by_state_table,
            split_weight_by_vnode: compact_task.split_weight_by_vnode,
            table_vnode_partition: compact_task.table_vnode_partition.clone(),
            table_watermarks: compact_task
                .table_watermarks
                .iter()
                .map(|(table_id, table_watermark)| (*table_id, table_watermark.into()))
                .collect(),
            ..Default::default()
        }
    }
}

impl CompactTask {
    pub fn task_type(&self) -> PbTaskType {
        PbTaskType::try_from(self.task_type).unwrap()
    }

    pub fn task_status(&self) -> PbTaskStatus {
        PbTaskStatus::try_from(self.task_status).unwrap()
    }

    pub fn set_task_status(&mut self, s: PbTaskStatus) {
        self.task_status = s;
    }

    pub fn get_input_ssts(&self) -> &Vec<InputLevel> {
        &self.input_ssts
    }

    pub fn get_task_id(&self) -> u64 {
        self.task_id
    }
}

#[derive(Clone, PartialEq, Default, Serialize)]
pub struct ValidationTask {
    pub sst_infos: Vec<SstableInfo>,
    pub sst_id_to_worker_id: HashMap<u64, u32>,
    pub epoch: u64,
}

impl From<PbValidationTask> for ValidationTask {
    fn from(pb_validation_task: PbValidationTask) -> Self {
        Self {
            sst_infos: pb_validation_task
                .sst_infos
                .into_iter()
                .map(SstableInfo::from)
                .collect_vec(),
            sst_id_to_worker_id: pb_validation_task.sst_id_to_worker_id.clone(),
            epoch: pb_validation_task.epoch,
        }
    }
}

impl From<ValidationTask> for PbValidationTask {
    fn from(validation_task: ValidationTask) -> Self {
        Self {
            sst_infos: validation_task
                .sst_infos
                .into_iter()
                .map(|sst| sst.into())
                .collect_vec(),
            sst_id_to_worker_id: validation_task.sst_id_to_worker_id.clone(),
            epoch: validation_task.epoch,
        }
    }
}

impl ProtoSerializeSizeEstimatedExt for ValidationTask {
    fn estimated_encode_len(&self) -> usize {
        self.sst_infos
            .iter()
            .map(|sst| sst.estimated_encode_len())
            .sum::<usize>()
            + self.sst_id_to_worker_id.len() * (size_of::<u64>() + size_of::<u32>())
            + size_of::<u64>()
    }
}

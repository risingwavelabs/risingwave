// Copyright 2026 RisingWave Labs
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

use std::collections::{BTreeMap, HashSet};

use bytes::Bytes;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::key_range::{KeyRange, KeyRangeCommon};
use risingwave_hummock_sdk::level::{Level, OverlappingLevel};
use risingwave_hummock_sdk::vnode_partition::{
    build_vnode_partition_boundary_keys, vnode_boundary_full_key,
};
use risingwave_pb::hummock::LevelType;

use crate::hummock::level_handler::LevelHandler;

#[derive(Debug)]
pub(crate) struct VnodePartition {
    pub(crate) index: usize,
    /// Number of non-empty runnable non-overlapping sub-levels in this partition.
    pub(crate) depth: u64,
    /// Maximum number of runnable SST key ranges covering the same key.
    pub(crate) max_overlap_depth: u64,
    pub(crate) total_sst_ref_count: u64,
    pub(crate) total_object_count: u64,
    pub(crate) runnable_sst_ref_count: u64,
    pub(crate) runnable_object_count: u64,
    pub(crate) runnable_file_size: u64,
    pub(crate) total_referenced_object_size: u64,
    pub(crate) runnable_referenced_object_size: u64,
    pub(crate) l0: OverlappingLevel,
}

/// A transient, fixed vnode-partition view of runnable L0 stack depth.
///
/// Partition boundaries come from table metadata and compaction-group config, while depth is
/// rebuilt from the current version and pending-file state for every selection. Empty vnode ranges
/// therefore remain stable partitions with zero pressure.
#[derive(Debug)]
pub(crate) struct L0VnodePartitionView {
    partitions: Vec<VnodePartition>,
}

impl L0VnodePartitionView {
    pub(crate) fn build(
        table_id: TableId,
        vnode_count: usize,
        partition_count: usize,
        l0: &OverlappingLevel,
        l0_handler: &LevelHandler,
    ) -> Option<Self> {
        if partition_count <= 1 || partition_count > vnode_count {
            return None;
        }

        let partition_ranges = build_partition_ranges(table_id, vnode_count, partition_count);
        let mut partition_levels = (0..partition_count)
            .map(|_| {
                l0.sub_levels
                    .iter()
                    .map(|level| Level {
                        level_idx: level.level_idx,
                        level_type: level.level_type,
                        table_infos: vec![],
                        total_file_size: 0,
                        sub_level_id: level.sub_level_id,
                        uncompressed_file_size: 0,
                        vnode_partition_count: 0,
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        for (level_index, level) in l0.sub_levels.iter().enumerate() {
            for sst in &level.table_infos {
                if sst.table_ids.as_slice() != [table_id]
                    || sst.key_range.left.is_empty()
                    || sst.key_range.right.is_empty()
                {
                    return None;
                }
                let mut matching_partitions = partition_ranges
                    .iter()
                    .enumerate()
                    .filter(|(_, range)| range.sstable_overlap(&sst.key_range));
                let (partition_index, _) = matching_partitions.next()?;

                // A filtered L0 view is safe only when every SST belongs to exactly one fixed
                // partition. Old or unaligned SSTs fall back to the unpartitioned selector until
                // compaction replaces them.
                if matching_partitions.next().is_some() {
                    return None;
                }

                let partition_level = &mut partition_levels[partition_index][level_index];
                partition_level.table_infos.push(sst.clone());
                partition_level.total_file_size += sst.sst_size;
                partition_level.uncompressed_file_size += sst.uncompressed_file_size;
            }
        }

        let partitions = partition_levels
            .into_iter()
            .enumerate()
            .map(|(index, sub_levels)| {
                // Empty sub-levels only contain SSTs from other disjoint vnode partitions. Drop
                // them so the existing picker sees the same ordered L0 stack for this partition
                // without being blocked by unrelated levels.
                let sub_levels = sub_levels
                    .into_iter()
                    .filter(|level| !level.table_infos.is_empty())
                    .collect::<Vec<_>>();
                let depth = sub_levels
                    .iter()
                    .filter(|level| {
                        level.level_type == LevelType::Nonoverlapping
                            && level
                                .table_infos
                                .iter()
                                .any(|sst| !l0_handler.is_pending_compact(&sst.sst_id))
                    })
                    .count() as u64;
                let max_overlap_depth = max_runnable_overlap_depth(&sub_levels, l0_handler);
                let total_sst_ref_count = sub_levels
                    .iter()
                    .map(|level| level.table_infos.len() as u64)
                    .sum();
                let total_object_count = sub_levels
                    .iter()
                    .flat_map(|level| level.table_infos.iter().map(|sst| sst.object_id))
                    .collect::<HashSet<_>>()
                    .len() as u64;
                let runnable_ssts = sub_levels
                    .iter()
                    .filter(|level| level.level_type == LevelType::Nonoverlapping)
                    .flat_map(|level| level.table_infos.iter())
                    .filter(|sst| !l0_handler.is_pending_compact(&sst.sst_id))
                    .collect::<Vec<_>>();
                let runnable_sst_ref_count = runnable_ssts.len() as u64;
                let runnable_object_count = runnable_ssts
                    .iter()
                    .map(|sst| sst.object_id)
                    .collect::<HashSet<_>>()
                    .len() as u64;
                let runnable_file_size = runnable_ssts.iter().map(|sst| sst.sst_size).sum();
                let runnable_referenced_object_size =
                    runnable_ssts.iter().map(|sst| sst.file_size).sum();
                let total_file_size = sub_levels.iter().map(|level| level.total_file_size).sum();
                let total_referenced_object_size = sub_levels
                    .iter()
                    .flat_map(|level| level.table_infos.iter())
                    .map(|sst| sst.file_size)
                    .sum();
                let uncompressed_file_size = sub_levels
                    .iter()
                    .map(|level| level.uncompressed_file_size)
                    .sum();

                VnodePartition {
                    index,
                    depth,
                    max_overlap_depth,
                    total_sst_ref_count,
                    total_object_count,
                    runnable_sst_ref_count,
                    runnable_object_count,
                    runnable_file_size,
                    total_referenced_object_size,
                    runnable_referenced_object_size,
                    l0: OverlappingLevel {
                        sub_levels,
                        total_file_size,
                        uncompressed_file_size,
                    },
                }
            })
            .collect();

        Some(Self { partitions })
    }

    pub(crate) fn into_partitions(
        self,
        min_level_count: u64,
        score_base: u64,
    ) -> impl Iterator<Item = (u64, VnodePartition)> {
        let threshold = std::cmp::max(1, min_level_count);
        self.partitions
            .into_iter()
            .map(move |partition| (partition.depth * score_base / threshold, partition))
    }

    #[cfg(test)]
    fn depths(&self) -> Vec<u64> {
        self.partitions
            .iter()
            .map(|partition| partition.depth)
            .collect()
    }

    #[cfg(test)]
    fn max_overlap_depths(&self) -> Vec<u64> {
        self.partitions
            .iter()
            .map(|partition| partition.max_overlap_depth)
            .collect()
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum RangeEventKind {
    ExclusiveEnd,
    Start,
    InclusiveEnd,
}

#[derive(Debug)]
struct RangeEvent {
    key: Bytes,
    kind: RangeEventKind,
    level_index: usize,
}

/// Point-overlap depth is observability only, not an admission or seed-discovery rule.
fn max_runnable_overlap_depth(sub_levels: &[Level], l0_handler: &LevelHandler) -> u64 {
    let mut events = vec![];
    for (level_index, level) in sub_levels.iter().enumerate() {
        if level.level_type != LevelType::Nonoverlapping {
            continue;
        }
        for sst in &level.table_infos {
            if l0_handler.is_pending_compact(&sst.sst_id) {
                continue;
            }
            events.push(RangeEvent {
                key: sst.key_range.left.clone(),
                kind: RangeEventKind::Start,
                level_index,
            });
            // An empty right boundary means positive infinity.
            if !sst.key_range.right.is_empty() {
                events.push(RangeEvent {
                    key: sst.key_range.right.clone(),
                    kind: if sst.key_range.right_exclusive {
                        RangeEventKind::ExclusiveEnd
                    } else {
                        RangeEventKind::InclusiveEnd
                    },
                    level_index,
                });
            }
        }
    }
    events.sort_unstable_by(|a, b| {
        compare_boundary_keys(&a.key, &b.key).then_with(|| a.kind.cmp(&b.kind))
    });

    // Count active sub-levels, even if adjacent SST full-key boundaries share a user key.
    let mut active = BTreeMap::<usize, usize>::new();
    let mut max_depth = 0;
    for event in events {
        match event.kind {
            RangeEventKind::Start => {
                *active.entry(event.level_index).or_default() += 1;
                max_depth = max_depth.max(active.len() as u64);
            }
            RangeEventKind::ExclusiveEnd | RangeEventKind::InclusiveEnd => {
                let count = active.get_mut(&event.level_index).unwrap();
                *count -= 1;
                if *count == 0 {
                    active.remove(&event.level_index);
                }
            }
        }
    }
    max_depth
}

fn compare_boundary_keys(left: &[u8], right: &[u8]) -> std::cmp::Ordering {
    match (left.is_empty(), right.is_empty()) {
        (true, true) => std::cmp::Ordering::Equal,
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        (false, false) => FullKey::decode(left)
            .user_key
            .cmp(&FullKey::decode(right).user_key),
    }
}

fn build_partition_ranges(
    table_id: TableId,
    vnode_count: usize,
    partition_count: usize,
) -> Vec<KeyRange> {
    let mut starts = Vec::with_capacity(partition_count);
    starts.push(vnode_boundary_full_key(table_id, 0));
    starts.extend(build_vnode_partition_boundary_keys(
        table_id,
        vnode_count,
        partition_count,
    ));

    starts
        .iter()
        .enumerate()
        .map(|(idx, left)| KeyRange {
            left: left.clone(),
            right: starts.get(idx + 1).cloned().unwrap_or_default(),
            right_exclusive: true,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use risingwave_hummock_sdk::level::Level;
    use risingwave_hummock_sdk::sstable_info::{SstableInfo, SstableInfoInner};

    use super::*;

    fn sst(id: u64, left_vnode: usize, right_vnode: usize) -> SstableInfo {
        sst_with_object(id, id, left_vnode, right_vnode)
    }

    fn sst_with_object(
        id: u64,
        object_id: u64,
        left_vnode: usize,
        right_vnode: usize,
    ) -> SstableInfo {
        let table_id = TableId::new(1);
        SstableInfoInner {
            object_id: object_id.into(),
            sst_id: id.into(),
            key_range: KeyRange {
                left: vnode_boundary_full_key(table_id, left_vnode),
                right: vnode_boundary_full_key(table_id, right_vnode),
                right_exclusive: true,
            },
            table_ids: vec![table_id],
            file_size: 10,
            sst_size: 1,
            ..Default::default()
        }
        .into()
    }

    fn l0(levels: Vec<Vec<SstableInfo>>) -> OverlappingLevel {
        OverlappingLevel {
            sub_levels: levels
                .into_iter()
                .enumerate()
                .map(|(idx, table_infos)| Level {
                    level_idx: 0,
                    level_type: LevelType::Nonoverlapping,
                    table_infos,
                    sub_level_id: idx as u64,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn sparse_writes_do_not_change_partition_layout() {
        let l0 = l0((1..=4).map(|id| vec![sst(id, 0, 16)]).collect());
        let view = L0VnodePartitionView::build(TableId::new(1), 256, 8, &l0, &LevelHandler::new(0))
            .unwrap();

        assert_eq!(view.depths(), vec![4, 0, 0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn depth_counts_non_empty_sub_levels_in_the_partition() {
        let l0 = l0(vec![vec![sst(1, 0, 4)], vec![sst(2, 8, 12)]]);
        let view = L0VnodePartitionView::build(TableId::new(1), 256, 8, &l0, &LevelHandler::new(0))
            .unwrap();

        assert_eq!(view.depths()[0], 2);
        assert_eq!(view.max_overlap_depths()[0], 1);
    }

    #[test]
    fn max_overlap_depth_counts_actual_key_range_stack() {
        let l0 = l0(vec![vec![sst(1, 0, 12)], vec![sst(2, 8, 16)]]);
        let view = L0VnodePartitionView::build(TableId::new(1), 256, 8, &l0, &LevelHandler::new(0))
            .unwrap();

        assert_eq!(view.depths()[0], 2);
        assert_eq!(view.max_overlap_depths()[0], 2);
    }

    #[test]
    fn exclusive_adjacent_ranges_do_not_overlap() {
        let l0 = l0(vec![vec![sst(1, 0, 8)], vec![sst(2, 8, 16)]]);
        let view = L0VnodePartitionView::build(TableId::new(1), 256, 8, &l0, &LevelHandler::new(0))
            .unwrap();

        assert_eq!(view.depths()[0], 2);
        assert_eq!(view.max_overlap_depths()[0], 1);
    }

    #[test]
    fn logical_references_are_separate_from_objects() {
        let l0 = l0(vec![
            vec![sst_with_object(1, 100, 0, 8)],
            vec![sst_with_object(2, 100, 8, 16)],
        ]);
        let view = L0VnodePartitionView::build(TableId::new(1), 256, 8, &l0, &LevelHandler::new(0))
            .unwrap();

        let partition = &view.partitions[0];
        assert_eq!(partition.total_sst_ref_count, 2);
        assert_eq!(partition.total_object_count, 1);
        assert_eq!(partition.runnable_sst_ref_count, 2);
        assert_eq!(partition.runnable_object_count, 1);
        assert_eq!(partition.total_referenced_object_size, 20);
        assert_eq!(partition.runnable_referenced_object_size, 20);
    }

    #[test]
    fn pending_ssts_are_removed_from_runnable_depth() {
        let l0 = l0((1..=4).map(|id| vec![sst(id, 0, 16)]).collect());
        let mut handler = LevelHandler::new(0);
        handler.add_pending_task(10, 1, &l0.sub_levels[0].table_infos);
        let view = L0VnodePartitionView::build(TableId::new(1), 256, 8, &l0, &handler).unwrap();

        assert_eq!(view.depths()[0], 3);
        assert_eq!(view.max_overlap_depths()[0], 3);
        assert_eq!(view.partitions[0].total_sst_ref_count, 4);
        assert_eq!(view.partitions[0].runnable_sst_ref_count, 3);
    }

    #[test]
    fn unrelated_sub_levels_are_removed_from_the_partition_view() {
        let mut l0 = l0(vec![
            vec![sst(1, 0, 16)],
            vec![sst(2, 32, 48)],
            vec![sst(3, 0, 16)],
        ]);
        l0.sub_levels[1].level_type = LevelType::Overlapping;
        let view = L0VnodePartitionView::build(TableId::new(1), 256, 8, &l0, &LevelHandler::new(0))
            .unwrap();

        assert_eq!(view.partitions[0].l0.sub_levels.len(), 2);
        assert!(
            view.partitions[0]
                .l0
                .sub_levels
                .iter()
                .all(|level| level.level_type == LevelType::Nonoverlapping)
        );
        assert_eq!(view.partitions[1].l0.sub_levels.len(), 1);
        assert_eq!(
            view.partitions[1].l0.sub_levels[0].level_type,
            LevelType::Overlapping
        );
    }

    #[test]
    fn unaligned_sst_disables_the_partition_view() {
        let l0 = l0(vec![vec![sst(1, 0, 64)]]);
        assert!(
            L0VnodePartitionView::build(TableId::new(1), 256, 8, &l0, &LevelHandler::new(0),)
                .is_none()
        );
    }
}

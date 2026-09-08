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

use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::key_range::{KeyRange, KeyRangeCommon};
use risingwave_hummock_sdk::level::{Level, OverlappingLevel};
use risingwave_hummock_sdk::vnode_partition::{
    build_vnode_partition_boundary_keys, vnode_boundary_full_key,
};
use risingwave_pb::hummock::LevelType;

use crate::hummock::level_handler::LevelHandler;

#[derive(Debug)]
struct VnodePartition {
    depth: u64,
    l0: OverlappingLevel,
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
            .map(|sub_levels| {
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
                let total_file_size = sub_levels.iter().map(|level| level.total_file_size).sum();
                let uncompressed_file_size = sub_levels
                    .iter()
                    .map(|level| level.uncompressed_file_size)
                    .sum();

                VnodePartition {
                    depth,
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
    ) -> impl Iterator<Item = (u64, OverlappingLevel)> {
        let threshold = std::cmp::max(1, min_level_count);
        self.partitions
            .into_iter()
            .map(move |partition| (partition.depth * score_base / threshold, partition.l0))
    }

    #[cfg(test)]
    fn depths(&self) -> Vec<u64> {
        self.partitions
            .iter()
            .map(|partition| partition.depth)
            .collect()
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
        let table_id = TableId::new(1);
        SstableInfoInner {
            object_id: id.into(),
            sst_id: id.into(),
            key_range: KeyRange {
                left: vnode_boundary_full_key(table_id, left_vnode),
                right: vnode_boundary_full_key(table_id, right_vnode),
                right_exclusive: true,
            },
            table_ids: vec![table_id],
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
    }

    #[test]
    fn pending_ssts_are_removed_from_runnable_depth() {
        let l0 = l0((1..=4).map(|id| vec![sst(id, 0, 16)]).collect());
        let mut handler = LevelHandler::new(0);
        handler.add_pending_task(10, 1, &l0.sub_levels[0].table_infos);
        let view = L0VnodePartitionView::build(TableId::new(1), 256, 8, &l0, &handler).unwrap();

        assert_eq!(view.depths()[0], 3);
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

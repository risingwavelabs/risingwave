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

pub mod hummock_version_ext;

use parse_display::Display;

use crate::CompactionGroupId;

pub type StateTableId = u32;

/// A compaction task's `StaticCompactionGroupId` indicates the compaction group that all its input
/// SSTs belong to.
#[derive(Display)]
pub enum StaticCompactionGroupId {
    /// Create a new compaction group.
    NewCompactionGroup = 0,
    /// All shared buffer local compaction task goes to here. Meta service will never see this
    /// value. Note that currently we've restricted the compaction task's input by `via
    /// compact_shared_buffer_by_compaction_group`
    SharedBuffer = 1,
    /// All states goes to here by default.
    StateDefault = 2,
    /// All MVs goes to here.
    MaterializedView = 3,
    /// Larger than any `StaticCompactionGroupId`.
    End = 4,
}

impl From<StaticCompactionGroupId> for CompactionGroupId {
    fn from(cg: StaticCompactionGroupId) -> Self {
        cg as CompactionGroupId
    }
}

pub mod group_split {

    use std::cmp::Ordering;

    use bytes::Bytes;
    use risingwave_pb::hummock::PbLevelType;

    use super::hummock_version_ext::insert_new_sub_level;
    use crate::can_concat;
    use crate::key::FullKey;
    use crate::key_range::KeyRange;
    use crate::level::{Level, Levels};
    use crate::sstable_info::SstableInfo;

    #[derive(Debug, PartialEq, Clone)]
    pub enum SstSplitType {
        Left,
        Right,
        Both,
    }

    pub fn need_to_split(sst: &SstableInfo, split_key: Bytes) -> SstSplitType {
        let key_range = &sst.key_range;
        // 1. compare left
        if split_key.cmp(&key_range.left).is_le() {
            return SstSplitType::Right;
        }

        // 2. compare right
        if key_range.right_exclusive {
            if split_key.cmp(&key_range.right).is_ge() {
                return SstSplitType::Left;
            }
        } else if split_key.cmp(&key_range.right).is_gt() {
            return SstSplitType::Left;
        }

        SstSplitType::Both
    }

    pub fn split_sst(
        origin_sst_info: &mut SstableInfo,
        new_sst_id: &mut u64,
        split_key: Bytes,
        left_size: u64,
        right_size: u64,
    ) -> SstableInfo {
        let mut branch_table_info = origin_sst_info.clone();
        branch_table_info.sst_id = *new_sst_id;
        origin_sst_info.sst_id = *new_sst_id + 1;
        *new_sst_id += 1;

        let (key_range_l, key_range_r) = {
            let key_range = &origin_sst_info.key_range;
            let l = KeyRange {
                left: key_range.left.clone(),
                right: split_key.clone(),
                right_exclusive: true,
            };

            let r = KeyRange {
                left: split_key.clone(),
                right: key_range.right.clone(),
                right_exclusive: key_range.right_exclusive,
            };

            (l, r)
        };
        let (table_ids_l, table_ids_r) =
            split_table_ids(&origin_sst_info.table_ids, split_key.clone());

        // rebuild the key_range and size and sstable file size
        {
            // origin_sst_info
            origin_sst_info.key_range = key_range_l.clone();
            origin_sst_info.sst_size = left_size;
            origin_sst_info.table_ids = table_ids_l;
        }

        {
            // new sst
            branch_table_info.key_range = key_range_r.clone();
            branch_table_info.sst_size = right_size;
            branch_table_info.table_ids = table_ids_r;
        }

        branch_table_info
    }

    pub fn split_table_ids(table_ids: &Vec<u32>, split_key: Bytes) -> (Vec<u32>, Vec<u32>) {
        assert!(table_ids.is_sorted());
        let split_full_key = FullKey::decode(&split_key);
        let split_user_key = split_full_key.user_key;
        let vnode = split_user_key.get_vnode_id();
        let table_id = split_user_key.table_id.table_id();

        // let pos = table_ids.binary_search(&table_id).unwrap();
        let pos = table_ids.partition_point(|&id| id < table_id);
        if vnode == 0 {
            // The split logic binds the vnode and assumes that the incoming split key epoch == 0, so we place the table_id to the right
            // NOTE: This branch can avoid split same table_id into two groups when vnode == 0, it related to the implementation of `state_table_info` with compaction group
            (table_ids[..pos].to_vec(), table_ids[pos..].to_vec())
        } else {
            (table_ids[..=pos].to_vec(), table_ids[pos..].to_vec())
        }

        // split table_id that related to split_key both left and right
        // let pos = table_ids.binary_search(&table_id).unwrap();
        // (table_ids[..=pos].to_vec(), table_ids[pos..].to_vec())
    }

    pub fn split_sst_info_for_level(
        level: &mut Level,
        new_sst_id: &mut u64,
        split_key: Bytes,
    ) -> Vec<SstableInfo> {
        if level.table_infos.is_empty() {
            return vec![];
        }
        if level.level_type == PbLevelType::Overlapping {
            let mut left_sst = vec![];
            let mut right_sst = vec![];

            for sst in &mut level.table_infos {
                let sst_split_type = need_to_split(sst, split_key.clone());
                println!(
                    "sub_level {} sst {} object_id {} sst_split_type: {:?}",
                    level.sub_level_id, sst.sst_id, sst.object_id, sst_split_type
                );
                match sst_split_type {
                    SstSplitType::Left => {
                        left_sst.push(sst.clone());
                    }
                    SstSplitType::Right => {
                        right_sst.push(sst.clone());
                    }
                    SstSplitType::Both => {
                        let estimated_size = sst.sst_size;
                        let branch_sst = split_sst(
                            sst,
                            new_sst_id,
                            split_key.clone(),
                            estimated_size / 2,
                            estimated_size / 2,
                        );
                        right_sst.push(branch_sst.clone());
                        left_sst.push(sst.clone());
                    }
                }
            }

            level.table_infos = left_sst;
            right_sst
        } else {
            let pos = get_split_pos(&level.table_infos, split_key.clone());
            if pos >= level.table_infos.len() {
                return vec![];
            }

            let mut insert_table_infos = vec![];
            let sst = &mut level.table_infos[pos];
            let sst_split_type = need_to_split(sst, split_key.clone());

            match sst_split_type {
                SstSplitType::Left => {
                    insert_table_infos.extend_from_slice(&level.table_infos[pos + 1..]);
                    level.table_infos = level.table_infos[0..=pos].to_vec();
                }
                SstSplitType::Right => {
                    insert_table_infos.extend_from_slice(&level.table_infos[pos..]); // the sst at pos has been split to the right
                    level.table_infos = level.table_infos[0..pos].to_vec();
                }
                SstSplitType::Both => {
                    // split the sst
                    let estimated_size = sst.sst_size;
                    let branch_sst = split_sst(
                        sst,
                        new_sst_id,
                        split_key,
                        estimated_size / 2,
                        estimated_size / 2,
                    );
                    insert_table_infos.push(branch_sst.clone());
                    // the sst at pos has been split to both left and right
                    // the branched sst has been inserted to the `insert_table_infos`
                    insert_table_infos.extend_from_slice(&level.table_infos[pos + 1..]);
                    level.table_infos = level.table_infos[0..=pos].to_vec();
                }
            };

            insert_table_infos
        }
    }

    pub fn get_split_pos(sstables: &Vec<SstableInfo>, split_key: Bytes) -> usize {
        sstables
            .partition_point(|sst| sst.key_range.left.cmp(&split_key).is_lt())
            .saturating_sub(1)
    }

    pub fn merge_levels(left_levels: &mut Levels, right_levels: Levels) {
        let right_l0 = right_levels.l0;

        let mut max_left_sub_level_id = left_levels
            .l0
            .sub_levels
            .iter()
            .map(|sub_level| sub_level.sub_level_id + 1)
            .max()
            .unwrap_or(0); // If there are no sub levels, the max sub level id is 0.
        let need_rewrite_right_sub_level_id = max_left_sub_level_id != 0;

        for mut right_sub_level in right_l0.sub_levels {
            // Rewrtie the sub level id of right sub level to avoid conflict with left sub levels. (conflict level type)
            // e.g. left sub levels: [0, 1, 2], right sub levels: [0, 1, 2], after rewrite, right sub levels: [3, 4, 5]
            if need_rewrite_right_sub_level_id {
                right_sub_level.sub_level_id = max_left_sub_level_id;
                max_left_sub_level_id += 1;
            }

            insert_new_sub_level(
                &mut left_levels.l0,
                right_sub_level.sub_level_id,
                right_sub_level.level_type,
                right_sub_level.table_infos,
                None,
            );
        }

        assert!(
            left_levels
                .l0
                .sub_levels
                .is_sorted_by_key(|sub_level| sub_level.sub_level_id),
            "{}",
            format!("left_levels.l0.sub_levels: {:?}", left_levels.l0.sub_levels)
        );

        // Reinitialise `vnode_partition_count`` to avoid misaligned hierarchies
        // caused by the merge of different compaction groups.(picker might reject the different `vnode_partition_count` sub_level to compact)
        left_levels
            .l0
            .sub_levels
            .iter_mut()
            .for_each(|sub_level| sub_level.vnode_partition_count = 0);

        for (idx, level) in right_levels.levels.into_iter().enumerate() {
            if level.table_infos.is_empty() {
                continue;
            }

            let insert_table_infos = level.table_infos.clone();
            left_levels.levels[idx].total_file_size += insert_table_infos
                .iter()
                .map(|sst| sst.sst_size)
                .sum::<u64>();
            left_levels.levels[idx].uncompressed_file_size += insert_table_infos
                .iter()
                .map(|sst| sst.uncompressed_file_size)
                .sum::<u64>();

            left_levels.levels[idx]
                .table_infos
                .extend(insert_table_infos);
            left_levels.levels[idx]
                .table_infos
                .sort_by(|sst1, sst2| sst1.key_range.cmp(&sst2.key_range));
            assert!(
                can_concat(&left_levels.levels[idx].table_infos),
                "{}",
                format!(
                    "left_levels.levels[{}].table_infos: {:?} level_idx {:?}",
                    idx, left_levels.levels[idx].table_infos, left_levels.levels[idx].level_idx
                )
            );
        }
    }

    // When `insert_hint` is `Ok(idx)`, it means that the sub level `idx` in `target_l0`
    // will extend these SSTs. When `insert_hint` is `Err(idx)`, it
    // means that we will add a new sub level `idx` into `target_l0`.
    pub fn get_sub_level_insert_hint(
        target_levels: &Vec<Level>,
        sub_level: &Level,
    ) -> Result<usize, usize> {
        for (idx, other) in target_levels.iter().enumerate() {
            match other.sub_level_id.cmp(&sub_level.sub_level_id) {
                Ordering::Less => {}
                Ordering::Equal => {
                    return Ok(idx);
                }
                Ordering::Greater => {
                    return Err(idx);
                }
            }
        }

        Err(target_levels.len())
    }
}

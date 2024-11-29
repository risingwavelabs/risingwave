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

/// The split will follow the following rules:
/// 1. Ssts with `split_key` will be split into two separate ssts and their `key_range` will be changed `sst_1`: [`sst.key_range.right`, `split_key`) `sst_2`: [`split_key`, `sst.key_range.right`].
/// 2. Currently only `vnode` 0 and `vnode` max is supported.
/// 3. Due to the above rule, `vnode` max will be rewritten as `table_id` + 1, vnode 0
pub mod group_split {
    use std::cmp::Ordering;
    use std::collections::BTreeSet;

    use bytes::Bytes;
    use risingwave_common::catalog::TableId;
    use risingwave_common::hash::VirtualNode;
    use risingwave_pb::hummock::PbLevelType;

    use super::hummock_version_ext::insert_new_sub_level;
    use super::StateTableId;
    use crate::key::{FullKey, TableKey};
    use crate::key_range::KeyRange;
    use crate::level::{Level, Levels};
    use crate::sstable_info::SstableInfo;
    use crate::{can_concat, HummockEpoch, KeyComparator};

    // By default, the split key is constructed with vnode = 0 and epoch = MAX, so that we can split table_id to the right group
    pub fn build_split_key(table_id: StateTableId, vnode: VirtualNode) -> Bytes {
        build_split_full_key(table_id, vnode).encode().into()
    }

    /// generate a full key for convenience to get the `table_id` and `vnode`
    pub fn build_split_full_key(
        mut table_id: StateTableId,
        mut vnode: VirtualNode,
    ) -> FullKey<Vec<u8>> {
        if VirtualNode::MAX_REPRESENTABLE == vnode {
            // Modify `table_id` to `next_table_id` to satisfy the `split_to_right`` rule, so that the `table_id`` originally passed in will be split to left.
            table_id = table_id.strict_add(1);
            vnode = VirtualNode::ZERO;
        }

        FullKey::new(
            TableId::from(table_id),
            TableKey(vnode.to_be_bytes().to_vec()),
            HummockEpoch::MAX,
        )
    }

    #[derive(Debug, PartialEq, Clone)]
    pub enum SstSplitType {
        Left,
        Right,
        Both,
    }

    /// Determine whether the SST needs to be split, and if so, which side to split.
    pub fn need_to_split(sst: &SstableInfo, split_key: Bytes) -> SstSplitType {
        let key_range = &sst.key_range;
        // 1. compare left
        if KeyComparator::compare_encoded_full_key(&split_key, &key_range.left).is_le() {
            return SstSplitType::Right;
        }

        // 2. compare right
        if key_range.right_exclusive {
            if KeyComparator::compare_encoded_full_key(&split_key, &key_range.right).is_ge() {
                return SstSplitType::Left;
            }
        } else if KeyComparator::compare_encoded_full_key(&split_key, &key_range.right).is_gt() {
            return SstSplitType::Left;
        }

        SstSplitType::Both
    }

    /// Split the SST into two parts based on the split key.
    /// The left part is the original SST, and the right part is the new SST.
    /// The split key is exclusive for the left part and inclusive for the right part.
    /// The `table_ids` of the new SST are calculated based on the split key.
    /// e.g.
    ///  `sst.table_ids` = [1, 2, 3, 4, 5, 6, 7, 8, 9], `split_key` = (`table_id` = 5, vnode = 0)
    ///  then the result:
    /// sst1 {
    ///     `sst_id`: `new_sst_id + 1`,
    ///     `table_ids`: [1, 2, 3, 4],
    ///     `key_range`: [left, `split_key`),
    ///     `sst_size`: `left_size`,
    /// }
    /// sst2 {
    ///    `sst_id`: `new_sst_id`,
    ///    `table_ids`: [5, 6, 7, 8, 9],
    ///    `key_range`: [`split_key`, right],
    ///    `sst_size`: `right_size`,
    /// }
    pub fn split_sst(
        origin_sst_info: SstableInfo,
        new_sst_id: &mut u64,
        split_key: Bytes,
        left_size: u64,
        right_size: u64,
    ) -> (Option<SstableInfo>, Option<SstableInfo>) {
        let mut origin_sst_info = origin_sst_info.get_inner();
        let mut branch_table_info = origin_sst_info.clone();
        branch_table_info.sst_id = *new_sst_id;
        *new_sst_id += 1;
        origin_sst_info.sst_id = *new_sst_id;
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
            split_table_ids_with_split_key(&origin_sst_info.table_ids, split_key.clone());

        // rebuild the key_range and size and sstable file size
        {
            // origin_sst_info
            origin_sst_info.key_range = key_range_l;
            origin_sst_info.sst_size = left_size;
            origin_sst_info.table_ids = table_ids_l;
        }

        {
            // new sst
            branch_table_info.key_range = key_range_r;
            branch_table_info.sst_size = right_size;
            branch_table_info.table_ids = table_ids_r;
        }

        // This function does not make any assumptions about the incoming sst, so add some judgement to ensure that the generated sst meets the restrictions.
        if origin_sst_info.table_ids.is_empty() {
            (None, Some(branch_table_info.into()))
        } else if branch_table_info.table_ids.is_empty() {
            (Some(origin_sst_info.into()), None)
        } else if KeyComparator::compare_encoded_full_key(
            &origin_sst_info.key_range.left,
            &origin_sst_info.key_range.right,
        )
        .is_eq()
        {
            // avoid empty key_range of origin_sst
            (None, Some(branch_table_info.into()))
        } else {
            (Some(origin_sst_info.into()), Some(branch_table_info.into()))
        }
    }

    /// The old split sst logic with `table_ids`
    /// This function is used to split the sst into two parts based on the `table_ids`.
    /// In contrast to `split_sst`, this function does not modify the `key_range` and does not guarantee that the split ssts can be merged, which needs to be guaranteed by the caller.
    pub fn split_sst_with_table_ids(
        origin_sst_info: &SstableInfo,
        new_sst_id: &mut u64,
        old_sst_size: u64,
        new_sst_size: u64,
        new_table_ids: Vec<u32>,
    ) -> (SstableInfo, SstableInfo) {
        let mut sst_info = origin_sst_info.get_inner();
        let mut branch_table_info = sst_info.clone();
        branch_table_info.sst_id = *new_sst_id;
        branch_table_info.sst_size = new_sst_size;
        *new_sst_id += 1;

        sst_info.sst_id = *new_sst_id;
        sst_info.sst_size = old_sst_size;
        *new_sst_id += 1;

        {
            // related github.com/risingwavelabs/risingwave/pull/17898/
            // This is a temporary implementation that will update `table_ids`` based on the new split rule after PR 17898
            // sst_info.table_ids = vec[1, 2, 3];
            // new_table_ids = vec[2, 3, 4];
            // branch_table_info.table_ids = vec[1, 2, 3] ∩ vec[2, 3, 4] = vec[2, 3]
            let set1: BTreeSet<_> = sst_info.table_ids.iter().cloned().collect();
            let set2: BTreeSet<_> = new_table_ids.into_iter().collect();
            let intersection: Vec<_> = set1.intersection(&set2).cloned().collect();

            // Update table_ids
            branch_table_info.table_ids = intersection;
            sst_info
                .table_ids
                .retain(|table_id| !branch_table_info.table_ids.contains(table_id));
        }

        (sst_info.into(), branch_table_info.into())
    }

    // Should avoid split same table_id into two groups
    pub fn split_table_ids_with_split_key(
        table_ids: &Vec<u32>,
        split_key: Bytes,
    ) -> (Vec<u32>, Vec<u32>) {
        assert!(table_ids.is_sorted());
        let split_full_key = FullKey::decode(&split_key);
        let split_user_key = split_full_key.user_key;
        let vnode = split_user_key.get_vnode_id();
        let table_id = split_user_key.table_id.table_id();
        split_table_ids_with_table_id_and_vnode(table_ids, table_id, vnode)
    }

    pub fn split_table_ids_with_table_id_and_vnode(
        table_ids: &Vec<u32>,
        table_id: StateTableId,
        vnode: usize,
    ) -> (Vec<u32>, Vec<u32>) {
        assert!(table_ids.is_sorted());
        assert_eq!(VirtualNode::ZERO, VirtualNode::from_index(vnode));
        let pos = table_ids.partition_point(|&id| id < table_id);
        (table_ids[..pos].to_vec(), table_ids[pos..].to_vec())
    }

    pub fn get_split_pos(sstables: &Vec<SstableInfo>, split_key: Bytes) -> usize {
        sstables
            .partition_point(|sst| {
                KeyComparator::compare_encoded_full_key(&sst.key_range.left, &split_key).is_lt()
            })
            .saturating_sub(1)
    }

    /// Merge the right levels into the left levels.
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

        // Reinitialise `vnode_partition_count` to avoid misaligned hierarchies
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

            let insert_table_infos = level.table_infos;
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
                    "left-group {} right-group {} left_levels.levels[{}].table_infos: {:?} level_idx {:?}",
                    left_levels.group_id,
                    right_levels.group_id,
                    idx,
                    left_levels.levels[idx].table_infos,
                    left_levels.levels[idx].level_idx
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

    /// Split the SSTs in the level according to the split key.
    pub fn split_sst_info_for_level_v2(
        level: &mut Level,
        new_sst_id: &mut u64,
        split_key: Bytes,
    ) -> Vec<SstableInfo> {
        if level.table_infos.is_empty() {
            return vec![];
        }

        let mut insert_table_infos = vec![];
        if level.level_type == PbLevelType::Overlapping {
            level.table_infos.retain_mut(|sst| {
                let sst_split_type = need_to_split(sst, split_key.clone());
                match sst_split_type {
                    SstSplitType::Left => true,
                    SstSplitType::Right => {
                        insert_table_infos.push(sst.clone());
                        false
                    }
                    SstSplitType::Both => {
                        let (left, right) = split_sst(
                            sst.clone(),
                            new_sst_id,
                            split_key.clone(),
                            sst.sst_size / 2,
                            sst.sst_size / 2,
                        );
                        if let Some(branch_sst) = right {
                            insert_table_infos.push(branch_sst);
                        }

                        if left.is_some() {
                            *sst = left.unwrap();
                            true
                        } else {
                            false
                        }
                    }
                }
            });
        } else {
            let pos = get_split_pos(&level.table_infos, split_key.clone());
            if pos >= level.table_infos.len() {
                return insert_table_infos;
            }

            let sst_split_type = need_to_split(&level.table_infos[pos], split_key.clone());
            match sst_split_type {
                SstSplitType::Left => {
                    insert_table_infos.extend_from_slice(&level.table_infos[pos + 1..]);
                    level.table_infos = level.table_infos[0..=pos].to_vec();
                }
                SstSplitType::Right => {
                    assert_eq!(0, pos);
                    insert_table_infos.extend_from_slice(&level.table_infos[pos..]); // the sst at pos has been split to the right
                    level.table_infos.clear();
                }
                SstSplitType::Both => {
                    // split the sst
                    let sst = level.table_infos[pos].clone();
                    let sst_size = sst.sst_size;
                    let (left, right) = split_sst(
                        sst,
                        new_sst_id,
                        split_key.clone(),
                        sst_size / 2,
                        sst_size / 2,
                    );

                    if let Some(branch_sst) = right {
                        insert_table_infos.push(branch_sst);
                    }

                    let right_start = pos + 1;
                    let left_end = pos;
                    // the sst at pos has been split to both left and right
                    // the branched sst has been inserted to the `insert_table_infos`
                    insert_table_infos.extend_from_slice(&level.table_infos[right_start..]);
                    level.table_infos = level.table_infos[0..=left_end].to_vec();
                    if let Some(origin_sst) = left {
                        // replace the origin sst with the left part
                        level.table_infos[left_end] = origin_sst;
                    } else {
                        level.table_infos.pop();
                    }
                }
            };
        }

        insert_table_infos
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::hash::VirtualNode;

    #[test]
    fn test_split_table_ids() {
        let table_ids = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        let (left, right) = super::group_split::split_table_ids_with_table_id_and_vnode(
            &table_ids,
            5,
            VirtualNode::ZERO.to_index(),
        );
        assert_eq!(left, vec![1, 2, 3, 4]);
        assert_eq!(right, vec![5, 6, 7, 8, 9]);

        // test table_id not in the table_ids

        let (left, right) = super::group_split::split_table_ids_with_table_id_and_vnode(
            &table_ids,
            10,
            VirtualNode::ZERO.to_index(),
        );
        assert_eq!(left, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
        assert!(right.is_empty());

        let (left, right) = super::group_split::split_table_ids_with_table_id_and_vnode(
            &table_ids,
            0,
            VirtualNode::ZERO.to_index(),
        );

        assert!(left.is_empty());
        assert_eq!(right, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[test]
    fn test_split_table_ids_with_split_key() {
        let table_ids = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        let split_key = super::group_split::build_split_key(5, VirtualNode::ZERO);
        let (left, right) =
            super::group_split::split_table_ids_with_split_key(&table_ids, split_key);
        assert_eq!(left, vec![1, 2, 3, 4]);
        assert_eq!(right, vec![5, 6, 7, 8, 9]);
    }
}

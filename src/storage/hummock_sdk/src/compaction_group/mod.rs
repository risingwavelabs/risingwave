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

    use super::hummock_version_ext::insert_new_sub_level;
    use crate::can_concat;
    use crate::level::{Level, Levels};

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
}

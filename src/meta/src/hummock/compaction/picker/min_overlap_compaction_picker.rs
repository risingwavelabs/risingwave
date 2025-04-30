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

use std::sync::Arc;

use risingwave_hummock_sdk::append_sstable_info_to_string;
use risingwave_hummock_sdk::level::{InputLevel, Level, Levels};
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_pb::hummock::LevelType;

use super::{CompactionInput, CompactionPicker, LocalPickerStatistic};
use crate::hummock::compaction::overlap_strategy::OverlapStrategy;
use crate::hummock::level_handler::LevelHandler;

pub struct MinOverlappingPicker {
    level: usize,
    target_level: usize,
    max_select_bytes: u64,
    vnode_partition_count: u32,
    overlap_strategy: Arc<dyn OverlapStrategy>,
}

impl MinOverlappingPicker {
    pub fn new(
        level: usize,
        target_level: usize,
        max_select_bytes: u64,
        vnode_partition_count: u32,
        overlap_strategy: Arc<dyn OverlapStrategy>,
    ) -> MinOverlappingPicker {
        MinOverlappingPicker {
            level,
            target_level,
            max_select_bytes,
            vnode_partition_count,
            overlap_strategy,
        }
    }

    pub fn pick_tables(
        &self,
        select_tables: &[SstableInfo],
        target_tables: &[SstableInfo],
        level_handlers: &[LevelHandler],
    ) -> (Vec<SstableInfo>, Vec<SstableInfo>) {
        let mut select_file_ranges = vec![];
        for (idx, sst) in select_tables.iter().enumerate() {
            if level_handlers[self.level].is_pending_compact(&sst.sst_id) {
                continue;
            }
            let mut overlap_info = self.overlap_strategy.create_overlap_info();
            overlap_info.update(sst);
            let overlap_files_range = overlap_info.check_multiple_overlap(target_tables);

            if overlap_files_range.is_empty() {
                return (vec![sst.clone()], vec![]);
            }
            select_file_ranges.push((idx, overlap_files_range));
        }
        select_file_ranges.retain(|(_, range)| {
            let mut pending_compact = false;
            for other in &target_tables[range.clone()] {
                if level_handlers[self.target_level].is_pending_compact(&other.sst_id) {
                    pending_compact = true;
                    break;
                }
            }
            !pending_compact
        });

        let mut min_score = u64::MAX;
        let mut min_score_select_range = 0..0;
        let mut min_score_target_range = 0..0;
        let mut min_score_select_file_size = 0;
        for left in 0..select_file_ranges.len() {
            let mut select_file_size = 0;
            let mut target_level_overlap_range = select_file_ranges[left].1.clone();
            let mut total_file_size = 0;
            for other in &target_tables[target_level_overlap_range.clone()] {
                total_file_size += other.sst_size;
            }
            let start_idx = select_file_ranges[left].0;
            let mut end_idx = start_idx + 1;
            for (idx, range) in select_file_ranges.iter().skip(left) {
                if select_file_size > self.max_select_bytes
                    || *idx > end_idx
                    || range.start >= target_level_overlap_range.end
                {
                    break;
                }
                select_file_size += select_tables[*idx].sst_size;
                if range.end > target_level_overlap_range.end {
                    for other in &target_tables[target_level_overlap_range.end..range.end] {
                        total_file_size += other.sst_size;
                    }
                    target_level_overlap_range.end = range.end;
                }
                let score = if select_file_size == 0 {
                    total_file_size
                } else {
                    total_file_size * 100 / select_file_size
                };
                end_idx = idx + 1;
                if score < min_score
                    || (score == min_score && select_file_size < min_score_select_file_size)
                {
                    min_score = score;
                    min_score_select_range = start_idx..end_idx;
                    min_score_target_range = target_level_overlap_range.clone();
                    min_score_select_file_size = select_file_size;
                }
            }
        }
        if min_score == u64::MAX {
            return (vec![], vec![]);
        }
        let select_input_ssts = select_tables[min_score_select_range].to_vec();
        let target_input_ssts = target_tables[min_score_target_range].to_vec();
        (select_input_ssts, target_input_ssts)
    }
}

impl CompactionPicker for MinOverlappingPicker {
    fn pick_compaction(
        &mut self,
        levels: &Levels,
        level_handlers: &[LevelHandler],
        stats: &mut LocalPickerStatistic,
    ) -> Option<CompactionInput> {
        assert!(self.level > 0);
        let (select_input_ssts, target_input_ssts) = self.pick_tables(
            &levels.get_level(self.level).table_infos,
            &levels.get_level(self.target_level).table_infos,
            level_handlers,
        );
        if select_input_ssts.is_empty() {
            stats.skip_by_pending_files += 1;
            return None;
        }
        Some(CompactionInput {
            select_input_size: select_input_ssts.iter().map(|sst| sst.sst_size).sum(),
            target_input_size: target_input_ssts.iter().map(|sst| sst.sst_size).sum(),
            total_file_count: (select_input_ssts.len() + target_input_ssts.len()) as u64,
            input_levels: vec![
                InputLevel {
                    level_idx: self.level as u32,
                    level_type: LevelType::Nonoverlapping,
                    table_infos: select_input_ssts,
                },
                InputLevel {
                    level_idx: self.target_level as u32,
                    level_type: LevelType::Nonoverlapping,
                    table_infos: target_input_ssts,
                },
            ],
            target_level: self.target_level,
            vnode_partition_count: self.vnode_partition_count,
            ..Default::default()
        })
    }
}

#[derive(Default, Debug)]
pub struct SubLevelSstables {
    pub total_file_size: u64,
    pub total_file_count: usize,
    pub sstable_infos: Vec<(u64, Vec<SstableInfo>)>,
    pub expected: bool,
}

pub struct NonOverlapSubLevelPicker {
    min_compaction_bytes: u64,
    max_compaction_bytes: u64,
    min_expected_level_count: usize,
    max_file_count: u64,
    overlap_strategy: Arc<dyn OverlapStrategy>,
    enable_check_task_level_overlap: bool,
    max_expected_level_count: usize,
    enable_optimize_l0_interval_selection: bool,
}

impl NonOverlapSubLevelPicker {
    pub fn new(
        min_compaction_bytes: u64,
        max_compaction_bytes: u64,
        min_expected_level_count: usize,
        max_file_count: u64,
        overlap_strategy: Arc<dyn OverlapStrategy>,
        enable_check_task_level_overlap: bool,
        max_expected_level_count: usize,
        enable_optimize_l0_interval_selection: bool,
    ) -> Self {
        Self {
            min_compaction_bytes,
            max_compaction_bytes,
            min_expected_level_count,
            max_file_count,
            overlap_strategy,
            enable_check_task_level_overlap,
            max_expected_level_count,
            enable_optimize_l0_interval_selection,
        }
    }

    #[cfg(test)]
    pub fn for_test(
        min_compaction_bytes: u64,
        max_compaction_bytes: u64,
        min_expected_level_count: usize,
        max_file_count: u64,
        overlap_strategy: Arc<dyn OverlapStrategy>,
        enable_check_task_level_overlap: bool,
        max_expected_level_count: usize,
        enable_optimize_l0_interval_selection: bool,
    ) -> Self {
        Self {
            min_compaction_bytes,
            max_compaction_bytes,
            min_expected_level_count,
            max_file_count,
            overlap_strategy,
            enable_check_task_level_overlap,
            max_expected_level_count,
            enable_optimize_l0_interval_selection,
        }
    }

    // Use the incoming sst as the basic range and select as many sub levels as possible
    // 1. Build basic range based on sst
    // 2. Add a new sub level in each round
    // 3. Expand sst according to the basic range from new to old sub levels.
    // 4. According to the size and count restrictions, select the plan that contains the most sub levels as much as possible
    fn pick_sub_level(
        &self,
        levels: &[Level],
        level_handler: &LevelHandler,
        sst: &SstableInfo,
    ) -> Option<SubLevelSstables> {
        let mut ret = SubLevelSstables::default();
        for sub_level in levels {
            ret.sstable_infos.push((sub_level.sub_level_id, vec![]));
        }

        let mut pick_levels_range = Vec::default();
        let mut max_select_level_count = 0;

        // Pay attention to the order here: Make sure to select the lowest sub_level to meet the requirements of base compaction. If you break the assumption of this order, you need to redesign it.
        // TODO: Use binary selection to replace the step algorithm to optimize algorithm complexity
        'expand_new_level: for (target_index, target_level) in levels.iter().enumerate() {
            if target_level.level_type != LevelType::Nonoverlapping {
                break;
            }

            if ret
                .sstable_infos
                .iter()
                .filter(|(_sub_level_id, ssts)| !ssts.is_empty())
                .count()
                > self.max_expected_level_count
            {
                break;
            }

            // reset the `basic_overlap_info` with basic sst
            let mut basic_overlap_info = self.overlap_strategy.create_overlap_info();
            basic_overlap_info.update(sst);

            let mut overlap_files_range =
                basic_overlap_info.check_multiple_include(&target_level.table_infos);
            if overlap_files_range.is_empty() {
                overlap_files_range =
                    basic_overlap_info.check_multiple_overlap(&target_level.table_infos);
            }

            if overlap_files_range.is_empty() {
                continue;
            }

            let mut overlap_levels = vec![];

            let mut add_files_size: u64 = 0;
            let mut add_files_count: usize = 0;

            let mut select_level_count = 0;
            for reverse_index in (0..=target_index).rev() {
                let target_tables = &levels[reverse_index].table_infos;

                overlap_files_range = if target_index == reverse_index {
                    overlap_files_range
                } else {
                    basic_overlap_info.check_multiple_overlap(target_tables)
                };

                // We allow a layer in the middle without overlap, so we need to continue to
                // the next layer to search for overlap
                if overlap_files_range.is_empty() {
                    // empty level
                    continue;
                }

                for other in &target_tables[overlap_files_range.clone()] {
                    if level_handler.is_pending_compact(&other.sst_id) {
                        break 'expand_new_level;
                    }
                    basic_overlap_info.update(other);

                    add_files_size += other.sst_size;
                    add_files_count += 1;
                }

                overlap_levels.push((reverse_index, overlap_files_range.clone()));
                select_level_count += 1;
            }

            if select_level_count > max_select_level_count {
                max_select_level_count = select_level_count;
                pick_levels_range = overlap_levels;
            }

            // When size / file count has exceeded the limit, we need to abandon this plan, it cannot be expanded to the last sub_level
            if max_select_level_count >= self.min_expected_level_count
                && (add_files_size >= self.max_compaction_bytes
                    || add_files_count >= self.max_file_count as usize)
            {
                break 'expand_new_level;
            }
        }

        if !pick_levels_range.is_empty() {
            for (reverse_index, sst_range) in pick_levels_range {
                let level_ssts = &levels[reverse_index].table_infos;
                ret.sstable_infos[reverse_index].1 = level_ssts[sst_range].to_vec();
                ret.total_file_count += ret.sstable_infos[reverse_index].1.len();
                ret.total_file_size += ret.sstable_infos[reverse_index]
                    .1
                    .iter()
                    .map(|sst| sst.sst_size)
                    .sum::<u64>();
            }

            // sort sst per level due to reverse expand
            ret.sstable_infos
                .iter_mut()
                .for_each(|(_sub_level_id, level_ssts)| {
                    level_ssts.sort_by(|sst1, sst2| sst1.key_range.cmp(&sst2.key_range));
                });
        } else {
            ret.total_file_count = 0;
            ret.total_file_size = 0;
        }

        if self.enable_check_task_level_overlap {
            self.verify_task_level_overlap(&ret, levels);
        }

        ret.sstable_infos
            .retain(|(_sub_level_id, ssts)| !ssts.is_empty());

        // To check whether the task is expected
        if ret.total_file_size > self.max_compaction_bytes
            || ret.total_file_count as u64 > self.max_file_count
            || ret.sstable_infos.len() > self.max_expected_level_count
        {
            // rotate the sstables to meet the `max_file_count` and `max_compaction_bytes` and `max_expected_level_count`
            let mut total_file_count = 0;
            let mut total_file_size = 0;
            let mut total_level_count = 0;
            for (index, (_sub_level_id, sstables)) in ret.sstable_infos.iter().enumerate() {
                total_file_count += sstables.len();
                total_file_size += sstables.iter().map(|sst| sst.sst_size).sum::<u64>();
                total_level_count += 1;

                // Atleast `min_expected_level_count`` level should be selected
                if total_level_count >= self.min_expected_level_count
                    && (total_file_count as u64 >= self.max_file_count
                        || total_file_size >= self.max_compaction_bytes
                        || total_level_count >= self.max_expected_level_count)
                {
                    ret.total_file_count = total_file_count;
                    ret.total_file_size = total_file_size;
                    ret.sstable_infos.truncate(index + 1);
                    break;
                }
            }
        }

        if ret.sstable_infos.is_empty() {
            return None;
        }

        Some(ret)
    }

    pub fn pick_l0_multi_non_overlap_level(
        &self,
        l0: &[Level],
        level_handler: &LevelHandler,
    ) -> Vec<SubLevelSstables> {
        if l0.len() < self.min_expected_level_count {
            return vec![];
        }

        let mut scores = vec![];
        // To find the sub_level with the most files;
        let intervals_level_idx = if self.enable_optimize_l0_interval_selection {
            l0.iter()
                .enumerate()
                .max_by(|(idx1, levels1), (idx2, levels2)| {
                    levels1
                        .table_infos
                        .len()
                        .cmp(&levels2.table_infos.len())
                        .then(idx2.cmp(idx1))
                })
                .map(|(idx, _)| idx)
                .unwrap()
        } else {
            0
        };

        let intervals = &l0[intervals_level_idx].table_infos;
        for sst in intervals {
            if level_handler.is_pending_compact(&sst.sst_id) {
                continue;
            }

            if let Some(score) = self.pick_sub_level(l0, level_handler, sst) {
                scores.push(score);
            }
        }

        if scores.is_empty() {
            return vec![];
        }

        let mut expected = Vec::with_capacity(scores.len());
        let mut unexpected = vec![];

        for mut selected_task in scores {
            if selected_task.sstable_infos.len() > self.max_expected_level_count
                || selected_task.sstable_infos.len() < self.min_expected_level_count
                || selected_task.total_file_size < self.min_compaction_bytes
            {
                selected_task.expected = false;
                unexpected.push(selected_task);
            } else {
                selected_task.expected = true;
                expected.push(selected_task);
            }
        }

        // The logic of sorting depends on the interval we expect to select.
        // 1. contain as many levels as possible
        // 2. fewer files in the bottom sub level, containing as many smaller intervals as possible.
        expected.sort_by(|a, b| {
            b.sstable_infos
                .len()
                .cmp(&a.sstable_infos.len())
                .then_with(|| a.total_file_count.cmp(&b.total_file_count))
                .then_with(|| a.total_file_size.cmp(&b.total_file_size))
        });

        // For unexpected tasks, We devised a separate algorithm to evaluate the priority of a task, based on the limit passed in,
        // we set tasks close to the limit to be high priority, here have three attributes:
        // 1. The number of levels selected is close to the limit
        // 2. The number of files selected is close to the limit
        // 3. The size of the selected file is close to the limit
        unexpected.sort_by(|a, b| {
            let a_select_count_offset =
                (a.sstable_infos.len() as i64 - self.max_expected_level_count as i64).abs();
            let b_select_count_offset =
                (b.sstable_infos.len() as i64 - self.max_expected_level_count as i64).abs();

            let a_file_count_offset =
                (a.total_file_count as i64 - self.max_file_count as i64).abs();
            let b_file_count_offset =
                (b.total_file_count as i64 - self.max_file_count as i64).abs();

            let a_file_size_offset =
                (a.total_file_size as i64 - self.max_compaction_bytes as i64).abs();
            let b_file_size_offset =
                (b.total_file_size as i64 - self.max_compaction_bytes as i64).abs();

            a_select_count_offset
                .cmp(&b_select_count_offset)
                .then_with(|| a_file_count_offset.cmp(&b_file_count_offset))
                .then_with(|| a_file_size_offset.cmp(&b_file_size_offset))
        });

        expected.extend(unexpected);

        expected
    }

    fn verify_task_level_overlap(&self, ret: &SubLevelSstables, levels: &[Level]) {
        use std::fmt::Write;

        use itertools::Itertools;

        use crate::hummock::compaction::overlap_strategy::OverlapInfo;
        let mut overlap_info: Option<Box<dyn OverlapInfo>> = None;
        for (level_idx, (_sub_level_id, ssts)) in ret.sstable_infos.iter().enumerate().rev() {
            if let Some(overlap_info) = overlap_info.as_mut() {
                // skip the check if `overlap_info` is not initialized (i.e. the first non-empty level is not met)
                let level = levels.get(level_idx).unwrap();
                let overlap_sst_range = overlap_info.check_multiple_overlap(&level.table_infos);
                if !overlap_sst_range.is_empty() {
                    let expected_sst_ids = level.table_infos[overlap_sst_range.clone()]
                        .iter()
                        .map(|s| s.object_id)
                        .collect_vec();
                    let actual_sst_ids = ssts.iter().map(|s| s.object_id).collect_vec();
                    // `actual_sst_ids` can be larger than `expected_sst_ids` because we may use a larger key range to select SSTs.
                    // `expected_sst_ids` must be a sub-range of `actual_sst_ids` to ensure correctness.
                    let start_idx = actual_sst_ids
                        .iter()
                        .position(|sst_id| sst_id == expected_sst_ids.first().unwrap());
                    if start_idx.map_or(true, |idx| {
                        actual_sst_ids[idx..idx + expected_sst_ids.len()] != expected_sst_ids
                    }) {
                        // Print SstableInfo for `actual_sst_ids`
                        let mut actual_sst_infos = String::new();
                        ssts.iter()
                            .for_each(|s| append_sstable_info_to_string(&mut actual_sst_infos, s));

                        // Print SstableInfo for `expected_sst_ids`
                        let mut expected_sst_infos = String::new();
                        level.table_infos[overlap_sst_range.clone()]
                            .iter()
                            .for_each(|s| {
                                append_sstable_info_to_string(&mut expected_sst_infos, s)
                            });

                        // Print SstableInfo for selected ssts in all sub-levels
                        let mut ret_sst_infos = String::new();
                        ret.sstable_infos.iter().enumerate().for_each(
                            |(idx, (_sub_level_id, ssts))| {
                                writeln!(
                                    ret_sst_infos,
                                    "sub level {}",
                                    levels.get(idx).unwrap().sub_level_id
                                )
                                .unwrap();
                                ssts.iter().for_each(|s| {
                                    append_sstable_info_to_string(&mut ret_sst_infos, s)
                                });
                            },
                        );
                        panic!(
                            "Compact task overlap check fails. Actual: {} Expected: {} Ret {}",
                            actual_sst_infos, expected_sst_infos, ret_sst_infos
                        );
                    }
                }
            } else if !ssts.is_empty() {
                // init the `overlap_info` when meeting the first non-empty level.
                overlap_info = Some(self.overlap_strategy.create_overlap_info());
            }

            for sst in ssts {
                overlap_info.as_mut().unwrap().update(sst);
            }
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::collections::BTreeSet;

    use risingwave_common::config::default::compaction_config;

    use super::*;
    use crate::hummock::compaction::overlap_strategy::RangeOverlapStrategy;
    use crate::hummock::compaction::selector::tests::{
        generate_l0_nonoverlapping_sublevels, generate_table,
    };

    #[test]
    fn test_compact_l1() {
        let mut picker =
            MinOverlappingPicker::new(1, 2, 10000, 0, Arc::new(RangeOverlapStrategy::default()));
        let levels = vec![
            Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![
                    generate_table(0, 1, 0, 100, 1),
                    generate_table(1, 1, 101, 200, 1),
                    generate_table(2, 1, 222, 300, 1),
                ],
                ..Default::default()
            },
            Level {
                level_idx: 2,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![
                    generate_table(4, 1, 0, 100, 1),
                    generate_table(5, 1, 101, 150, 1),
                    generate_table(6, 1, 151, 201, 1),
                    generate_table(7, 1, 501, 800, 1),
                    generate_table(8, 2, 301, 400, 1),
                ],
                ..Default::default()
            },
        ];
        let levels = Levels {
            levels,
            l0: generate_l0_nonoverlapping_sublevels(vec![]),
            ..Default::default()
        };
        let mut level_handlers = vec![
            LevelHandler::new(0),
            LevelHandler::new(1),
            LevelHandler::new(2),
        ];

        // pick a non-overlapping files. It means that this file could be trivial move to next
        // level.
        let mut local_stats = LocalPickerStatistic::default();
        let ret = picker
            .pick_compaction(&levels, &level_handlers, &mut local_stats)
            .unwrap();
        assert_eq!(ret.input_levels[0].level_idx, 1);
        assert_eq!(ret.target_level, 2);
        assert_eq!(ret.input_levels[0].table_infos.len(), 1);
        assert_eq!(ret.input_levels[0].table_infos[0].sst_id, 2);
        assert_eq!(ret.input_levels[1].table_infos.len(), 0);
        ret.add_pending_task(0, &mut level_handlers);

        let ret = picker
            .pick_compaction(&levels, &level_handlers, &mut local_stats)
            .unwrap();
        assert_eq!(ret.input_levels[0].level_idx, 1);
        assert_eq!(ret.target_level, 2);
        assert_eq!(ret.input_levels[0].table_infos.len(), 1);
        assert_eq!(ret.input_levels[0].table_infos[0].sst_id, 0);
        assert_eq!(ret.input_levels[1].table_infos.len(), 1);
        assert_eq!(ret.input_levels[1].table_infos[0].sst_id, 4);
        ret.add_pending_task(1, &mut level_handlers);

        let ret = picker
            .pick_compaction(&levels, &level_handlers, &mut local_stats)
            .unwrap();
        assert_eq!(ret.input_levels[0].table_infos.len(), 1);
        assert_eq!(ret.input_levels[0].table_infos[0].sst_id, 1);
        assert_eq!(ret.input_levels[1].table_infos.len(), 2);
        assert_eq!(ret.input_levels[1].table_infos[0].sst_id, 5);
    }

    #[test]
    fn test_expand_l1_files() {
        let mut picker =
            MinOverlappingPicker::new(1, 2, 10000, 0, Arc::new(RangeOverlapStrategy::default()));
        let levels = vec![
            Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![
                    generate_table(0, 1, 50, 99, 2),
                    generate_table(1, 1, 100, 149, 2),
                    generate_table(2, 1, 150, 249, 2),
                ],
                ..Default::default()
            },
            Level {
                level_idx: 2,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![
                    generate_table(4, 1, 50, 199, 1),
                    generate_table(5, 1, 200, 399, 1),
                ],
                ..Default::default()
            },
        ];
        let levels = Levels {
            levels,
            l0: generate_l0_nonoverlapping_sublevels(vec![]),
            ..Default::default()
        };
        let levels_handler = vec![
            LevelHandler::new(0),
            LevelHandler::new(1),
            LevelHandler::new(2),
        ];

        // pick a non-overlapping files. It means that this file could be trivial move to next
        // level.
        let ret = picker
            .pick_compaction(
                &levels,
                &levels_handler,
                &mut LocalPickerStatistic::default(),
            )
            .unwrap();
        assert_eq!(ret.input_levels[0].level_idx, 1);
        assert_eq!(ret.input_levels[1].level_idx, 2);

        assert_eq!(ret.input_levels[0].table_infos.len(), 2);
        assert_eq!(ret.input_levels[0].table_infos[0].sst_id, 0);
        assert_eq!(ret.input_levels[0].table_infos[1].sst_id, 1);

        assert_eq!(ret.input_levels[1].table_infos.len(), 1);
        assert_eq!(ret.input_levels[1].table_infos[0].sst_id, 4);
    }

    #[test]
    fn test_pick_l0_multi_level() {
        let levels = vec![
            Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![
                    generate_table(0, 1, 50, 99, 2),
                    generate_table(1, 1, 100, 149, 2),
                    generate_table(2, 1, 150, 249, 2),
                    generate_table(6, 1, 250, 300, 2),
                    generate_table(7, 1, 350, 400, 2),
                    generate_table(8, 1, 450, 500, 2),
                ],
                total_file_size: 800,
                ..Default::default()
            },
            Level {
                level_idx: 2,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![
                    generate_table(4, 1, 50, 199, 1),
                    generate_table(5, 1, 200, 249, 1),
                    generate_table(9, 1, 250, 300, 2),
                    generate_table(10, 1, 350, 400, 2),
                    generate_table(11, 1, 450, 500, 2),
                ],
                total_file_size: 350,
                ..Default::default()
            },
            Level {
                level_idx: 3,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![
                    generate_table(11, 1, 250, 300, 2),
                    generate_table(12, 1, 350, 400, 2),
                    generate_table(13, 1, 450, 500, 2),
                ],
                total_file_size: 150,
                ..Default::default()
            },
            Level {
                level_idx: 4,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![
                    generate_table(14, 1, 250, 300, 2),
                    generate_table(15, 1, 350, 400, 2),
                    generate_table(16, 1, 450, 500, 2),
                ],
                total_file_size: 150,
                ..Default::default()
            },
        ];

        let levels_handlers = vec![
            LevelHandler::new(0),
            LevelHandler::new(1),
            LevelHandler::new(2),
        ];

        {
            // no limit
            let picker = NonOverlapSubLevelPicker::new(
                0,
                10000,
                1,
                10000,
                Arc::new(RangeOverlapStrategy::default()),
                true,
                compaction_config::max_l0_compact_level_count() as usize,
                true,
            );
            let ret = picker.pick_l0_multi_non_overlap_level(&levels, &levels_handlers[0]);
            assert_eq!(6, ret.len());
        }

        {
            // limit max bytes
            let picker = NonOverlapSubLevelPicker::new(
                0,
                100,
                1,
                10000,
                Arc::new(RangeOverlapStrategy::default()),
                true,
                compaction_config::max_l0_compact_level_count() as usize,
                true,
            );
            let ret = picker.pick_l0_multi_non_overlap_level(&levels, &levels_handlers[0]);
            assert_eq!(6, ret.len());
        }

        {
            // limit max file_count
            let picker = NonOverlapSubLevelPicker::new(
                0,
                10000,
                1,
                5,
                Arc::new(RangeOverlapStrategy::default()),
                true,
                compaction_config::max_l0_compact_level_count() as usize,
                true,
            );
            let ret = picker.pick_l0_multi_non_overlap_level(&levels, &levels_handlers[0]);
            assert_eq!(6, ret.len());
        }
    }

    #[test]
    fn test_pick_l0_multi_level2() {
        let levels = vec![
            Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![
                    generate_table(0, 1, 50, 99, 2),
                    generate_table(1, 1, 100, 149, 2),
                    generate_table(2, 1, 150, 249, 2),
                    generate_table(6, 1, 250, 300, 2),
                    generate_table(7, 1, 350, 400, 2),
                    generate_table(8, 1, 450, 500, 2),
                ],
                total_file_size: 800,
                ..Default::default()
            },
            Level {
                level_idx: 2,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![
                    generate_table(4, 1, 50, 99, 1),
                    generate_table(5, 1, 150, 200, 1),
                    generate_table(9, 1, 250, 300, 2),
                    generate_table(10, 1, 350, 400, 2),
                    generate_table(11, 1, 450, 500, 2),
                ],
                total_file_size: 250,
                ..Default::default()
            },
            Level {
                level_idx: 3,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![
                    generate_table(11, 1, 250, 300, 2),
                    generate_table(12, 1, 350, 400, 2),
                    generate_table(13, 1, 450, 500, 2),
                ],
                total_file_size: 150,
                ..Default::default()
            },
            Level {
                level_idx: 4,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![
                    generate_table(14, 1, 250, 300, 2),
                    generate_table(15, 1, 350, 400, 2),
                    generate_table(16, 1, 450, 500, 2),
                ],
                total_file_size: 150,
                ..Default::default()
            },
        ];

        let levels_handlers = vec![
            LevelHandler::new(0),
            LevelHandler::new(1),
            LevelHandler::new(2),
        ];

        {
            // no limit
            let picker = NonOverlapSubLevelPicker::new(
                0,
                10000,
                1,
                10000,
                Arc::new(RangeOverlapStrategy::default()),
                true,
                compaction_config::max_l0_compact_level_count() as usize,
                true,
            );
            let ret = picker.pick_l0_multi_non_overlap_level(&levels, &levels_handlers[0]);
            assert_eq!(6, ret.len());
        }

        {
            // limit max bytes
            let max_compaction_bytes = 100;
            let picker = NonOverlapSubLevelPicker::new(
                60,
                max_compaction_bytes,
                1,
                10000,
                Arc::new(RangeOverlapStrategy::default()),
                true,
                compaction_config::max_l0_compact_level_count() as usize,
                true,
            );
            let ret = picker.pick_l0_multi_non_overlap_level(&levels, &levels_handlers[0]);
            assert_eq!(6, ret.len());

            for plan in &ret {
                if plan.total_file_size >= max_compaction_bytes {
                    assert!(plan.expected);
                } else {
                    assert!(!plan.expected);
                }
            }
        }

        {
            // limit max file_count
            let max_file_count = 2;
            let picker = NonOverlapSubLevelPicker::new(
                0,
                10000,
                1,
                max_file_count,
                Arc::new(RangeOverlapStrategy::default()),
                true,
                compaction_config::max_l0_compact_level_count() as usize,
                true,
            );
            let ret = picker.pick_l0_multi_non_overlap_level(&levels, &levels_handlers[0]);
            assert_eq!(6, ret.len());

            for plan in ret {
                let mut sst_id_set = BTreeSet::default();
                for sst in &plan.sstable_infos {
                    sst_id_set.insert(sst.1[0].sst_id);
                }
                assert!(sst_id_set.len() <= max_file_count as usize);
            }
        }

        {
            // limit min_depth
            let min_depth = 3;
            let picker = NonOverlapSubLevelPicker::new(
                10,
                10000,
                min_depth,
                100,
                Arc::new(RangeOverlapStrategy::default()),
                true,
                compaction_config::max_l0_compact_level_count() as usize,
                true,
            );
            let ret = picker.pick_l0_multi_non_overlap_level(&levels, &levels_handlers[0]);
            assert_eq!(6, ret.len());

            for plan in ret {
                if plan.sstable_infos.len() >= min_depth {
                    assert!(plan.expected);
                } else {
                    assert!(!plan.expected);
                }
            }
        }
    }

    #[test]
    fn test_trivial_move_bug() {
        let levels = [
            Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![generate_table(0, 1, 400, 500, 2)],
                total_file_size: 100,
                ..Default::default()
            },
            Level {
                level_idx: 2,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![
                    generate_table(1, 1, 100, 200, 1),
                    generate_table(2, 1, 600, 700, 1),
                ],
                total_file_size: 200,
                ..Default::default()
            },
            Level {
                level_idx: 3,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![
                    generate_table(3, 1, 100, 300, 2),
                    generate_table(4, 1, 600, 800, 1),
                ],
                total_file_size: 400,
                ..Default::default()
            },
        ];

        let levels_handlers = vec![
            LevelHandler::new(0),
            LevelHandler::new(1),
            LevelHandler::new(2),
            LevelHandler::new(3),
        ];
        // no limit
        let picker =
            MinOverlappingPicker::new(2, 3, 1000, 0, Arc::new(RangeOverlapStrategy::default()));
        let (select_files, target_files) = picker.pick_tables(
            &levels[1].table_infos,
            &levels[2].table_infos,
            &levels_handlers,
        );
        let overlap_strategy = Arc::new(RangeOverlapStrategy::default());
        let mut overlap_info = overlap_strategy.create_overlap_info();
        for sst in &select_files {
            overlap_info.update(sst);
        }
        let range = overlap_info.check_multiple_overlap(&levels[0].table_infos);
        assert!(range.is_empty());
        assert_eq!(select_files.len(), 1);
        assert_eq!(target_files.len(), 1);
    }

    #[test]
    fn test_pick_unexpected_task() {
        let levels = vec![
            Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![generate_table(0, 1, 50, 100, 2)], // 50
                total_file_size: 50,
                ..Default::default()
            },
            Level {
                level_idx: 2,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![
                    generate_table(1, 1, 101, 150, 1), // 50
                ],
                total_file_size: 50,
                ..Default::default()
            },
            Level {
                level_idx: 3,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![
                    generate_table(2, 1, 151, 200, 2), // 50
                ],
                total_file_size: 50,
                ..Default::default()
            },
            Level {
                level_idx: 4,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![
                    generate_table(3, 1, 50, 300, 2), // 250
                ],
                total_file_size: 250,
                ..Default::default()
            },
        ];

        let levels_handlers = vec![
            LevelHandler::new(0),
            LevelHandler::new(1),
            LevelHandler::new(2),
            LevelHandler::new(3),
            LevelHandler::new(4),
        ];

        {
            // no limit
            let picker = NonOverlapSubLevelPicker::new(
                0,
                10000,
                1,
                10000,
                Arc::new(RangeOverlapStrategy::default()),
                true,
                compaction_config::max_l0_compact_level_count() as usize,
                true,
            );
            let ret = picker.pick_l0_multi_non_overlap_level(&levels, &levels_handlers[0]);
            {
                let plan = &ret[0];
                assert_eq!(4, plan.sstable_infos.len());
            }
        }

        {
            // limit size
            let picker = NonOverlapSubLevelPicker::new(
                0,
                150,
                1,
                10000,
                Arc::new(RangeOverlapStrategy::default()),
                true,
                compaction_config::max_l0_compact_level_count() as usize,
                true,
            );
            let ret = picker.pick_l0_multi_non_overlap_level(&levels, &levels_handlers[0]);
            {
                let plan = &ret[0];
                assert_eq!(3, plan.sstable_infos.len());
            }
        }

        {
            // limit count
            let picker = NonOverlapSubLevelPicker::new(
                0,
                10000,
                1,
                3,
                Arc::new(RangeOverlapStrategy::default()),
                true,
                compaction_config::max_l0_compact_level_count() as usize,
                true,
            );
            let ret = picker.pick_l0_multi_non_overlap_level(&levels, &levels_handlers[0]);
            {
                let plan = &ret[0];
                assert_eq!(3, plan.sstable_infos.len());
            }
        }

        {
            // limit expected level count
            let max_expected_level_count = 3;
            let picker = NonOverlapSubLevelPicker::for_test(
                0,
                10000,
                1,
                3,
                Arc::new(RangeOverlapStrategy::default()),
                true,
                max_expected_level_count,
                true,
            );
            let ret = picker.pick_l0_multi_non_overlap_level(&levels, &levels_handlers[0]);
            {
                let plan = &ret[0];
                assert_eq!(max_expected_level_count, plan.sstable_infos.len());

                assert_eq!(0, plan.sstable_infos[0].1[0].sst_id);
                assert_eq!(1, plan.sstable_infos[1].1[0].sst_id);
                assert_eq!(2, plan.sstable_infos[2].1[0].sst_id);
            }
        }

        {
            // limit min_compacaion_bytes
            let max_expected_level_count = 100;
            let picker = NonOverlapSubLevelPicker::for_test(
                1000,
                10000,
                1,
                100,
                Arc::new(RangeOverlapStrategy::default()),
                true,
                max_expected_level_count,
                true,
            );
            let ret = picker.pick_l0_multi_non_overlap_level(&levels, &levels_handlers[0]);
            {
                let plan = &ret[0];

                assert_eq!(0, plan.sstable_infos[0].1[0].sst_id);
                assert_eq!(1, plan.sstable_infos[1].1[0].sst_id);
                assert_eq!(2, plan.sstable_infos[2].1[0].sst_id);
                assert_eq!(3, plan.sstable_infos[3].1[0].sst_id);
                assert!(!plan.expected);
            }
        }
    }
}

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

use std::collections::BTreeSet;
use std::sync::Arc;

use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockLevelsExt;
use risingwave_hummock_sdk::prost_key_range::KeyRangeExt;
use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::{InputLevel, Level, LevelType, SstableInfo};

use super::{CompactionInput, CompactionPicker, LocalPickerStatistic, MAX_COMPACT_LEVEL_COUNT};
use crate::hummock::compaction::overlap_strategy::OverlapStrategy;
use crate::hummock::level_handler::LevelHandler;

pub struct MinOverlappingPicker {
    level: usize,
    target_level: usize,
    max_select_bytes: u64,
    split_by_table: bool,
    overlap_strategy: Arc<dyn OverlapStrategy>,
}

impl MinOverlappingPicker {
    pub fn new(
        level: usize,
        target_level: usize,
        max_select_bytes: u64,
        split_by_table: bool,
        overlap_strategy: Arc<dyn OverlapStrategy>,
    ) -> MinOverlappingPicker {
        MinOverlappingPicker {
            level,
            target_level,
            max_select_bytes,
            split_by_table,
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
                total_file_size += other.file_size;
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
                select_file_size += select_tables[*idx].file_size;
                if range.end > target_level_overlap_range.end {
                    for other in &target_tables[target_level_overlap_range.end..range.end] {
                        total_file_size += other.file_size;
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
            select_input_size: select_input_ssts.iter().map(|sst| sst.file_size).sum(),
            target_input_size: target_input_ssts.iter().map(|sst| sst.file_size).sum(),
            total_file_count: (select_input_ssts.len() + target_input_ssts.len()) as u64,
            input_levels: vec![
                InputLevel {
                    level_idx: self.level as u32,
                    level_type: LevelType::Nonoverlapping as i32,
                    table_infos: select_input_ssts,
                },
                InputLevel {
                    level_idx: self.target_level as u32,
                    level_type: LevelType::Nonoverlapping as i32,
                    table_infos: target_input_ssts,
                },
            ],
            target_level: self.target_level,
            ..Default::default()
        })
    }
}

#[derive(Default)]
pub struct SubLevelSstables {
    pub total_file_size: u64,
    pub total_file_count: usize,
    pub sstable_infos: Vec<Vec<SstableInfo>>,
}

pub struct NonOverlapSubLevelPicker {
    min_compaction_bytes: u64,
    max_compaction_bytes: u64,
    min_depth: usize,
    max_file_count: u64,
    overlap_strategy: Arc<dyn OverlapStrategy>,
}

impl NonOverlapSubLevelPicker {
    pub fn new(
        min_compaction_bytes: u64,
        max_compaction_bytes: u64,
        min_depth: usize,
        max_file_count: u64,
        overlap_strategy: Arc<dyn OverlapStrategy>,
    ) -> Self {
        Self {
            min_compaction_bytes,
            max_compaction_bytes,
            min_depth,
            max_file_count,
            overlap_strategy,
        }
    }

    fn pick_sub_level(
        &self,
        levels: &[Level],
        level_handler: &LevelHandler,
        sst_index: usize,
        sst: &SstableInfo,
    ) -> SubLevelSstables {
        let mut ret = SubLevelSstables {
            total_file_count: 1,
            total_file_size: sst.file_size,
            sstable_infos: vec![vec![]; levels.len()],
        };
        ret.sstable_infos[0].extend(vec![sst.clone()]);
        let mut overlap_info = self.overlap_strategy.create_overlap_info();
        let mut select_sst_id_set = BTreeSet::default();
        #[allow(clippy::single_range_in_vec_init)]
        let mut overlap_len_and_begins = vec![(sst_index..(sst_index + 1))];
        for sst in &ret.sstable_infos[0] {
            overlap_info.update(sst);
            select_sst_id_set.insert(sst.sst_id);
        }

        for (target_index, target_level) in levels.iter().enumerate().skip(1) {
            if target_level.level_type() != LevelType::Nonoverlapping {
                break;
            }

            // more than 1 sub_level
            if ret.total_file_count > 1 && ret.total_file_size >= self.max_compaction_bytes
                || ret.total_file_count >= self.max_file_count as usize
            {
                break;
            }

            let mut overlap_files_range =
                overlap_info.check_multiple_include(&target_level.table_infos);
            if overlap_files_range.is_empty() {
                overlap_files_range =
                    overlap_info.check_multiple_overlap(&target_level.table_infos);
            }
            // We allow a layer in the middle without overlap, so we need to continue to
            // the next layer to search for overlap
            let mut pending_compact = false;
            let mut current_level_size = 0;
            for index in overlap_files_range.start..overlap_files_range.end {
                let other = &target_level.table_infos[index];
                if level_handler.is_pending_compact(&other.sst_id) {
                    pending_compact = true;
                    break;
                }
                overlap_info.update(other);
                select_sst_id_set.insert(other.sst_id);
                current_level_size += other.file_size;
            }

            if pending_compact {
                break;
            }

            let mut extra_overlap_levels = vec![];

            let mut add_files_size = 0;
            // check reverse overlap
            for (reverse_index, old_overlap_range) in
                overlap_len_and_begins.iter_mut().enumerate().rev()
            {
                let target_tables = &levels[reverse_index].table_infos;
                // It has select all files in this sub-level, so it can not overlap with more files.
                if ret.sstable_infos[reverse_index].len() == target_tables.len() {
                    continue;
                }
                let new_overlap_range = overlap_info.check_multiple_overlap(target_tables);
                let mut extra_overlap_sst = Vec::with_capacity(new_overlap_range.len());
                for new_overlap_index in new_overlap_range.clone() {
                    if old_overlap_range.contains(&new_overlap_index) {
                        // Since some of the files have already been selected when selecting
                        // upwards, we filter here to avoid adding sst repeatedly
                        continue;
                    }

                    let other = &target_tables[new_overlap_index];
                    if level_handler.is_pending_compact(&other.sst_id) {
                        pending_compact = true;
                        break;
                    }
                    debug_assert!(!select_sst_id_set.contains(&other.sst_id));
                    add_files_size += other.file_size;
                    overlap_info.update(other);
                    select_sst_id_set.insert(other.sst_id);
                    extra_overlap_sst.push(other.clone());
                }

                if pending_compact {
                    break;
                }

                extra_overlap_levels.push((reverse_index, extra_overlap_sst));
                *old_overlap_range = new_overlap_range;
            }

            // check reverse overlap
            if pending_compact {
                // encountering a pending file means we don't need to continue processing this
                // interval
                break;
            }

            let add_files_count = overlap_files_range.len()
                + extra_overlap_levels
                    .iter()
                    .map(|(_, files)| files.len())
                    .sum::<usize>();

            if ret
                .sstable_infos
                .iter()
                .filter(|ssts| !ssts.is_empty())
                .count()
                > MAX_COMPACT_LEVEL_COUNT
            {
                break;
            }

            ret.total_file_count += add_files_count;
            ret.total_file_size += add_files_size + current_level_size;
            if !overlap_files_range.is_empty() {
                ret.sstable_infos[target_index]
                    .extend_from_slice(&target_level.table_infos[overlap_files_range.clone()]);
            }
            overlap_len_and_begins.push(overlap_files_range);
            for (reverse_index, files) in extra_overlap_levels {
                ret.sstable_infos[reverse_index].extend(files);
            }
        }

        ret.sstable_infos.retain(|ssts| !ssts.is_empty());
        // sort sst per level due to reverse expand
        ret.sstable_infos.iter_mut().for_each(|level_ssts| {
            level_ssts.sort_by(|sst1, sst2| {
                let a = sst1.key_range.as_ref().unwrap();
                let b = sst2.key_range.as_ref().unwrap();
                a.compare(b)
            });
        });
        ret
    }

    pub fn pick_l0_multi_non_overlap_level(
        &self,
        l0: &[Level],
        level_handler: &LevelHandler,
    ) -> Vec<SubLevelSstables> {
        if l0.len() < self.min_depth {
            return vec![];
        }

        let mut scores = vec![];
        let select_tables = &l0[0].table_infos;
        for (sst_index, sst) in select_tables.iter().enumerate() {
            if level_handler.is_pending_compact(&sst.sst_id) {
                continue;
            }

            let ret = self.pick_sub_level(l0, level_handler, sst_index, sst);
            if ret.sstable_infos.len() < self.min_depth
                && ret.total_file_size < self.min_compaction_bytes
            {
                continue;
            }
            scores.push(ret);
        }

        if scores.is_empty() {
            return vec![];
        }

        // The logic of sorting depends on the interval we expect to select.
        // 1. contain as many levels as possible
        // 2. fewer files in the bottom sub level, containing as many smaller intervals as possible.
        scores.sort_by(|a, b| {
            b.sstable_infos
                .len()
                .cmp(&a.sstable_infos.len())
                .then_with(|| a.total_file_count.cmp(&b.total_file_count))
                .then_with(|| a.total_file_size.cmp(&b.total_file_size))
        });
        scores
    }
}

#[cfg(test)]
pub mod tests {
    pub use risingwave_pb::hummock::{KeyRange, Level, LevelType};

    use super::*;
    use crate::hummock::compaction::overlap_strategy::RangeOverlapStrategy;
    use crate::hummock::compaction::selector::tests::{
        generate_l0_nonoverlapping_sublevels, generate_table,
    };

    #[test]
    fn test_compact_l1() {
        let mut picker = MinOverlappingPicker::new(
            1,
            2,
            10000,
            false,
            Arc::new(RangeOverlapStrategy::default()),
        );
        let levels = vec![
            Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(0, 1, 0, 100, 1),
                    generate_table(1, 1, 101, 200, 1),
                    generate_table(2, 1, 222, 300, 1),
                ],

                total_file_size: 0,
                sub_level_id: 0,
                uncompressed_file_size: 0,
            },
            Level {
                level_idx: 2,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(4, 1, 0, 100, 1),
                    generate_table(5, 1, 101, 150, 1),
                    generate_table(6, 1, 151, 201, 1),
                    generate_table(7, 1, 501, 800, 1),
                    generate_table(8, 2, 301, 400, 1),
                ],
                total_file_size: 0,
                sub_level_id: 0,
                uncompressed_file_size: 0,
            },
        ];
        let levels = Levels {
            levels,
            l0: Some(generate_l0_nonoverlapping_sublevels(vec![])),
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
        assert_eq!(ret.input_levels[0].table_infos[0].get_sst_id(), 2);
        assert_eq!(ret.input_levels[1].table_infos.len(), 0);
        ret.add_pending_task(0, &mut level_handlers);

        let ret = picker
            .pick_compaction(&levels, &level_handlers, &mut local_stats)
            .unwrap();
        assert_eq!(ret.input_levels[0].level_idx, 1);
        assert_eq!(ret.target_level, 2);
        assert_eq!(ret.input_levels[0].table_infos.len(), 1);
        assert_eq!(ret.input_levels[0].table_infos[0].get_sst_id(), 0);
        assert_eq!(ret.input_levels[1].table_infos.len(), 1);
        assert_eq!(ret.input_levels[1].table_infos[0].get_sst_id(), 4);
        ret.add_pending_task(1, &mut level_handlers);

        let ret = picker
            .pick_compaction(&levels, &level_handlers, &mut local_stats)
            .unwrap();
        assert_eq!(ret.input_levels[0].table_infos.len(), 1);
        assert_eq!(ret.input_levels[0].table_infos[0].get_sst_id(), 1);
        assert_eq!(ret.input_levels[1].table_infos.len(), 2);
        assert_eq!(ret.input_levels[1].table_infos[0].get_sst_id(), 5);
    }

    #[test]
    fn test_expand_l1_files() {
        let mut picker = MinOverlappingPicker::new(
            1,
            2,
            10000,
            false,
            Arc::new(RangeOverlapStrategy::default()),
        );
        let levels = vec![
            Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(0, 1, 50, 99, 2),
                    generate_table(1, 1, 100, 149, 2),
                    generate_table(2, 1, 150, 249, 2),
                ],
                total_file_size: 0,
                sub_level_id: 0,
                uncompressed_file_size: 0,
            },
            Level {
                level_idx: 2,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(4, 1, 50, 199, 1),
                    generate_table(5, 1, 200, 399, 1),
                ],
                total_file_size: 0,
                sub_level_id: 0,
                uncompressed_file_size: 0,
            },
        ];
        let levels = Levels {
            levels,
            l0: Some(generate_l0_nonoverlapping_sublevels(vec![])),
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
        assert_eq!(ret.input_levels[0].table_infos[0].get_sst_id(), 0);
        assert_eq!(ret.input_levels[0].table_infos[1].get_sst_id(), 1);

        assert_eq!(ret.input_levels[1].table_infos.len(), 1);
        assert_eq!(ret.input_levels[1].table_infos[0].get_sst_id(), 4);
    }

    #[test]
    fn test_pick_l0_multi_level() {
        let levels = vec![
            Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(0, 1, 50, 99, 2),
                    generate_table(1, 1, 100, 149, 2),
                    generate_table(2, 1, 150, 249, 2),
                    generate_table(6, 1, 250, 300, 2),
                    generate_table(7, 1, 350, 400, 2),
                    generate_table(8, 1, 450, 500, 2),
                ],
                total_file_size: 800,
                sub_level_id: 0,
                uncompressed_file_size: 0,
            },
            Level {
                level_idx: 2,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(4, 1, 50, 199, 1),
                    generate_table(5, 1, 200, 399, 1),
                    generate_table(9, 1, 250, 300, 2),
                    generate_table(10, 1, 350, 400, 2),
                    generate_table(11, 1, 450, 500, 2),
                ],
                total_file_size: 250,
                sub_level_id: 0,
                uncompressed_file_size: 0,
            },
            Level {
                level_idx: 3,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(11, 1, 250, 300, 2),
                    generate_table(12, 1, 350, 400, 2),
                    generate_table(13, 1, 450, 500, 2),
                ],
                total_file_size: 150,
                sub_level_id: 0,
                uncompressed_file_size: 0,
            },
            Level {
                level_idx: 4,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(14, 1, 250, 300, 2),
                    generate_table(15, 1, 350, 400, 2),
                    generate_table(16, 1, 450, 500, 2),
                ],
                total_file_size: 150,
                sub_level_id: 0,
                uncompressed_file_size: 0,
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
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(0, 1, 50, 99, 2),
                    generate_table(1, 1, 100, 149, 2),
                    generate_table(2, 1, 150, 249, 2),
                    generate_table(6, 1, 250, 300, 2),
                    generate_table(7, 1, 350, 400, 2),
                    generate_table(8, 1, 450, 500, 2),
                ],
                total_file_size: 800,
                sub_level_id: 0,
                uncompressed_file_size: 0,
            },
            Level {
                level_idx: 2,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(4, 1, 50, 99, 1),
                    generate_table(5, 1, 150, 200, 1),
                    generate_table(9, 1, 250, 300, 2),
                    generate_table(10, 1, 350, 400, 2),
                    generate_table(11, 1, 450, 500, 2),
                ],
                total_file_size: 250,
                sub_level_id: 0,
                uncompressed_file_size: 0,
            },
            Level {
                level_idx: 3,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(11, 1, 250, 300, 2),
                    generate_table(12, 1, 350, 400, 2),
                    generate_table(13, 1, 450, 500, 2),
                ],
                total_file_size: 150,
                sub_level_id: 0,
                uncompressed_file_size: 0,
            },
            Level {
                level_idx: 4,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(14, 1, 250, 300, 2),
                    generate_table(15, 1, 350, 400, 2),
                    generate_table(16, 1, 450, 500, 2),
                ],
                total_file_size: 150,
                sub_level_id: 0,
                uncompressed_file_size: 0,
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
            );
            let ret = picker.pick_l0_multi_non_overlap_level(&levels, &levels_handlers[0]);
            assert_eq!(6, ret.len());
        }

        {
            // limit max bytes
            let max_compaction_bytes = 100;
            let picker = NonOverlapSubLevelPicker::new(
                0,
                max_compaction_bytes,
                1,
                10000,
                Arc::new(RangeOverlapStrategy::default()),
            );
            let ret = picker.pick_l0_multi_non_overlap_level(&levels, &levels_handlers[0]);
            assert_eq!(6, ret.len());
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
            );
            let ret = picker.pick_l0_multi_non_overlap_level(&levels, &levels_handlers[0]);
            assert_eq!(6, ret.len());

            for plan in ret {
                let mut sst_id_set = BTreeSet::default();
                for sst in &plan.sstable_infos {
                    sst_id_set.insert(sst[0].get_sst_id());
                }
                assert!(sst_id_set.len() <= max_file_count as usize);
            }
        }

        {
            // limit min_depth
            let min_depth = 3;
            let picker = NonOverlapSubLevelPicker::new(
                1000,
                10000,
                min_depth,
                10000,
                Arc::new(RangeOverlapStrategy::default()),
            );
            let ret = picker.pick_l0_multi_non_overlap_level(&levels, &levels_handlers[0]);
            assert_eq!(3, ret.len());

            for plan in ret {
                let mut sst_id_set = BTreeSet::default();
                for sst in &plan.sstable_infos {
                    sst_id_set.insert(sst[0].get_sst_id());
                }
                assert!(plan.sstable_infos.len() >= min_depth);
            }
        }
    }

    #[test]
    fn test_trivial_move_bug() {
        let levels = vec![
            Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![generate_table(0, 1, 400, 500, 2)],
                total_file_size: 100,
                sub_level_id: 0,
                uncompressed_file_size: 0,
            },
            Level {
                level_idx: 2,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(1, 1, 100, 200, 1),
                    generate_table(2, 1, 600, 700, 1),
                ],
                total_file_size: 200,
                sub_level_id: 0,
                uncompressed_file_size: 0,
            },
            Level {
                level_idx: 3,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(3, 1, 100, 300, 2),
                    generate_table(4, 1, 600, 800, 1),
                ],
                total_file_size: 400,
                sub_level_id: 0,
                uncompressed_file_size: 0,
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
            MinOverlappingPicker::new(2, 3, 1000, false, Arc::new(RangeOverlapStrategy::default()));
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
}

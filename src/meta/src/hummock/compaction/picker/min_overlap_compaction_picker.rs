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

use itertools::Itertools;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockLevelsExt;
use risingwave_hummock_sdk::prost_key_range::KeyRangeExt;
use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::{InputLevel, Level, LevelType, SstableInfo};

use crate::hummock::compaction::overlap_strategy::OverlapStrategy;
use crate::hummock::compaction::{CompactionInput, CompactionPicker, LocalPickerStatistic};
use crate::hummock::level_handler::LevelHandler;

pub struct MinOverlappingPicker {
    max_select_bytes: u64,
    level: usize,
    target_level: usize,
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
            max_select_bytes,
            level,
            target_level,
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
        let mut scores = vec![];
        for left in 0..select_tables.len() {
            if level_handlers[self.level].is_pending_compact(&select_tables[left].sst_id) {
                continue;
            }
            let mut overlap_info = self.overlap_strategy.create_overlap_info();
            let mut select_file_size = 0;
            for (right, table) in select_tables.iter().enumerate().skip(left) {
                if level_handlers[self.level].is_pending_compact(&table.sst_id) {
                    break;
                }
                if self.split_by_table && table.table_ids != select_tables[left].table_ids {
                    break;
                }
                if select_file_size > self.max_select_bytes {
                    break;
                }
                select_file_size += table.file_size;
                overlap_info.update(table);
                let overlap_files = overlap_info.check_multiple_overlap(target_tables);
                let mut total_file_size = 0;
                let mut pending_compact = false;
                for other in overlap_files {
                    if level_handlers[self.target_level].is_pending_compact(&other.sst_id) {
                        pending_compact = true;
                        break;
                    }
                    total_file_size += other.file_size;
                }
                if pending_compact {
                    break;
                }
                scores.push((total_file_size * 100 / select_file_size, (left, right)));
            }
        }
        if scores.is_empty() {
            return (vec![], vec![]);
        }
        let (_, (left, right)) = scores
            .iter()
            .min_by(|(score1, x), (score2, y)| {
                score1
                    .cmp(score2)
                    .then_with(|| (y.1 - y.0).cmp(&(x.1 - x.0)))
            })
            .unwrap();
        let select_input_ssts = select_tables[*left..(right + 1)].to_vec();
        let target_input_ssts = self
            .overlap_strategy
            .check_base_level_overlap(&select_input_ssts, target_tables);
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
            target_sub_level_id: 0,
        })
    }
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

    pub fn pick_l0_multi_non_overlap_level(
        &self,
        l0: &[Level],
        level_handler: &LevelHandler,
    ) -> Vec<(u64, usize, Vec<Vec<SstableInfo>>)> {
        if l0.len() < self.min_depth {
            return vec![];
        }

        let mut scores = vec![];
        let select_tables = &l0[0].table_infos;
        for left in 0..select_tables.len() {
            if level_handler.is_pending_compact(&select_tables[left].sst_id) {
                continue;
            }

            let mut all_file_size = 0;
            let mut all_file_count = 0;
            for (right, table) in select_tables.iter().enumerate().skip(left) {
                if level_handler.is_pending_compact(&table.sst_id) {
                    break;
                }

                if all_file_size > self.max_compaction_bytes
                    || all_file_count > self.max_file_count as usize
                {
                    break;
                }

                let mut select_sst_id_set = BTreeSet::default();
                let mut level_select_files: Vec<Vec<SstableInfo>> = vec![vec![]; l0.len()];
                level_select_files[0].extend(select_tables[left..=right].to_vec());
                let mut overlap_info = self.overlap_strategy.create_overlap_info();
                for sst in &level_select_files[0] {
                    overlap_info.update(sst);
                    select_sst_id_set.insert(sst.sst_id);

                    all_file_size += sst.file_size;
                }

                all_file_count += right - left + 1;

                let mut select_level_count = 1;
                let mut last_level_index = 0;

                let mut pending_compact = false;
                for (target_index, target_level) in l0.iter().enumerate().skip(1) {
                    if target_level.level_type() != LevelType::Nonoverlapping {
                        break;
                    }

                    if all_file_size > self.max_compaction_bytes
                        || all_file_count > self.max_file_count as usize
                    {
                        break;
                    }

                    let target_tables = &target_level.table_infos;
                    let overlap_files = overlap_info.check_multiple_overlap(target_tables);

                    for other in &overlap_files {
                        if level_handler.is_pending_compact(&other.sst_id) {
                            pending_compact = true;
                            break;
                        }
                        overlap_info.update(other);
                        select_sst_id_set.insert(other.sst_id);

                        all_file_size += other.file_size;
                        all_file_count += 1;
                    }

                    if pending_compact {
                        break;
                    }

                    last_level_index = target_index;

                    if overlap_files.is_empty() {
                        // We allow a layer in the middle without overlap, so we need to continue to
                        // the next layer to search for overlap
                        continue;
                    }

                    level_select_files[target_index].extend(overlap_files.into_iter());
                    select_level_count += 1;
                }

                if pending_compact {
                    break;
                }

                // check reverse overlap
                for reverse_index in (0..last_level_index).rev() {
                    let target_tables = &l0[reverse_index].table_infos;
                    let overlap_files = overlap_info.check_multiple_overlap(target_tables);
                    let mut extra_overlap_sst = Vec::with_capacity(overlap_files.len());
                    for other in overlap_files {
                        if level_handler.is_pending_compact(&other.sst_id) {
                            pending_compact = true;
                            break;
                        }

                        if select_sst_id_set.contains(&other.sst_id) {
                            // Since some of the files have already been selected when selecting
                            // upwards, we filter here to avoid adding sst repeatedly
                            continue;
                        }

                        all_file_size += other.file_size;
                        all_file_count += 1;

                        overlap_info.update(&other);
                        select_sst_id_set.insert(other.sst_id);
                        extra_overlap_sst.push(other);
                    }

                    if pending_compact {
                        break;
                    }

                    if !extra_overlap_sst.is_empty() {
                        level_select_files[reverse_index].extend(extra_overlap_sst.into_iter());
                    }
                }

                if pending_compact {
                    // encountering a pending file means we don't need to continue processing this
                    // interval
                    break;
                }

                if all_file_size < self.min_compaction_bytes || select_level_count < self.min_depth
                {
                    // Avoiding too small compaction and number of layers
                    continue;
                }

                // sort sst per level due to reverse expand
                let level_select_files = level_select_files
                    .into_iter()
                    .filter(|level_ssts| !level_ssts.is_empty())
                    .map(|mut level_ssts| {
                        level_ssts.sort_by(|sst1, sst2| {
                            let a = sst1.key_range.as_ref().unwrap();
                            let b = sst2.key_range.as_ref().unwrap();
                            a.compare(b)
                        });

                        level_ssts
                    })
                    .collect_vec();

                if level_select_files.is_empty() {
                    // No files are selected in all layers
                    break;
                }

                let (select_level_size, overlap_level_size) = {
                    let select_level_size = level_select_files[0]
                        .iter()
                        .map(|sst| sst.file_size)
                        .sum::<u64>();

                    let overlap_level_size = all_file_size - select_level_size;
                    (select_level_size, overlap_level_size)
                };

                // The algorithm is not aware of the existence of partitions, which means that we
                // can cope with partition changes without additional processing. The reason for
                // computing write_amp in l0 is that we can assume that when partitions are aligned,
                // a file partitioned at the bottom sub_level will select the smallest possible file
                // write_map. When we select files that span two partitions, in the expected case,
                // the overlap range is larger and more files are selected at the top level,
                // resulting in a larger write_amp. So measuring an interval by write_amp allows us
                // to prefer interval-aligned files.
                let write_amp_delta =
                    (overlap_level_size as f64 * 100.0 / select_level_size as f64) as u64;

                scores.push((
                    (
                        select_level_count,
                        write_amp_delta,
                        level_select_files[0].len(),
                        all_file_size,
                        all_file_count,
                    ),
                    level_select_files,
                ));
            }
        }
        if scores.is_empty() {
            return vec![];
        }

        // The logic of sorting depends on the interval we expect to select.
        // 1. contain as many levels as possible
        // 2. fewer write amps, in other words more independent intervals to ensure parallelism
        // 3. fewer files in the bottom sub level, containing as many smaller intervals as possible.
        scores
            .into_iter()
            .sorted_by(
                |(
                    (
                        select_level_count,
                        write_amp_delta,
                        select_level_file_count,
                        _all_file_size,
                        _select_file_count,
                    ),
                    _x,
                ),
                 (
                    (
                        select_level_count2,
                        write_amp_delta2,
                        select_level_file_count2,
                        _all_file_size2,
                        _select_file_count2,
                    ),
                    _y,
                )| {
                    select_level_count2
                        .cmp(select_level_count)
                        .then_with(|| write_amp_delta.cmp(write_amp_delta2)) // a way to choose a small guard
                        .then_with(|| select_level_file_count.cmp(select_level_file_count2))
                },
            )
            .map(
                |(
                    (
                        _select_level_count,
                        _write_amp_delta,
                        _select_level_file_count,
                        all_file_size,
                        select_file_count,
                    ),
                    x,
                )| { (all_file_size, select_file_count, x) },
            )
            .collect_vec()
    }
}

#[cfg(test)]
pub mod tests {
    pub use risingwave_pb::hummock::{KeyRange, Level, LevelType};

    use super::*;
    use crate::hummock::compaction::level_selector::tests::{
        generate_l0_nonoverlapping_sublevels, generate_table,
    };
    use crate::hummock::compaction::overlap_strategy::RangeOverlapStrategy;

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
        assert_eq!(ret.input_levels[0].table_infos.len(), 2);
        assert_eq!(ret.input_levels[1].table_infos.len(), 3);
        assert_eq!(ret.input_levels[0].table_infos[0].get_sst_id(), 0);
        assert_eq!(ret.input_levels[1].table_infos[0].get_sst_id(), 4);
        ret.add_pending_task(1, &mut level_handlers);

        let ret = picker.pick_compaction(&levels, &level_handlers, &mut local_stats);
        assert!(ret.is_none());
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
            assert_eq!(21, ret.len());
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
            assert_eq!(9, ret.len());
        }
    }
}

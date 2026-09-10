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

use std::sync::Arc;

use risingwave_hummock_sdk::level::{InputLevel, Levels};
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
            overlap_info.update(&sst.key_range);
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
                    // Adjacent target indices are safe to combine; a gap would include SSTs
                    // that were not covered by the per-source pending checks above.
                    || range.start > target_level_overlap_range.end
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
                let score = (total_file_size * 100)
                    .checked_div(select_file_size)
                    .unwrap_or(total_file_size);
                end_idx = idx + 1;
                if score < min_score
                    || (score == min_score && select_file_size > min_score_select_file_size)
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

#[cfg(test)]
pub mod tests {
    use risingwave_hummock_sdk::compact_task::{CompactTask, CompactTaskAssignment};
    use risingwave_hummock_sdk::level::Level;

    use super::*;
    use crate::hummock::compaction::in_progress_compaction::InProgressCompactionView;
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
        assert_eq!(ret.input_levels[0].table_infos.len(), 2);
        assert_eq!(ret.input_levels[0].table_infos[0].sst_id, 0);
        assert_eq!(ret.input_levels[0].table_infos[1].sst_id, 1);
        assert_eq!(ret.input_levels[1].table_infos.len(), 3);
        assert_eq!(ret.input_levels[1].table_infos[0].sst_id, 4);
        assert_eq!(ret.input_levels[1].table_infos[1].sst_id, 5);
        assert_eq!(ret.input_levels[1].table_infos[2].sst_id, 6);
        ret.add_pending_task(1, &mut level_handlers);

        assert!(
            picker
                .pick_compaction(&levels, &level_handlers, &mut local_stats)
                .is_none()
        );
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
    fn test_batch_equal_ratio_adjacent_target_ranges() {
        let picker =
            MinOverlappingPicker::new(5, 6, 1000, 0, Arc::new(RangeOverlapStrategy::default()));
        // Key gaps are allowed when there are no extra target SSTs between the ranges.
        let source = vec![
            generate_table(0, 1, 0, 99, 2),
            generate_table(1, 1, 200, 299, 2),
            generate_table(2, 1, 400, 499, 2),
        ];
        let target = vec![
            generate_table(3, 1, 0, 99, 1),
            generate_table(4, 1, 200, 299, 1),
            generate_table(5, 1, 400, 499, 1),
        ];
        let handlers: Vec<_> = (0..=6).map(LevelHandler::new).collect();
        assert_eq!(
            picker.pick_tables(&source, &target, &handlers),
            (source, target)
        );
    }

    #[test]
    fn test_does_not_cross_uncovered_target_even_when_score_rounds_equal() {
        let picker =
            MinOverlappingPicker::new(5, 6, 1000, 0, Arc::new(RangeOverlapStrategy::default()));
        let source = vec![
            generate_table(0, 1, 0, 99, 2),
            generate_table(1, 1, 200, 299, 2),
        ];
        let target = vec![
            generate_table(2, 1, 0, 99, 1),
            generate_table(3, 1, 150, 150, 1),
            generate_table(4, 1, 200, 299, 1),
        ];
        // Including the gap SST would still round to score 100: 201 * 100 / 200.
        // Its pending state is not checked by either source SST's overlap range.
        for pending in [false, true] {
            let mut handlers: Vec<_> = (0..=7).map(LevelHandler::new).collect();
            if pending {
                handlers[6].add_pending_task(1, 7, [&target[1]]);
            }
            assert_eq!(
                picker.pick_tables(&source, &target, &handlers),
                (vec![source[0].clone()], vec![target[0].clone()])
            );
        }
    }

    #[test]
    fn test_does_not_cross_pending_source() {
        let picker =
            MinOverlappingPicker::new(5, 6, 1000, 0, Arc::new(RangeOverlapStrategy::default()));
        let source = vec![
            generate_table(0, 1, 0, 99, 2),
            generate_table(1, 1, 100, 199, 2),
            generate_table(2, 1, 200, 299, 2),
        ];
        let target = vec![generate_table(3, 1, 0, 299, 1)];
        let mut handlers: Vec<_> = (0..=6).map(LevelHandler::new).collect();
        handlers[5].add_pending_task(1, 6, [&source[1]]);
        assert_eq!(
            picker.pick_tables(&source, &target, &handlers),
            (vec![source[0].clone()], target)
        );
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
            overlap_info.update(&sst.key_range);
        }
        let range = overlap_info.check_multiple_overlap(&levels[0].table_infos);
        assert!(!range.is_empty());
        assert_eq!(select_files.len(), 2);
        assert_eq!(target_files.len(), 2);

        let in_progress = InProgressCompactionView::for_group(
            &[CompactTaskAssignment {
                compact_task: CompactTask {
                    task_id: 1,
                    compaction_group_id: 1.into(),
                    target_level: 3,
                    input_ssts: vec![
                        InputLevel {
                            level_idx: 2,
                            level_type: LevelType::Nonoverlapping,
                            table_infos: select_files,
                        },
                        InputLevel {
                            level_idx: 3,
                            level_type: LevelType::Nonoverlapping,
                            table_infos: target_files,
                        },
                    ],
                    ..Default::default()
                },
                context_id: 1.into(),
            }],
            1.into(),
        );
        // After moving from L1 into L2, the gap SST looks trivial-movable to L3 by
        // input IDs alone. The target guard must reject it while the wide task runs.
        let (gap_source, gap_target) = picker.pick_tables(
            &levels[0].table_infos,
            &levels[2].table_infos,
            &levels_handlers,
        );
        assert_eq!(gap_source.len(), 1);
        assert!(gap_target.is_empty());
        let gap_input = CompactionInput {
            target_level: 3,
            input_levels: vec![InputLevel {
                level_idx: 2,
                level_type: LevelType::Nonoverlapping,
                table_infos: gap_source,
            }],
            ..Default::default()
        };
        assert!(in_progress.has_conflict_with_input(&gap_input));
        assert!(!InProgressCompactionView::default().has_conflict_with_input(&gap_input));
    }
}

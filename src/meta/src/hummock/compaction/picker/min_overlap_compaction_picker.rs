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

use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockLevelsExt;
use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::{InputLevel, LevelType, SstableInfo};

use super::{CompactionInput, CompactionPicker, LocalPickerStatistic, MAX_COMPACT_LEVEL_COUNT};
use crate::hummock::compaction::overlap_strategy::OverlapStrategy;
use crate::hummock::compaction::SubLevelPartition;
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
                let overlap_files_range = overlap_info.check_multiple_overlap(target_tables);
                let mut total_file_size = 0;
                let mut pending_compact = false;
                if !overlap_files_range.is_empty() {
                    for other in &target_tables[overlap_files_range] {
                        if level_handlers[self.target_level].is_pending_compact(&other.sst_id) {
                            pending_compact = true;
                            break;
                        }
                        total_file_size += other.file_size;
                    }
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
                    .then_with(|| (x.1 - x.0).cmp(&(y.1 - y.0)))
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
        let select_level = levels.get_level(self.level);
        let (select_input_ssts, target_input_ssts) = self.pick_tables(
            &select_level.table_infos,
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
            vnode_partition_count: levels.vnode_partition_count,
            ..Default::default()
        })
    }
}

pub struct PartitionMinOverlappingPicker {
    inner: MinOverlappingPicker,
    partitions: Vec<SubLevelPartition>,
}

impl PartitionMinOverlappingPicker {
    pub fn new(
        level: usize,
        target_level: usize,
        max_select_bytes: u64,
        overlap_strategy: Arc<dyn OverlapStrategy>,
        partitions: Vec<SubLevelPartition>,
    ) -> Self {
        Self {
            inner: MinOverlappingPicker {
                level,
                target_level,
                max_select_bytes,
                split_by_table: false,
                overlap_strategy,
            },
            partitions,
        }
    }
}

impl CompactionPicker for PartitionMinOverlappingPicker {
    fn pick_compaction(
        &mut self,
        levels: &Levels,
        level_handlers: &[LevelHandler],
        stats: &mut LocalPickerStatistic,
    ) -> Option<CompactionInput> {
        assert!(self.inner.level > 0);
        let select_level = levels.get_level(self.inner.level);
        self.partitions.sort_by_key(|part| {
            part.sub_levels
                .iter()
                .map(|info| info.total_file_size)
                .sum::<u64>()
        });
        self.partitions.reverse();
        for part in &self.partitions {
            let info = &part.sub_levels[0];
            if info.right_idx > info.left_idx {
                let (select_input_ssts, target_input_ssts) = self.inner.pick_tables(
                    &select_level.table_infos[info.left_idx..info.right_idx],
                    &levels.get_level(self.inner.target_level).table_infos,
                    level_handlers,
                );
                if !select_input_ssts.is_empty() {
                    return Some(CompactionInput {
                        input_levels: vec![
                            InputLevel {
                                level_idx: self.inner.level as u32,
                                level_type: LevelType::Nonoverlapping as i32,
                                table_infos: select_input_ssts,
                            },
                            InputLevel {
                                level_idx: self.inner.target_level as u32,
                                level_type: LevelType::Nonoverlapping as i32,
                                table_infos: target_input_ssts,
                            },
                        ],
                        target_level: self.inner.target_level,
                        target_sub_level_id: 0,
                        vnode_partition_count: levels.vnode_partition_count,
                    });
                }
                stats.skip_by_pending_files += 1;
            }
        }
        None
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
                ..Default::default()
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
                ..Default::default()
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
        assert_eq!(ret.input_levels[1].table_infos.len(), 1);
        assert_eq!(ret.input_levels[0].table_infos[0].get_sst_id(), 0);
        assert_eq!(ret.input_levels[1].table_infos[0].get_sst_id(), 4);
        ret.add_pending_task(1, &mut level_handlers);

        let ret = picker
            .pick_compaction(&levels, &level_handlers, &mut local_stats)
            .unwrap();
        assert_eq!(ret.input_levels[0].table_infos.len(), 1);
        assert_eq!(ret.input_levels[1].table_infos.len(), 2);
        assert_eq!(ret.input_levels[0].table_infos[0].get_sst_id(), 1);
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
                ..Default::default()
            },
            Level {
                level_idx: 2,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(4, 1, 50, 199, 1),
                    generate_table(5, 1, 200, 399, 1),
                ],
                ..Default::default()
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
    fn test_trivial_move_bug() {
        let levels = vec![
            Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![generate_table(0, 1, 400, 500, 2)],
                total_file_size: 100,
                ..Default::default()
            },
            Level {
                level_idx: 2,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(1, 1, 100, 200, 1),
                    generate_table(2, 1, 600, 700, 1),
                ],
                total_file_size: 200,
                ..Default::default()
            },
            Level {
                level_idx: 3,
                level_type: LevelType::Nonoverlapping as i32,
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

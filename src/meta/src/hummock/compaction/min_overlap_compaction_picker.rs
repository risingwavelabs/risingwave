// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

use super::CompactionPicker;
use crate::hummock::compaction::overlap_strategy::OverlapStrategy;
use crate::hummock::compaction::CompactionInput;
use crate::hummock::level_handler::LevelHandler;

pub struct MinOverlappingPicker {
    max_select_bytes: u64,
    level: usize,
    target_level: usize,
    overlap_strategy: Arc<dyn OverlapStrategy>,
}

impl MinOverlappingPicker {
    pub fn new(
        level: usize,
        target_level: usize,
        max_select_bytes: u64,
        overlap_strategy: Arc<dyn OverlapStrategy>,
    ) -> MinOverlappingPicker {
        MinOverlappingPicker {
            max_select_bytes,
            level,
            target_level,
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
            if level_handlers[self.level].is_pending_compact(&select_tables[left].id) {
                continue;
            }
            let mut overlap_info = self.overlap_strategy.create_overlap_info();
            let mut select_file_size = 0;
            for (right, table) in select_tables.iter().enumerate().skip(left) {
                if level_handlers[self.level].is_pending_compact(&table.id) {
                    break;
                }
                if select_file_size > self.max_select_bytes {
                    break;
                }
                select_file_size += table.file_size;
                overlap_info.update(table);
                let overlap_files = overlap_info.check_multiple_overlap(target_tables);
                let mut total_file_size = 0;
                let mut pending_campct = false;
                for other in overlap_files {
                    if level_handlers[self.target_level].is_pending_compact(&other.id) {
                        pending_campct = true;
                        break;
                    }
                    total_file_size += other.file_size;
                }
                if pending_campct {
                    break;
                }
                scores.push((total_file_size * 100 / select_file_size, (left, right)));
            }
        }
        if scores.is_empty() {
            return (vec![], vec![]);
        }
        let (_, (left, right)) = scores.iter().min_by(|x, y| x.0.cmp(&y.0)).unwrap();
        let select_input_ssts = select_tables[*left..(right + 1)].to_vec();
        let target_input_ssts = self
            .overlap_strategy
            .check_base_level_overlap(&select_input_ssts, target_tables);
        (select_input_ssts, target_input_ssts)
    }
}

impl CompactionPicker for MinOverlappingPicker {
    fn pick_compaction(
        &self,
        levels: &Levels,
        level_handlers: &[LevelHandler],
    ) -> Option<CompactionInput> {
        assert!(self.level > 0);
        let (select_input_ssts, target_input_ssts) = self.pick_tables(
            &levels.get_level(self.level).table_infos,
            &levels.get_level(self.target_level).table_infos,
            level_handlers,
        );
        if select_input_ssts.is_empty() {
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
        let picker =
            MinOverlappingPicker::new(1, 2, 10000, Arc::new(RangeOverlapStrategy::default()));
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
            },
        ];
        let levels = Levels {
            levels,
            l0: Some(generate_l0_nonoverlapping_sublevels(vec![])),
        };
        let mut level_handlers = vec![
            LevelHandler::new(0),
            LevelHandler::new(1),
            LevelHandler::new(2),
        ];

        // pick a non-overlapping files. It means that this file could be trivial move to next
        // level.
        let ret = picker.pick_compaction(&levels, &level_handlers).unwrap();
        assert_eq!(ret.input_levels[0].level_idx, 1);
        assert_eq!(ret.target_level, 2);
        assert_eq!(ret.input_levels[0].table_infos.len(), 1);
        assert_eq!(ret.input_levels[0].table_infos[0].id, 2);
        assert_eq!(ret.input_levels[1].table_infos.len(), 0);
        ret.add_pending_task(0, &mut level_handlers);

        let ret = picker.pick_compaction(&levels, &level_handlers).unwrap();
        assert_eq!(ret.input_levels[0].level_idx, 1);
        assert_eq!(ret.target_level, 2);
        assert_eq!(ret.input_levels[0].table_infos.len(), 1);
        assert_eq!(ret.input_levels[1].table_infos.len(), 1);
        assert_eq!(ret.input_levels[0].table_infos[0].id, 0);
        assert_eq!(ret.input_levels[1].table_infos[0].id, 4);
        ret.add_pending_task(1, &mut level_handlers);

        let ret = picker.pick_compaction(&levels, &level_handlers).unwrap();
        assert_eq!(ret.input_levels[0].level_idx, 1);
        assert_eq!(ret.target_level, 2);
        assert_eq!(ret.input_levels[0].table_infos.len(), 1);
        assert_eq!(ret.input_levels[1].table_infos.len(), 2);
        assert_eq!(ret.input_levels[0].table_infos[0].id, 1);
        assert_eq!(ret.input_levels[1].table_infos[0].id, 5);
        assert_eq!(ret.input_levels[1].table_infos[1].id, 6);
    }

    #[test]
    fn test_expand_l1_files() {
        let picker =
            MinOverlappingPicker::new(1, 2, 10000, Arc::new(RangeOverlapStrategy::default()));
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
            },
        ];
        let levels = Levels {
            levels,
            l0: Some(generate_l0_nonoverlapping_sublevels(vec![])),
        };
        let levels_handler = vec![
            LevelHandler::new(0),
            LevelHandler::new(1),
            LevelHandler::new(2),
        ];

        // pick a non-overlapping files. It means that this file could be trivial move to next
        // level.
        let ret = picker.pick_compaction(&levels, &levels_handler).unwrap();
        assert_eq!(ret.input_levels[0].level_idx, 1);
        assert_eq!(ret.input_levels[1].level_idx, 2);

        assert_eq!(ret.input_levels[0].table_infos.len(), 2);
        assert_eq!(ret.input_levels[0].table_infos[0].id, 0);
        assert_eq!(ret.input_levels[0].table_infos[1].id, 1);

        assert_eq!(ret.input_levels[1].table_infos.len(), 1);
        assert_eq!(ret.input_levels[1].table_infos[0].id, 4);
    }
}

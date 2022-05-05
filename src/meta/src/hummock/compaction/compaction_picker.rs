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

use risingwave_pb::hummock::Level;

use crate::hummock::compaction::overlap_strategy::OverlapStrategy;
use crate::hummock::compaction::SearchResult;
use crate::hummock::level_handler::LevelHandler;

pub trait CompactionPicker {
    fn pick_compaction(
        &self,
        levels: &[Level],
        level_handlers: &mut [LevelHandler],
    ) -> Option<SearchResult>;
}

pub struct MinOverlappingPicker {
    compact_task_id: u64,
    overlap_strategy: Arc<dyn OverlapStrategy>,
    level: usize,
}

impl MinOverlappingPicker {
    pub fn new(
        compact_task_id: u64,
        level: usize,
        overlap_strategy: Arc<dyn OverlapStrategy>,
    ) -> MinOverlappingPicker {
        MinOverlappingPicker {
            compact_task_id,
            overlap_strategy,
            level,
        }
    }
}

impl CompactionPicker for MinOverlappingPicker {
    fn pick_compaction(
        &self,
        levels: &[Level],
        level_handlers: &mut [LevelHandler],
    ) -> Option<SearchResult> {
        let target_level = self.level + 1;
        let mut scores = vec![];
        for table in &levels[self.level].table_infos {
            if level_handlers[self.level].is_pending_compact(&table.id) {
                continue;
            }
            let mut total_file_size = 0;
            let mut pending_campct = false;
            let overlap_files = self
                .overlap_strategy
                .check_base_level_overlap(&[table.clone()], &levels[target_level].table_infos);
            for other in overlap_files {
                if level_handlers[target_level].is_pending_compact(&other.id) {
                    pending_campct = true;
                    break;
                }
                total_file_size += other.file_size;
            }
            if pending_campct {
                continue;
            }
            scores.push((total_file_size * 100 / (table.file_size + 1), table.clone()));
        }
        if scores.is_empty() {
            return None;
        }
        scores.sort_by_key(|x| x.0);
        let (_, table) = scores.first().unwrap();
        let select_input_ssts = vec![table.clone()];
        let target_input_ssts = self
            .overlap_strategy
            .check_base_level_overlap(&select_input_ssts, &levels[target_level].table_infos);
        level_handlers[self.level].add_pending_task(self.compact_task_id, &select_input_ssts);
        if !target_input_ssts.is_empty() {
            level_handlers[target_level].add_pending_task(self.compact_task_id, &target_input_ssts);
        }
        Some(SearchResult {
            select_level: Level {
                level_idx: self.level as u32,
                level_type: levels[self.level].level_type,
                table_infos: select_input_ssts,
            },
            target_level: Level {
                level_idx: target_level as u32,
                level_type: levels[target_level].level_type,
                table_infos: target_input_ssts,
            },
            split_ranges: vec![],
        })
    }
}

#[cfg(test)]
pub mod tests {
    use risingwave_pb::hummock::LevelType;

    use super::*;
    use crate::hummock::compaction::overlap_strategy::RangeOverlapStrategy;
    use crate::hummock::compaction::tier_compaction_picker::tests::generate_table;

    #[test]
    fn test_compact_l1() {
        let picker = MinOverlappingPicker::new(0, 1, Arc::new(RangeOverlapStrategy::default()));
        let levels = vec![
            Level {
                level_idx: 0,
                level_type: LevelType::Overlapping as i32,
                table_infos: vec![],
            },
            Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(0, 1, 0, 100, 1),
                    generate_table(1, 1, 101, 200, 1),
                    generate_table(2, 1, 222, 300, 1),
                ],
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
            },
        ];
        let mut levels_handler = vec![
            LevelHandler::new(0),
            LevelHandler::new(1),
            LevelHandler::new(2),
        ];

        // pick a non-overlapping files. It means that this file could be trival move to next level.
        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();
        assert_eq!(ret.select_level.level_idx, 1);
        assert_eq!(ret.target_level.level_idx, 2);
        assert_eq!(ret.select_level.table_infos.len(), 1);
        assert_eq!(ret.select_level.table_infos[0].id, 2);
        assert_eq!(ret.target_level.table_infos.len(), 0);

        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();
        assert_eq!(ret.select_level.level_idx, 1);
        assert_eq!(ret.target_level.level_idx, 2);
        assert_eq!(ret.select_level.table_infos.len(), 1);
        assert_eq!(ret.target_level.table_infos.len(), 1);
        assert_eq!(ret.select_level.table_infos[0].id, 0);
        assert_eq!(ret.target_level.table_infos[0].id, 4);

        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();
        assert_eq!(ret.select_level.level_idx, 1);
        assert_eq!(ret.target_level.level_idx, 2);
        assert_eq!(ret.select_level.table_infos.len(), 1);
        assert_eq!(ret.target_level.table_infos.len(), 2);
        assert_eq!(ret.select_level.table_infos[0].id, 1);
        assert_eq!(ret.target_level.table_infos[0].id, 5);
        assert_eq!(ret.target_level.table_infos[1].id, 6);
    }
}

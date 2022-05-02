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

pub struct SizeOverlapPicker {
    compact_task_id: u64,
    overlap_strategy: Arc<dyn OverlapStrategy>,
    level: usize,
}

impl SizeOverlapPicker {
    pub fn new(
        compact_task_id: u64,
        level: usize,
        overlap_strategy: Arc<dyn OverlapStrategy>,
    ) -> SizeOverlapPicker {
        SizeOverlapPicker {
            compact_task_id,
            overlap_strategy,
            level,
        }
    }
}

impl CompactionPicker for SizeOverlapPicker {
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
                .check_multiple_overlap(&[table.clone()], &levels[target_level].table_infos);
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
            scores.push((total_file_size, table.clone()));
        }
        if scores.is_empty() {
            return None;
        }
        scores.sort_by_key(|x| x.0);
        let (_, table) = scores.pop().unwrap();
        let select_input_ssts = vec![table];
        let target_input_ssts = self
            .overlap_strategy
            .check_multiple_overlap(&select_input_ssts, &levels[target_level].table_infos);
        level_handlers[target_level].add_pending_task(self.compact_task_id, &target_input_ssts);
        level_handlers[self.level].add_pending_task(self.compact_task_id, &select_input_ssts);
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

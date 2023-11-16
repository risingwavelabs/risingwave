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

use risingwave_pb::hummock::{InputLevel, LevelType, SstableInfo};

use super::{CompactionInput, LocalPickerStatistic};
use crate::hummock::compaction::overlap_strategy::OverlapStrategy;
use crate::hummock::level_handler::LevelHandler;

pub struct TrivialMovePicker {
    level: usize,
    target_level: usize,
    overlap_strategy: Arc<dyn OverlapStrategy>,
}

impl TrivialMovePicker {
    pub fn new(
        level: usize,
        target_level: usize,
        overlap_strategy: Arc<dyn OverlapStrategy>,
    ) -> Self {
        Self {
            level,
            target_level,
            overlap_strategy,
        }
    }

    pub fn pick_trivial_move_sst(
        &self,
        select_tables: &[SstableInfo],
        target_tables: &[SstableInfo],
        level_handlers: &[LevelHandler],
        stats: &mut LocalPickerStatistic,
    ) -> Option<SstableInfo> {
        let mut skip_by_pending = false;
        for sst in select_tables {
            if level_handlers[self.level].is_pending_compact(&sst.sst_id) {
                skip_by_pending = true;
                continue;
            }
            let mut overlap_info = self.overlap_strategy.create_overlap_info();
            overlap_info.update(sst);
            let overlap_files_range = overlap_info.check_multiple_overlap(target_tables);

            if overlap_files_range.is_empty() {
                return Some(sst.clone());
            }
        }

        if skip_by_pending {
            stats.skip_by_pending_files += 1;
        }

        None
    }

    pub fn pick_trivial_move_task(
        &self,
        select_tables: &[SstableInfo],
        target_tables: &[SstableInfo],
        level_handlers: &[LevelHandler],
        stats: &mut LocalPickerStatistic,
    ) -> Option<CompactionInput> {
        if let Some(trivial_move_sst) =
            self.pick_trivial_move_sst(select_tables, target_tables, level_handlers, stats)
        {
            return Some(CompactionInput {
                select_input_size: trivial_move_sst.file_size,
                total_file_count: 1,
                input_levels: vec![
                    InputLevel {
                        level_idx: self.level as u32,
                        level_type: LevelType::Nonoverlapping as i32,
                        table_infos: vec![trivial_move_sst],
                    },
                    InputLevel {
                        level_idx: self.target_level as u32,
                        level_type: LevelType::Nonoverlapping as i32,
                        table_infos: vec![],
                    },
                ],
                target_level: self.target_level,
                ..Default::default()
            });
        }

        None
    }
}

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

use std::sync::Arc;

use risingwave_hummock_sdk::level::InputLevel;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_pb::hummock::LevelType;

use super::{CompactionInput, LocalPickerStatistic};
use crate::hummock::compaction::overlap_strategy::OverlapStrategy;
use crate::hummock::level_handler::LevelHandler;

pub struct TrivialMovePicker {
    level: usize,
    target_level: usize,
    overlap_strategy: Arc<dyn OverlapStrategy>,

    // The minimum size of sst that can be selected as trivial move.
    sst_allowed_trivial_move_min_size: u64,
}

impl TrivialMovePicker {
    pub fn new(
        level: usize,
        target_level: usize,
        overlap_strategy: Arc<dyn OverlapStrategy>,
        sst_allowed_trivial_move_min_size: u64,
    ) -> Self {
        Self {
            level,
            target_level,
            overlap_strategy,
            sst_allowed_trivial_move_min_size,
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
            if sst.sst_size < self.sst_allowed_trivial_move_min_size {
                continue;
            }

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
                select_input_size: trivial_move_sst.sst_size,
                total_file_count: 1,
                input_levels: vec![
                    InputLevel {
                        level_idx: self.level as u32,
                        level_type: LevelType::Nonoverlapping,
                        table_infos: vec![trivial_move_sst],
                    },
                    InputLevel {
                        level_idx: self.target_level as u32,
                        level_type: LevelType::Nonoverlapping,
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

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;

    use risingwave_hummock_sdk::sstable_info::{SstableInfo, SstableInfoInner};

    use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
    use crate::hummock::compaction::create_overlap_strategy;
    use crate::hummock::level_handler::LevelHandler;

    #[test]
    fn test_allowed_trivial_move_min_size() {
        let sst: SstableInfo = SstableInfoInner {
            sst_id: 1,
            file_size: 100,
            sst_size: 100,
            ..Default::default()
        }
        .into();

        let config = Arc::new(
            CompactionConfigBuilder::new()
                .level0_tier_compact_file_number(2)
                .level0_sub_level_compact_level_count(1)
                .build(),
        );
        let levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];
        let overlap_strategy = create_overlap_strategy(config.compaction_mode());
        {
            let trivial_move_picker =
                super::TrivialMovePicker::new(0, 1, overlap_strategy.clone(), 200);
            let trivial_move_task = trivial_move_picker.pick_trivial_move_task(
                &[sst.clone()],
                &[],
                &levels_handler,
                &mut Default::default(),
            );

            assert!(trivial_move_task.is_none());
        }

        {
            let trivial_move_picker =
                super::TrivialMovePicker::new(0, 1, overlap_strategy.clone(), 50);

            let trivial_move_task = trivial_move_picker.pick_trivial_move_task(
                &[sst.clone()],
                &[],
                &levels_handler,
                &mut Default::default(),
            );

            assert!(trivial_move_task.is_some());
        }
    }
}

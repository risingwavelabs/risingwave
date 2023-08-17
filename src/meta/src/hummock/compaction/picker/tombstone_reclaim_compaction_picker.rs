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

use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::InputLevel;

use crate::hummock::compaction::overlap_strategy::OverlapStrategy;
use crate::hummock::compaction::picker::CompactionInput;
use crate::hummock::level_handler::LevelHandler;

pub struct TombstoneReclaimCompactionPicker {
    overlap_strategy: Arc<dyn OverlapStrategy>,
    max_compaction_bytes: u64,
    delete_ratio_percent: u64,
    range_delete_ratio_percent: u64,
}

#[derive(Default)]
pub struct TombstoneReclaimPickerState {
    pub last_level: usize,
}

impl TombstoneReclaimCompactionPicker {
    pub fn new(
        overlap_strategy: Arc<dyn OverlapStrategy>,
        max_compaction_bytes: u64,
        delete_ratio_percent: u64,
        range_delete_ratio_percent: u64,
    ) -> Self {
        Self {
            overlap_strategy,
            max_compaction_bytes,
            delete_ratio_percent,
            range_delete_ratio_percent,
        }
    }

    pub fn pick_compaction(
        &self,
        levels: &Levels,
        level_handlers: &[LevelHandler],
        state: &mut TombstoneReclaimPickerState,
    ) -> Option<CompactionInput> {
        assert!(!levels.levels.is_empty());
        let mut select_input_ssts = vec![];

        while state.last_level <= levels.levels.len() {
            let mut select_file_size = 0;
            for sst in &levels.levels[state.last_level - 1].table_infos {
                let need_reclaim = sst.range_tombstone_count * self.range_delete_ratio_percent
                    / 100
                    > sst.total_key_count
                    || sst.stale_key_count * self.delete_ratio_percent / 100 > sst.total_key_count;
                if !need_reclaim || level_handlers[state.last_level].is_pending_compact(&sst.sst_id)
                {
                    if !select_input_ssts.is_empty() {
                        // Our goal is to pick as many complete layers of data as possible and keep
                        // the picked files contiguous to avoid overlapping
                        // key_ranges, so the strategy is to pick as many
                        // contiguous files as possible (at least one)
                        break;
                    }
                    continue;
                }

                select_input_ssts.push(sst.clone());
                select_file_size += sst.file_size;
                if select_file_size > self.max_compaction_bytes {
                    break;
                }
            }

            // turn to next_round
            if !select_input_ssts.is_empty() {
                let target_level = if state.last_level
                    == levels.levels.last().unwrap().level_idx as usize
                {
                    InputLevel {
                        level_idx: state.last_level as u32,
                        level_type: levels.levels[state.last_level - 1].level_type,
                        table_infos: vec![],
                    }
                } else {
                    let target_table_infos = self.overlap_strategy.check_base_level_overlap(
                        &select_input_ssts,
                        &levels.levels[state.last_level].table_infos,
                    );
                    let mut pending_compact = false;
                    for sst in &target_table_infos {
                        if level_handlers[state.last_level + 1].is_pending_compact(&sst.sst_id) {
                            pending_compact = true;
                            break;
                        }
                    }
                    if pending_compact {
                        continue;
                    }
                    InputLevel {
                        level_idx: (state.last_level + 1) as u32,
                        level_type: levels.levels[state.last_level].level_type,
                        table_infos: target_table_infos,
                    }
                };
                return Some(CompactionInput {
                    target_level: target_level.level_idx as usize,
                    input_levels: vec![
                        InputLevel {
                            level_idx: state.last_level as u32,
                            level_type: levels.levels[state.last_level - 1].level_type,
                            table_infos: select_input_ssts,
                        },
                        target_level,
                    ],
                    target_sub_level_id: 0,
                });
            }
            state.last_level += 1;
        }
        state.last_level = 0;
        None
    }
}

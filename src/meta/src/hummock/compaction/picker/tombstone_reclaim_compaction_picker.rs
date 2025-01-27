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

use risingwave_hummock_sdk::level::{InputLevel, Levels};

use crate::hummock::compaction::overlap_strategy::OverlapStrategy;
use crate::hummock::compaction::picker::CompactionInput;
use crate::hummock::level_handler::LevelHandler;

pub struct TombstoneReclaimCompactionPicker {
    overlap_strategy: Arc<dyn OverlapStrategy>,
    delete_ratio: u64,
    range_delete_ratio: u64,
}

#[derive(Default)]
pub struct TombstoneReclaimPickerState {
    pub last_level: usize,
}

impl TombstoneReclaimCompactionPicker {
    pub fn new(
        overlap_strategy: Arc<dyn OverlapStrategy>,
        delete_ratio: u64,
        range_delete_ratio: u64,
    ) -> Self {
        Self {
            overlap_strategy,
            delete_ratio,
            range_delete_ratio,
        }
    }

    pub fn pick_compaction(
        &self,
        levels: &Levels,
        level_handlers: &[LevelHandler],
        state: &mut TombstoneReclaimPickerState,
    ) -> Option<CompactionInput> {
        assert!(!levels.levels.is_empty());
        if state.last_level == 0 {
            state.last_level = 1;
        }

        while state.last_level <= levels.levels.len() {
            let mut select_input_ssts = vec![];
            for sst in &levels.levels[state.last_level - 1].table_infos {
                let need_reclaim = (sst.range_tombstone_count * 100
                    >= sst.total_key_count * self.range_delete_ratio)
                    || (sst.stale_key_count * 100 >= sst.total_key_count * self.delete_ratio);
                if !need_reclaim || level_handlers[state.last_level].is_pending_compact(&sst.sst_id)
                {
                    continue;
                }

                select_input_ssts.push(sst.clone());
                break;
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
                        state.last_level += 1;
                        continue;
                    }
                    InputLevel {
                        level_idx: (state.last_level + 1) as u32,
                        level_type: levels.levels[state.last_level].level_type,
                        table_infos: target_table_infos,
                    }
                };
                return Some(CompactionInput {
                    select_input_size: select_input_ssts.iter().map(|sst| sst.sst_size).sum(),
                    target_input_size: target_level
                        .table_infos
                        .iter()
                        .map(|sst| sst.sst_size)
                        .sum(),
                    total_file_count: (select_input_ssts.len() + target_level.table_infos.len())
                        as u64,
                    target_level: target_level.level_idx as usize,
                    input_levels: vec![
                        InputLevel {
                            level_idx: state.last_level as u32,
                            level_type: levels.levels[state.last_level - 1].level_type,
                            table_infos: select_input_ssts,
                        },
                        target_level,
                    ],
                    ..Default::default()
                });
            }
            state.last_level += 1;
        }
        state.last_level = 0;
        None
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
    use crate::hummock::compaction::create_overlap_strategy;
    use crate::hummock::compaction::selector::tests::{
        generate_level, generate_table, generate_table_impl,
    };

    #[test]
    fn test_basic() {
        let mut levels = Levels {
            levels: vec![
                generate_level(1, vec![]),
                generate_level(
                    2,
                    vec![
                        generate_table(1, 1, 1, 100, 1),
                        generate_table(2, 1, 101, 200, 1),
                    ],
                ),
            ],
            ..Default::default()
        };
        let levels_handler = vec![
            LevelHandler::new(0),
            LevelHandler::new(1),
            LevelHandler::new(2),
        ];
        let mut state = TombstoneReclaimPickerState::default();

        let config = Arc::new(CompactionConfigBuilder::new().build());

        let strategy = create_overlap_strategy(config.compaction_mode());
        let picker = TombstoneReclaimCompactionPicker::new(strategy.clone(), 40, 20);
        let ret = picker.pick_compaction(&levels, &levels_handler, &mut state);
        assert!(ret.is_none());
        let mut sst = generate_table_impl(3, 1, 201, 300, 1);
        sst.stale_key_count = 40;
        sst.total_key_count = 100;
        levels.levels[1].table_infos.push(sst.into());

        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut state)
            .unwrap();
        assert_eq!(2, ret.input_levels.len());
        assert_eq!(3, ret.input_levels[0].table_infos[0].sst_id);
        let mut sst = generate_table_impl(4, 1, 1, 100, 1);
        sst.stale_key_count = 30;
        sst.range_tombstone_count = 30;
        sst.total_key_count = 100;
        levels.levels[0].table_infos.push(sst.into());
        let picker = TombstoneReclaimCompactionPicker::new(strategy, 50, 10);
        let mut state = TombstoneReclaimPickerState::default();
        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut state)
            .unwrap();
        assert_eq!(2, ret.input_levels.len());
        assert_eq!(4, ret.input_levels[0].table_infos[0].sst_id);
        assert_eq!(1, ret.input_levels[1].table_infos[0].sst_id);
    }
}

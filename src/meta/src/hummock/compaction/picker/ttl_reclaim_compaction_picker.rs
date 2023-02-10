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

use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::InputLevel;

use crate::hummock::compaction::CompactionInput;
use crate::hummock::level_handler::LevelHandler;

#[derive(Default)]
pub struct TtlPickerState {
    last_select_index: usize,
}

pub struct TtlReclaimCompactionPicker {
    max_ttl_reclaim_bytes: u64,
    // todo: filter table option
}

impl TtlReclaimCompactionPicker {
    pub fn new(max_ttl_reclaim_bytes: u64) -> Self {
        Self {
            max_ttl_reclaim_bytes,
        }
    }
}

impl TtlReclaimCompactionPicker {
    pub fn pick_compaction(
        &self,
        levels: &Levels,
        level_handlers: &[LevelHandler],
        state: &mut TtlPickerState,
    ) -> Option<CompactionInput> {
        assert!(!levels.levels.is_empty());
        let reclaimed_level = levels.levels.last().unwrap();
        let mut select_input_ssts = vec![];
        let level_handler = &level_handlers[reclaimed_level.level_idx as usize];

        if state.last_select_index >= reclaimed_level.table_infos.len() {
            state.last_select_index = 0;
        }

        let start_indedx = state.last_select_index;
        let mut select_file_size = 0;

        for sst in &reclaimed_level.table_infos[start_indedx..] {
            state.last_select_index += 1;
            if level_handler.is_pending_compact(&sst.id) {
                continue;
            }

            select_input_ssts.push(sst.clone());
            select_file_size += sst.file_size;
            if select_file_size > self.max_ttl_reclaim_bytes {
                break;
            }
        }

        if select_input_ssts.is_empty() {
            return None;
        }

        Some(CompactionInput {
            input_levels: vec![
                InputLevel {
                    level_idx: reclaimed_level.level_idx,
                    level_type: reclaimed_level.level_type,
                    table_infos: select_input_ssts,
                },
                InputLevel {
                    level_idx: reclaimed_level.level_idx,
                    level_type: reclaimed_level.level_type,
                    table_infos: vec![],
                },
            ],
            target_level: reclaimed_level.level_idx as usize,
            target_sub_level_id: 0,
        })
    }
}

#[cfg(test)]
mod test {

    use std::sync::Arc;

    use itertools::Itertools;
    use risingwave_pb::hummock::compact_task;
    pub use risingwave_pb::hummock::{KeyRange, Level, LevelType};

    use super::*;
    use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
    use crate::hummock::compaction::level_selector::tests::{
        assert_compaction_task, generate_l0_nonoverlapping_sublevels, generate_level,
        generate_table,
    };
    use crate::hummock::compaction::level_selector::{LevelSelector, TtlCompactionSelector};
    use crate::hummock::compaction::LocalSelectorStatistic;

    #[test]
    fn test_ttl_reclaim_compaction_selector() {
        let config = Arc::new(
            CompactionConfigBuilder::new()
                .max_level(4)
                .max_space_reclaim_bytes(400)
                .build(),
        );
        let l0 = generate_l0_nonoverlapping_sublevels(vec![]);
        assert_eq!(l0.sub_levels.len(), 0);
        let levels = vec![
            generate_level(1, vec![]),
            generate_level(2, vec![]),
            generate_level(
                3,
                vec![
                    generate_table(0, 1, 150, 151, 1),
                    generate_table(1, 1, 250, 251, 1),
                ],
            ),
            Level {
                level_idx: 4,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(2, 1, 0, 100, 1),
                    generate_table(3, 1, 101, 200, 1),
                    generate_table(4, 1, 222, 300, 1),
                    generate_table(5, 1, 333, 400, 1),
                    generate_table(6, 1, 444, 500, 1),
                    generate_table(7, 1, 555, 600, 1),
                    generate_table(8, 1, 666, 700, 1),
                    generate_table(9, 1, 777, 800, 1),
                    generate_table(10, 1, 888, 900, 1),
                ],
                total_file_size: 0,
                sub_level_id: 0,
            },
        ];
        assert_eq!(levels.len(), 4);
        let levels = Levels {
            levels,
            l0: Some(l0),
        };
        let mut levels_handler = (0..5).map(LevelHandler::new).collect_vec();
        let mut local_stats = LocalSelectorStatistic::default();
        let mut selector = TtlCompactionSelector::new(config);

        {
            // pick space reclaim
            let task = selector
                .pick_compaction(1, &levels, &mut levels_handler, &mut local_stats)
                .unwrap();
            assert_compaction_task(&task, &levels_handler);
            assert_eq!(task.input.input_levels.len(), 2);
            assert_eq!(task.input.input_levels[0].level_idx, 4);
            assert_eq!(task.input.input_levels[0].table_infos.len(), 5);

            let mut start_id = 2;
            for sst in &task.input.input_levels[0].table_infos {
                assert_eq!(start_id, sst.id);
                start_id += 1;
            }

            assert_eq!(task.input.input_levels[1].level_idx, 4);
            assert_eq!(task.input.input_levels[1].table_infos.len(), 0);
            assert_eq!(task.input.target_level, 4);
            assert!(matches!(
                task.compaction_task_type,
                compact_task::TaskType::Ttl
            ));
        }

        {
            for level_handler in &mut levels_handler {
                for pending_task_id in &level_handler.pending_tasks_ids() {
                    level_handler.remove_task(*pending_task_id);
                }
            }

            // pick space reclaim
            let task = selector
                .pick_compaction(1, &levels, &mut levels_handler, &mut local_stats)
                .unwrap();
            assert_compaction_task(&task, &levels_handler);
            assert_eq!(task.input.input_levels.len(), 2);
            assert_eq!(task.input.input_levels[0].level_idx, 4);

            // test select index, picker will select file from last_select_index
            let all_file_count = levels.get_levels().last().unwrap().get_table_infos().len();
            assert_eq!(
                task.input.input_levels[0].table_infos.len(),
                all_file_count - 5
            );

            let mut start_id = 7;
            for sst in &task.input.input_levels[0].table_infos {
                assert_eq!(start_id, sst.id);
                start_id += 1;
            }

            assert_eq!(task.input.input_levels[1].level_idx, 4);
            assert_eq!(task.input.input_levels[1].table_infos.len(), 0);
            assert_eq!(task.input.target_level, 4);
            assert!(matches!(
                task.compaction_task_type,
                compact_task::TaskType::Ttl
            ));
        }
    }
}

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

use std::collections::HashSet;

use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::{InputLevel, SstableInfo};

use crate::hummock::compaction::CompactionInput;
use crate::hummock::level_handler::LevelHandler;

pub struct SpaceReclaimCompactionPicker {
    // config
    pub max_space_reclaim_bytes: u64,
    pub all_table_ids: HashSet<u32>,
}

#[derive(Default)]
pub struct SpaceReclaimPickerState {
    pub last_select_index: usize,
}

impl SpaceReclaimCompactionPicker {
    pub fn new(max_space_reclaim_bytes: u64, all_table_ids: HashSet<u32>) -> Self {
        Self {
            max_space_reclaim_bytes,
            all_table_ids,
        }
    }

    fn filter(&self, sst: &SstableInfo) -> bool {
        let table_id_in_sst = sst.table_ids.iter().cloned().collect::<HashSet<u32>>();
        // it means all the table exist , so we not need to pick this sst
        table_id_in_sst
            .iter()
            .all(|id| self.all_table_ids.contains(id))
    }
}

impl SpaceReclaimCompactionPicker {
    pub fn pick_compaction(
        &mut self,
        levels: &Levels,
        level_handlers: &[LevelHandler],
        state: &mut SpaceReclaimPickerState,
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
            if level_handler.is_pending_compact(&sst.id) || self.filter(sst) {
                continue;
            }

            select_input_ssts.push(sst.clone());
            select_file_size += sst.file_size;
            if select_file_size > self.max_space_reclaim_bytes {
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

    use std::collections::HashMap;

    use itertools::Itertools;
    use risingwave_pb::hummock::compact_task;
    pub use risingwave_pb::hummock::{KeyRange, Level, LevelType};

    use super::*;
    use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
    use crate::hummock::compaction::level_selector::tests::{
        assert_compaction_task, generate_l0_nonoverlapping_sublevels, generate_level,
        generate_table_with_ids_and_epochs,
    };
    use crate::hummock::compaction::level_selector::SpaceReclaimCompactionSelector;
    use crate::hummock::compaction::{LevelSelector, LocalSelectorStatistic};
    use crate::hummock::model::CompactionGroup;

    #[test]
    fn test_space_reclaim_compaction_selector() {
        let max_space_reclaim_bytes = 400;
        let config = CompactionConfigBuilder::new()
            .max_level(4)
            .max_space_reclaim_bytes(max_space_reclaim_bytes)
            .build();
        let group_config = CompactionGroup::new(1, config);

        let l0 = generate_l0_nonoverlapping_sublevels(vec![]);
        assert_eq!(l0.sub_levels.len(), 0);
        let levels = vec![
            generate_level(1, vec![]),
            generate_level(2, vec![]),
            generate_level(
                3,
                vec![
                    generate_table_with_ids_and_epochs(0, 1, 150, 151, 1, vec![0], 0, 0),
                    generate_table_with_ids_and_epochs(1, 1, 250, 251, 1, vec![1], 0, 0),
                ],
            ),
            Level {
                level_idx: 4,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table_with_ids_and_epochs(2, 1, 0, 100, 1, vec![2], 0, 0),
                    generate_table_with_ids_and_epochs(3, 1, 101, 200, 1, vec![3], 0, 0),
                    generate_table_with_ids_and_epochs(4, 1, 222, 300, 1, vec![4], 0, 0),
                    generate_table_with_ids_and_epochs(5, 1, 333, 400, 1, vec![5], 0, 0),
                    generate_table_with_ids_and_epochs(6, 1, 444, 500, 1, vec![6], 0, 0),
                    generate_table_with_ids_and_epochs(7, 1, 555, 600, 1, vec![7], 0, 0),
                    generate_table_with_ids_and_epochs(8, 1, 666, 700, 1, vec![8], 0, 0),
                    generate_table_with_ids_and_epochs(9, 1, 777, 800, 1, vec![9], 0, 0),
                    generate_table_with_ids_and_epochs(10, 1, 888, 900, 1, vec![10], 0, 0),
                ],
                total_file_size: 0,
                sub_level_id: 0,
            },
        ];
        assert_eq!(levels.len(), 4);
        let mut levels = Levels {
            levels,
            l0: Some(l0),
            ..Default::default()
        };
        let mut levels_handler = (0..5).map(LevelHandler::new).collect_vec();
        let mut local_stats = LocalSelectorStatistic::default();

        let mut selector = SpaceReclaimCompactionSelector::default();
        {
            // pick space reclaim
            let task = selector
                .pick_compaction(
                    1,
                    &group_config,
                    &levels,
                    &mut levels_handler,
                    &mut local_stats,
                    HashMap::default(),
                )
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
                compact_task::TaskType::SpaceReclaim
            ));

            // in this case, no files is pending, so it limit by max_space_reclaim_bytes
            let select_file_size: u64 = task.input.input_levels[0]
                .table_infos
                .iter()
                .map(|sst| sst.file_size)
                .sum();
            assert!(select_file_size > max_space_reclaim_bytes);
        }

        {
            for level_handler in &mut levels_handler {
                for pending_task_id in &level_handler.pending_tasks_ids() {
                    level_handler.remove_task(*pending_task_id);
                }
            }

            // pick space reclaim
            let task = selector
                .pick_compaction(
                    1,
                    &group_config,
                    &levels,
                    &mut levels_handler,
                    &mut local_stats,
                    HashMap::default(),
                )
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
                compact_task::TaskType::SpaceReclaim
            ));
        }

        {
            for level_handler in &mut levels_handler {
                for pending_task_id in &level_handler.pending_tasks_ids() {
                    level_handler.remove_task(*pending_task_id);
                }
            }

            levels.member_table_ids = vec![2, 3, 4, 5, 6, 7, 8, 9, 10];
            // pick space reclaim
            let task = selector.pick_compaction(
                1,
                &group_config,
                &levels,
                &mut levels_handler,
                &mut local_stats,
                HashMap::default(),
            );
            assert!(task.is_none());
        }

        {
            for level_handler in &mut levels_handler {
                for pending_task_id in &level_handler.pending_tasks_ids() {
                    level_handler.remove_task(*pending_task_id);
                }
            }

            levels.member_table_ids = vec![2, 3, 4, 5, 6, 7, 8, 9];
            // pick space reclaim
            let task = selector
                .pick_compaction(
                    1,
                    &group_config,
                    &levels,
                    &mut levels_handler,
                    &mut local_stats,
                    HashMap::default(),
                )
                .unwrap();
            assert_compaction_task(&task, &levels_handler);
            assert_eq!(task.input.input_levels.len(), 2);
            assert_eq!(task.input.input_levels[0].level_idx, 4);

            assert_eq!(task.input.input_levels[0].table_infos.len(), 1);

            let mut start_id = 10;
            for sst in &task.input.input_levels[0].table_infos {
                assert_eq!(start_id, sst.id);
                start_id += 1;
            }

            assert_eq!(task.input.input_levels[1].level_idx, 4);
            assert_eq!(task.input.input_levels[1].table_infos.len(), 0);
            assert_eq!(task.input.target_level, 4);
            assert!(matches!(
                task.compaction_task_type,
                compact_task::TaskType::SpaceReclaim
            ));
        }
    }
}

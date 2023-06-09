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

use super::CompactionInput;
use crate::hummock::level_handler::LevelHandler;

// The execution model of SpaceReclaimCompactionPicker scans through the last level of files by
// key_range and selects the appropriate files to generate compaction
pub struct SpaceReclaimCompactionPicker {
    // config
    pub max_space_reclaim_bytes: u64,

    // for filter
    pub all_table_ids: HashSet<u32>,
}

// According to the execution model of SpaceReclaimCompactionPicker, SpaceReclaimPickerState is
// designed to record the state of each round of scanning
#[derive(Default)]
pub struct SpaceReclaimPickerState {
    pub last_level: usize,
}

impl SpaceReclaimCompactionPicker {
    pub fn new(max_space_reclaim_bytes: u64, all_table_ids: HashSet<u32>) -> Self {
        Self {
            max_space_reclaim_bytes,
            all_table_ids,
        }
    }

    fn exist_table_count(&self, sst: &SstableInfo) -> usize {
        // it means all the table exist , so we not need to pick this sst
        sst.table_ids
            .iter()
            .filter(|id| self.all_table_ids.contains(id))
            .count()
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
        let mut select_input_ssts = vec![];
        if let Some(l0) = levels.l0.as_ref() && state.last_level == 0 {
            // only pick trivial reclaim sstables because this kind of task could be optimized and do not need send to compactor.
            for level in &l0.sub_levels {
                for sst in &level.table_infos {
                    let exist_count = self.exist_table_count(sst);
                    if exist_count == sst.table_ids.len() ||  level_handlers[0].is_pending_compact( &sst.sst_id) {
                        if !select_input_ssts.is_empty() {
                            break;
                        }
                    } else if exist_count == 0 {
                        select_input_ssts.push(sst.clone());
                    } else if !select_input_ssts.is_empty() {
                        break;
                    }
                }
                if !select_input_ssts.is_empty() {
                    return Some(CompactionInput {
                        input_levels: vec![
                            InputLevel {
                                level_idx: level.level_idx,
                                level_type: level.level_type,
                                table_infos: select_input_ssts,
                            },
                            InputLevel {
                                level_idx: 0,
                                level_type: level.level_type,
                                table_infos: vec![],
                            },
                        ],
                        target_level: level.level_idx as usize,
                        target_sub_level_id: level.sub_level_id,
                    });
                }
            }
            state.last_level = 1;
        }
        while state.last_level <= levels.levels.len() {
            let mut is_trivial_task = true;
            let mut select_file_size = 0;
            for sst in &levels.levels[state.last_level - 1].table_infos {
                let exist_count = self.exist_table_count(sst);
                let need_reclaim = exist_count < sst.table_ids.len();
                let is_trivial = exist_count == 0;
                if level_handlers[state.last_level].is_pending_compact(&sst.sst_id) || !need_reclaim
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

                if !is_trivial {
                    if !select_input_ssts.is_empty() && is_trivial_task {
                        break;
                    }
                    is_trivial_task = false;
                }

                select_input_ssts.push(sst.clone());
                select_file_size += sst.file_size;
                if select_file_size > self.max_space_reclaim_bytes && !is_trivial_task {
                    break;
                }
            }

            // turn to next_round
            if !select_input_ssts.is_empty() {
                return Some(CompactionInput {
                    input_levels: vec![
                        InputLevel {
                            level_idx: state.last_level as u32,
                            level_type: levels.levels[state.last_level - 1].level_type,
                            table_infos: select_input_ssts,
                        },
                        InputLevel {
                            level_idx: state.last_level as u32,
                            level_type: levels.levels[state.last_level - 1].level_type,
                            table_infos: vec![],
                        },
                    ],
                    target_level: state.last_level,
                    target_sub_level_id: 0,
                });
            }
            state.last_level += 1;
        }
        state.last_level = 0;
        None
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
        let mut levels = vec![
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
                    generate_table_with_ids_and_epochs(10, 1, 888, 1600, 1, vec![10], 0, 0),
                    generate_table_with_ids_and_epochs(11, 1, 1600, 1800, 1, vec![10], 0, 0),
                ],
                total_file_size: 0,
                sub_level_id: 0,
                uncompressed_file_size: 0,
            },
        ];

        {
            let sst_10 = levels[3].table_infos.get_mut(8).unwrap();
            assert_eq!(10, sst_10.get_sst_id());
            sst_10.key_range.as_mut().unwrap().right_exclusive = true;
        }

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
            // test max_pick_files limit

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
            assert_eq!(task.input.input_levels[0].level_idx, 3);
            assert_eq!(task.input.input_levels[0].table_infos.len(), 2);
            levels_handler[4].add_pending_task(0, 4, &levels.levels[3].table_infos[5..6]);
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
            assert_eq!(task.input.input_levels.len(), 2);
            assert_eq!(task.input.input_levels[0].level_idx, 4);
            assert_eq!(task.input.input_levels[0].table_infos.len(), 5);

            let mut start_id = 2;
            for sst in &task.input.input_levels[0].table_infos {
                assert_eq!(start_id, sst.get_sst_id());
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
            // test pick next range
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
            assert_eq!(task.input.input_levels[0].table_infos.len(), 4);
            assert_eq!(task.input.target_level, 4);
            assert!(matches!(
                task.compaction_task_type,
                compact_task::TaskType::SpaceReclaim
            ));
            let mut start_id = 8;
            for sst in &task.input.input_levels[0].table_infos {
                assert_eq!(start_id, sst.get_sst_id());
                start_id += 1;
            }

            assert!(selector
                .pick_compaction(
                    1,
                    &group_config,
                    &levels,
                    &mut levels_handler,
                    &mut local_stats,
                    HashMap::default(),
                )
                .is_none())
        }

        {
            // test state, after above 2

            for level_handler in &mut levels_handler {
                for pending_task_id in &level_handler.pending_tasks_ids() {
                    level_handler.remove_task(*pending_task_id);
                }
            }

            levels.member_table_ids = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
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
            assert_eq!(task.input.input_levels[0].level_idx, 3);
            assert_eq!(task.input.input_levels[0].table_infos.len(), 2);
            assert_eq!(task.input.target_level, 3);
            assert!(matches!(
                task.compaction_task_type,
                compact_task::TaskType::SpaceReclaim
            ));
        }

        {
            // test continuous file selection
            for level_handler in &mut levels_handler {
                for pending_task_id in &level_handler.pending_tasks_ids() {
                    level_handler.remove_task(*pending_task_id);
                }
            }

            // rebuild selector
            selector = SpaceReclaimCompactionSelector::default();
            // cut range [3,4] [6] [8,9,10]
            levels.member_table_ids = vec![0, 1, 2, 5, 7];
            let expect_task_file_count = vec![2, 1, 4];
            let expect_task_sst_id_range = vec![vec![3, 4], vec![6], vec![8, 9, 10, 11]];
            for (index, x) in expect_task_file_count.iter().enumerate() {
                // // pick space reclaim
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

                assert_eq!(task.input.input_levels[0].table_infos.len(), *x);
                let select_sst = &task.input.input_levels[0]
                    .table_infos
                    .iter()
                    .map(|sst| sst.get_sst_id())
                    .collect_vec();
                assert!(select_sst.is_sorted());
                assert_eq!(expect_task_sst_id_range[index], *select_sst);

                assert_eq!(task.input.input_levels[1].level_idx, 4);
                assert_eq!(task.input.input_levels[1].table_infos.len(), 0);
                assert_eq!(task.input.target_level, 4);
                assert!(matches!(
                    task.compaction_task_type,
                    compact_task::TaskType::SpaceReclaim
                ));
            }
        }

        {
            // test continuous file selection with filter change
            for level_handler in &mut levels_handler {
                for pending_task_id in &level_handler.pending_tasks_ids() {
                    level_handler.remove_task(*pending_task_id);
                }
            }

            // rebuild selector
            selector = SpaceReclaimCompactionSelector::default();
            // cut range [3,4] [6] [8,9,10]
            levels.member_table_ids = vec![0, 1, 2, 5, 7];
            let expect_task_file_count = vec![2, 1, 5];
            let expect_task_sst_id_range = vec![vec![3, 4], vec![6], vec![7, 8, 9, 10, 11]];
            for (index, x) in expect_task_file_count.iter().enumerate() {
                if index == expect_task_file_count.len() - 1 {
                    levels.member_table_ids = vec![2, 5];
                }

                // // pick space reclaim
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

                assert_eq!(task.input.input_levels[0].table_infos.len(), *x);
                let select_sst = &task.input.input_levels[0]
                    .table_infos
                    .iter()
                    .map(|sst| sst.get_sst_id())
                    .collect_vec();
                assert!(select_sst.is_sorted());
                assert_eq!(expect_task_sst_id_range[index], *select_sst);

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
}

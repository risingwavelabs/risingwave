// Copyright 2025 RisingWave Labs
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

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use risingwave_common::catalog::TableOption;
use risingwave_common::util::epoch::Epoch;
use risingwave_hummock_sdk::compaction_group::StateTableId;
use risingwave_hummock_sdk::key_range::{KeyRange, KeyRangeCommon};
use risingwave_hummock_sdk::level::{InputLevel, Levels};
use risingwave_hummock_sdk::sstable_info::SstableInfo;

use super::CompactionInput;
use crate::hummock::level_handler::LevelHandler;

const MIN_TTL_EXPIRE_INTERVAL_MS: u64 = 60 * 60 * 1000; // 1h

#[derive(Default)]
pub struct TtlPickerState {
    // Because of the right_exclusive, we use KeyRangeCommon to determine if the end_bounds
    // overlap instead of directly comparing Vec<u8>. We don't need to use the start_bound in the
    // filter, set it to -inf

    // record the end_bound that has been scanned
    pub last_select_end_bound: KeyRange,

    // record the end_bound in the current round of scanning tasks
    pub end_bound_in_round: KeyRange,
}

impl TtlPickerState {
    pub fn valid(&self) -> bool {
        !self.end_bound_in_round.right.is_empty()
    }

    pub fn init(&mut self, key_range: KeyRange) {
        self.last_select_end_bound = KeyRange {
            left: Bytes::default(),
            right: key_range.left.clone(),
            right_exclusive: true,
        };
        self.end_bound_in_round = key_range;
    }

    pub fn clear(&mut self) {
        self.end_bound_in_round = KeyRange::default();
        self.last_select_end_bound = KeyRange::default();
    }
}

pub struct TtlReclaimCompactionPicker {
    table_id_to_ttl: HashMap<u32, u32>,
}

impl TtlReclaimCompactionPicker {
    pub fn new(table_id_to_options: &HashMap<StateTableId, TableOption>) -> Self {
        let table_id_to_ttl: HashMap<u32, u32> = table_id_to_options
            .iter()
            .filter(|id_to_option| {
                let table_option = id_to_option.1;
                table_option.retention_seconds.is_some()
            })
            .map(|id_to_option| (*id_to_option.0, id_to_option.1.retention_seconds.unwrap()))
            .collect();

        Self { table_id_to_ttl }
    }

    fn filter(&self, sst: &SstableInfo, current_epoch_physical_time: u64) -> bool {
        let table_id_in_sst = sst.table_ids.iter().cloned().collect::<HashSet<u32>>();
        let expire_epoch =
            Epoch::from_physical_time(current_epoch_physical_time - MIN_TTL_EXPIRE_INTERVAL_MS);

        for table_id in table_id_in_sst {
            match self.table_id_to_ttl.get(&table_id) {
                Some(ttl_second_u32) => {
                    assert!(*ttl_second_u32 > 0);
                    // default to zero.
                    let ttl_mill = *ttl_second_u32 as u64 * 1000;
                    let min_epoch = expire_epoch.subtract_ms(ttl_mill);
                    if Epoch(sst.min_epoch) < min_epoch {
                        return false;
                    }
                }
                None => continue,
            }
        }

        true
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

        if reclaimed_level.table_infos.is_empty() {
            // 1. not file to be picked
            state.clear();
            return None;
        }

        if state.valid()
            && state
                .last_select_end_bound
                .compare_right_with(&state.end_bound_in_round.right)
                == std::cmp::Ordering::Greater
        {
            // in round but end_key overflow
            // turn to next_round
            state.clear();
            return None;
        }

        if !state.valid() {
            // new round init key_range bound with table_infos
            let first_sst = reclaimed_level.table_infos.first().unwrap();
            let last_sst = reclaimed_level.table_infos.last().unwrap();

            let key_range_this_round = KeyRange {
                left: first_sst.key_range.left.clone(),
                right: last_sst.key_range.right.clone(),
                right_exclusive: last_sst.key_range.right_exclusive,
            };

            state.init(key_range_this_round);
        }

        let current_epoch_physical_time = Epoch::now().physical_time();

        for sst in &reclaimed_level.table_infos {
            let unmatched_sst = sst.key_range.sstable_overlap(&state.last_select_end_bound);

            if unmatched_sst
                || level_handler.is_pending_compact(&sst.sst_id)
                || self.filter(sst, current_epoch_physical_time)
            {
                continue;
            }

            select_input_ssts.push(sst.clone());
            break;
        }

        // turn to next_round
        if select_input_ssts.is_empty() {
            state.clear();
            return None;
        }

        let select_last_sst = select_input_ssts.last().unwrap();
        state.last_select_end_bound.full_key_extend(&KeyRange {
            left: Bytes::default(),
            right: select_last_sst.key_range.right.clone(),
            right_exclusive: select_last_sst.key_range.right_exclusive,
        });

        Some(CompactionInput {
            select_input_size: select_input_ssts.iter().map(|sst| sst.sst_size).sum(),
            total_file_count: select_input_ssts.len() as _,
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
            ..Default::default()
        })
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use itertools::Itertools;
    use risingwave_hummock_sdk::level::Level;
    use risingwave_hummock_sdk::sstable_info::SstableInfoInner;
    use risingwave_hummock_sdk::version::HummockVersionStateTableInfo;
    pub use risingwave_pb::hummock::LevelType;
    use risingwave_pb::hummock::compact_task;

    use super::*;
    use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
    use crate::hummock::compaction::selector::tests::{
        assert_compaction_task, generate_l0_nonoverlapping_sublevels, generate_level,
        generate_table_with_ids_and_epochs,
    };
    use crate::hummock::compaction::selector::{CompactionSelector, TtlCompactionSelector};
    use crate::hummock::compaction::{CompactionDeveloperConfig, LocalSelectorStatistic};
    use crate::hummock::model::CompactionGroup;
    use crate::hummock::test_utils::compaction_selector_context;

    #[test]
    fn test_ttl_reclaim_compaction_selector() {
        let config = CompactionConfigBuilder::new()
            .max_level(4)
            .max_space_reclaim_bytes(400)
            .build();
        let group_config = CompactionGroup::new(1, config);
        let l0 = generate_l0_nonoverlapping_sublevels(vec![]);
        assert_eq!(l0.sub_levels.len(), 0);

        let current_epoch_time = Epoch::now().physical_time();
        let expired_epoch = Epoch::from_physical_time(
            current_epoch_time - MIN_TTL_EXPIRE_INTERVAL_MS - (1000 * 1000),
        )
        .0;
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
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![
                    generate_table_with_ids_and_epochs(2, 1, 0, 100, 1, vec![2], expired_epoch, 0),
                    generate_table_with_ids_and_epochs(
                        3,
                        1,
                        101,
                        200,
                        1,
                        vec![3],
                        expired_epoch,
                        0,
                    ),
                    generate_table_with_ids_and_epochs(
                        4,
                        1,
                        222,
                        300,
                        1,
                        vec![4],
                        expired_epoch,
                        u64::MAX,
                    ),
                    generate_table_with_ids_and_epochs(
                        5,
                        1,
                        333,
                        400,
                        1,
                        vec![5],
                        expired_epoch,
                        u64::MAX,
                    ),
                    generate_table_with_ids_and_epochs(
                        6,
                        1,
                        444,
                        500,
                        1,
                        vec![6],
                        expired_epoch,
                        u64::MAX,
                    ),
                    generate_table_with_ids_and_epochs(
                        7,
                        1,
                        555,
                        600,
                        1,
                        vec![7],
                        expired_epoch,
                        u64::MAX,
                    ),
                    generate_table_with_ids_and_epochs(
                        8,
                        1,
                        666,
                        700,
                        1,
                        vec![8],
                        expired_epoch,
                        u64::MAX,
                    ),
                    generate_table_with_ids_and_epochs(
                        9,
                        1,
                        777,
                        800,
                        1,
                        vec![9],
                        expired_epoch,
                        u64::MAX,
                    ),
                    generate_table_with_ids_and_epochs(
                        10,
                        1,
                        888,
                        1600,
                        1,
                        vec![10],
                        expired_epoch,
                        u64::MAX,
                    ),
                    generate_table_with_ids_and_epochs(
                        11,
                        1,
                        1600,
                        1800,
                        1,
                        vec![10],
                        expired_epoch,
                        u64::MAX,
                    ),
                ],
                total_file_size: 0,
                sub_level_id: 0,
                uncompressed_file_size: 0,
                ..Default::default()
            },
        ];

        {
            let sst_10 = levels[3].table_infos.get_mut(8).unwrap();
            assert_eq!(10, sst_10.sst_id);
            *sst_10 = SstableInfoInner {
                key_range: KeyRange {
                    right_exclusive: true,
                    ..sst_10.key_range.clone()
                },
                ..sst_10.get_inner()
            }
            .into();
        }

        assert_eq!(levels.len(), 4);
        let levels = Levels {
            levels,
            l0,
            ..Default::default()
        };
        let mut levels_handler = (0..5).map(LevelHandler::new).collect_vec();
        let mut local_stats = LocalSelectorStatistic::default();
        let mut selector = TtlCompactionSelector::default();
        {
            let table_id_to_options: HashMap<u32, TableOption> = (2..=10)
                .map(|table_id| {
                    (
                        table_id as u32,
                        TableOption {
                            retention_seconds: Some(5_u32),
                        },
                    )
                })
                .collect();
            // pick ttl reclaim
            let task = selector
                .pick_compaction(
                    1,
                    compaction_selector_context(
                        &group_config,
                        &levels,
                        &BTreeSet::new(),
                        &mut levels_handler,
                        &mut local_stats,
                        &table_id_to_options,
                        Arc::new(CompactionDeveloperConfig::default()),
                        &Default::default(),
                        &HummockVersionStateTableInfo::empty(),
                    ),
                )
                .unwrap();
            assert_compaction_task(&task, &levels_handler);
            assert_eq!(task.input.input_levels.len(), 2);
            assert_eq!(task.input.input_levels[0].level_idx, 4);
            assert_eq!(task.input.input_levels[0].table_infos.len(), 1);

            let mut start_id = 2;
            for sst in &task.input.input_levels[0].table_infos {
                assert_eq!(start_id, sst.sst_id);
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

            let table_id_to_options: HashMap<u32, TableOption> = (2..=10)
                .map(|table_id| {
                    (
                        table_id as u32,
                        TableOption {
                            retention_seconds: Some(5_u32),
                        },
                    )
                })
                .collect();

            // pick ttl reclaim
            let task = selector
                .pick_compaction(
                    1,
                    compaction_selector_context(
                        &group_config,
                        &levels,
                        &BTreeSet::new(),
                        &mut levels_handler,
                        &mut local_stats,
                        &table_id_to_options,
                        Arc::new(CompactionDeveloperConfig::default()),
                        &Default::default(),
                        &HummockVersionStateTableInfo::empty(),
                    ),
                )
                .unwrap();
            assert_compaction_task(&task, &levels_handler);
            assert_eq!(task.input.input_levels.len(), 2);
            assert_eq!(task.input.input_levels[0].level_idx, 4);

            // test select index, picker will select file from state
            assert_eq!(task.input.input_levels[0].table_infos.len(), 1);

            let mut start_id = 3;
            for sst in &task.input.input_levels[0].table_infos {
                assert_eq!(start_id, sst.sst_id);
                start_id += 1;
            }

            assert_eq!(task.input.input_levels[1].level_idx, 4);
            assert_eq!(task.input.input_levels[1].table_infos.len(), 0);
            assert_eq!(task.input.target_level, 4);
            assert!(matches!(
                task.compaction_task_type,
                compact_task::TaskType::Ttl
            ));

            // test pick key_range right exclusive
            let task = selector
                .pick_compaction(
                    1,
                    compaction_selector_context(
                        &group_config,
                        &levels,
                        &BTreeSet::new(),
                        &mut levels_handler,
                        &mut local_stats,
                        &table_id_to_options,
                        Arc::new(CompactionDeveloperConfig::default()),
                        &Default::default(),
                        &HummockVersionStateTableInfo::empty(),
                    ),
                )
                .unwrap();
            assert_compaction_task(&task, &levels_handler);
            assert_eq!(task.input.input_levels.len(), 2);
            assert_eq!(task.input.input_levels[0].level_idx, 4);
            assert_eq!(task.input.input_levels[0].table_infos.len(), 1);
            assert_eq!(task.input.input_levels[1].level_idx, 4);
            assert_eq!(task.input.input_levels[1].table_infos.len(), 0);
            assert_eq!(task.input.target_level, 4);
            assert!(matches!(
                task.compaction_task_type,
                compact_task::TaskType::Ttl
            ));
            for sst in &task.input.input_levels[0].table_infos {
                assert_eq!(start_id, sst.sst_id);
                start_id += 1;
            }
        }

        {
            for level_handler in &mut levels_handler {
                for pending_task_id in &level_handler.pending_tasks_ids() {
                    level_handler.remove_task(*pending_task_id);
                }
            }

            // rebuild selector
            selector = TtlCompactionSelector::default();
            let mut table_id_to_options: HashMap<u32, TableOption> = (2..=10)
                .map(|table_id| {
                    (
                        table_id as u32,
                        TableOption {
                            retention_seconds: Some(7200),
                        },
                    )
                })
                .collect();

            table_id_to_options.insert(
                5,
                TableOption {
                    retention_seconds: Some(5),
                },
            );

            // // pick ttl reclaim
            let task = selector
                .pick_compaction(
                    1,
                    compaction_selector_context(
                        &group_config,
                        &levels,
                        &BTreeSet::new(),
                        &mut levels_handler,
                        &mut local_stats,
                        &table_id_to_options,
                        Arc::new(CompactionDeveloperConfig::default()),
                        &Default::default(),
                        &HummockVersionStateTableInfo::empty(),
                    ),
                )
                .unwrap();
            assert_compaction_task(&task, &levels_handler);
            assert_eq!(task.input.input_levels.len(), 2);
            assert_eq!(task.input.input_levels[0].level_idx, 4);

            // test table_option_filter
            assert_eq!(task.input.input_levels[0].table_infos.len(), 1);
            let select_sst = &task.input.input_levels[0].table_infos.first().unwrap();
            assert_eq!(select_sst.sst_id, 5);

            assert_eq!(task.input.input_levels[1].level_idx, 4);
            assert_eq!(task.input.input_levels[1].table_infos.len(), 0);
            assert_eq!(task.input.target_level, 4);
            assert!(matches!(
                task.compaction_task_type,
                compact_task::TaskType::Ttl
            ));
        }

        {
            // test empty table_option filter

            for level_handler in &mut levels_handler {
                for pending_task_id in &level_handler.pending_tasks_ids() {
                    level_handler.remove_task(*pending_task_id);
                }
            }

            // rebuild selector
            selector = TtlCompactionSelector::default();

            // // pick ttl reclaim
            let task = selector.pick_compaction(
                1,
                compaction_selector_context(
                    &group_config,
                    &levels,
                    &BTreeSet::new(),
                    &mut levels_handler,
                    &mut local_stats,
                    &HashMap::default(),
                    Arc::new(CompactionDeveloperConfig::default()),
                    &Default::default(),
                    &HummockVersionStateTableInfo::empty(),
                ),
            );

            // empty table_options does not select any files
            assert!(task.is_none());
        }

        {
            // test continuous file selection
            for level_handler in &mut levels_handler {
                for pending_task_id in &level_handler.pending_tasks_ids() {
                    level_handler.remove_task(*pending_task_id);
                }
            }

            // rebuild selector
            selector = TtlCompactionSelector::default();
            let mut table_id_to_options: HashMap<u32, TableOption> = (2..=10)
                .map(|table_id| {
                    (
                        table_id as u32,
                        TableOption {
                            retention_seconds: Some(5_u32),
                        },
                    )
                })
                .collect();

            // cut range [2,3,4] [6,7] [10]
            table_id_to_options.insert(
                5,
                TableOption {
                    retention_seconds: Some(7200_u32),
                },
            );

            table_id_to_options.insert(
                8,
                TableOption {
                    retention_seconds: Some(7200_u32),
                },
            );

            table_id_to_options.insert(
                9,
                TableOption {
                    retention_seconds: Some(7200_u32),
                },
            );

            let expect_task_file_count = [1, 1, 1];
            let expect_task_sst_id_range = [vec![2], vec![3], vec![4]];
            for (index, x) in expect_task_file_count.iter().enumerate() {
                // // pick ttl reclaim
                let task = selector
                    .pick_compaction(
                        1,
                        compaction_selector_context(
                            &group_config,
                            &levels,
                            &BTreeSet::new(),
                            &mut levels_handler,
                            &mut local_stats,
                            &table_id_to_options,
                            Arc::new(CompactionDeveloperConfig::default()),
                            &Default::default(),
                            &HummockVersionStateTableInfo::empty(),
                        ),
                    )
                    .unwrap();

                assert_compaction_task(&task, &levels_handler);
                assert_eq!(task.input.input_levels.len(), 2);
                assert_eq!(task.input.input_levels[0].level_idx, 4);

                // test table_option_filter
                assert_eq!(task.input.input_levels[0].table_infos.len(), *x);
                let select_sst = &task.input.input_levels[0]
                    .table_infos
                    .iter()
                    .map(|sst| sst.sst_id)
                    .collect_vec();
                assert!(select_sst.is_sorted());
                assert_eq!(expect_task_sst_id_range[index], *select_sst);

                assert_eq!(task.input.input_levels[1].level_idx, 4);
                assert_eq!(task.input.input_levels[1].table_infos.len(), 0);
                assert_eq!(task.input.target_level, 4);
                assert!(matches!(
                    task.compaction_task_type,
                    compact_task::TaskType::Ttl
                ));
            }
        }

        {
            // test continuous file selection  with filter change
            for level_handler in &mut levels_handler {
                for pending_task_id in &level_handler.pending_tasks_ids() {
                    level_handler.remove_task(*pending_task_id);
                }
            }

            // rebuild selector
            selector = TtlCompactionSelector::default();
            let mut table_id_to_options: HashMap<u32, TableOption> = (2..=10)
                .map(|table_id| {
                    (
                        table_id as u32,
                        TableOption {
                            retention_seconds: Some(5_u32),
                        },
                    )
                })
                .collect();

            // cut range [2,3,4] [6,7] [10]
            table_id_to_options.insert(
                5,
                TableOption {
                    retention_seconds: Some(7200_u32),
                },
            );

            table_id_to_options.insert(
                8,
                TableOption {
                    retention_seconds: Some(7200_u32),
                },
            );

            table_id_to_options.insert(
                9,
                TableOption {
                    retention_seconds: Some(7200_u32),
                },
            );

            let expect_task_file_count = [1, 1];
            let expect_task_sst_id_range = [vec![2], vec![3]];
            for (index, x) in expect_task_file_count.iter().enumerate() {
                if index == expect_task_file_count.len() - 1 {
                    table_id_to_options.insert(
                        5,
                        TableOption {
                            retention_seconds: Some(5_u32),
                        },
                    );
                }

                // // pick ttl reclaim
                let task = selector
                    .pick_compaction(
                        1,
                        compaction_selector_context(
                            &group_config,
                            &levels,
                            &BTreeSet::new(),
                            &mut levels_handler,
                            &mut local_stats,
                            &table_id_to_options,
                            Arc::new(CompactionDeveloperConfig::default()),
                            &Default::default(),
                            &HummockVersionStateTableInfo::empty(),
                        ),
                    )
                    .unwrap();

                assert_compaction_task(&task, &levels_handler);
                assert_eq!(task.input.input_levels.len(), 2);
                assert_eq!(task.input.input_levels[0].level_idx, 4);

                // test table_option_filter
                assert_eq!(task.input.input_levels[0].table_infos.len(), *x);
                let select_sst = &task.input.input_levels[0]
                    .table_infos
                    .iter()
                    .map(|sst| sst.sst_id)
                    .collect_vec();
                assert!(select_sst.is_sorted());
                assert_eq!(expect_task_sst_id_range[index], *select_sst);

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
}

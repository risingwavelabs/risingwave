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

use std::collections::HashSet;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockLevelsExt;
use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::{InputLevel, Level, LevelType, OverlappingLevel, SstableInfo};

use super::overlap_strategy::OverlapInfo;
use crate::hummock::compaction::overlap_strategy::{OverlapStrategy, RangeOverlapInfo};
use crate::hummock::compaction::{CompactionInput, CompactionPicker, ManualCompactionOption};
use crate::hummock::level_handler::LevelHandler;

pub struct ManualCompactionPicker {
    overlap_strategy: Arc<dyn OverlapStrategy>,
    option: ManualCompactionOption,
    target_level: usize,
}

impl ManualCompactionPicker {
    pub fn new(
        overlap_strategy: Arc<dyn OverlapStrategy>,
        option: ManualCompactionOption,
        target_level: usize,
    ) -> Self {
        Self {
            overlap_strategy,
            option,
            target_level,
        }
    }

    fn pick_l0_to_sub_level(
        &self,
        l0: &OverlappingLevel,
        level_handlers: &[LevelHandler],
    ) -> Option<CompactionInput> {
        assert_eq!(self.option.level, 0);
        let mut input_levels = vec![];
        let mut sub_level_id = 0;
        let mut start_idx = None;
        let mut end_idx = None;
        // Decides the range of sub levels as input.
        // We need pick consecutive sub_levels. See #5217.
        for (idx, level) in l0.sub_levels.iter().enumerate() {
            if !self.filter_level_by_option(level) {
                continue;
            }
            if level_handlers[0].is_level_pending_compact(level) {
                return None;
            }
            // Pick this sub_level.
            if start_idx.is_none() {
                sub_level_id = level.sub_level_id;
                start_idx = Some(idx as u64);
                end_idx = start_idx;
            } else {
                end_idx = Some(idx as u64);
            }
        }
        let (start_idx, end_idx) = match (start_idx, end_idx) {
            (Some(start_idx), Some(end_idx)) => (start_idx, end_idx),
            _ => {
                return None;
            }
        };
        // Construct input.
        for level in l0
            .sub_levels
            .iter()
            .skip(start_idx as usize)
            .take((end_idx - start_idx + 1) as usize)
        {
            input_levels.push(InputLevel {
                level_idx: 0,
                level_type: level.level_type,
                table_infos: level.table_infos.clone(),
            });
        }
        if input_levels.is_empty() {
            return None;
        }
        input_levels.reverse();
        Some(CompactionInput {
            input_levels,
            target_level: 0,
            target_sub_level_id: sub_level_id,
        })
    }

    fn pick_l0_to_base_level(
        &self,
        levels: &Levels,
        level_handlers: &[LevelHandler],
    ) -> Option<CompactionInput> {
        assert!(self.option.level == 0 && self.target_level > 0);
        for l in 1..self.target_level {
            assert!(levels.levels[l - 1].table_infos.is_empty());
        }
        let l0 = levels.l0.as_ref().unwrap();
        let mut input_levels = vec![];
        let mut max_sub_level_idx = usize::MAX;
        let mut info = self.overlap_strategy.create_overlap_info();
        // Decides the range of sub levels as input.
        for (idx, level) in l0.sub_levels.iter().enumerate() {
            if !self.filter_level_by_option(level) {
                continue;
            }
            if level_handlers[0].is_level_pending_compact(level) {
                return None;
            }
            // Pick this sub_level.
            max_sub_level_idx = idx;
        }
        if max_sub_level_idx == usize::MAX {
            return None;
        }
        // Construct input.
        for idx in 0..=max_sub_level_idx {
            for table in &l0.sub_levels[idx].table_infos {
                info.update(table);
            }
            input_levels.push(InputLevel {
                level_idx: 0,
                level_type: l0.sub_levels[idx].level_type,
                table_infos: l0.sub_levels[idx].table_infos.clone(),
            })
        }
        let target_input_ssts =
            info.check_multiple_overlap(&levels.levels[self.target_level - 1].table_infos);
        if target_input_ssts
            .iter()
            .any(|table| level_handlers[self.target_level].is_pending_compact(&table.id))
        {
            return None;
        }
        if input_levels.is_empty() {
            return None;
        }
        input_levels.reverse();
        input_levels.push(InputLevel {
            level_idx: self.target_level as u32,
            level_type: LevelType::Nonoverlapping as i32,
            table_infos: target_input_ssts,
        });

        Some(CompactionInput {
            input_levels,
            target_level: self.target_level,
            target_sub_level_id: 0,
        })
    }

    /// Returns false if the given `sst` is rejected by filter defined by `option`.
    /// Otherwise returns true.
    fn filter_level_by_option(&self, level: &Level) -> bool {
        let mut hint_sst_ids: HashSet<u64> = HashSet::new();
        hint_sst_ids.extend(self.option.sst_ids.iter());
        let tmp_sst_info = SstableInfo {
            key_range: Some(self.option.key_range.clone()),
            ..Default::default()
        };
        if self
            .overlap_strategy
            .check_overlap_with_tables(&[tmp_sst_info], &level.table_infos)
            .is_empty()
        {
            return false;
        }
        if !hint_sst_ids.is_empty()
            && !level
                .table_infos
                .iter()
                .any(|t| hint_sst_ids.contains(&t.id))
        {
            return false;
        }
        if !self.option.internal_table_id.is_empty()
            && !level.table_infos.iter().any(|sst_info| {
                sst_info
                    .table_ids
                    .iter()
                    .any(|t| self.option.internal_table_id.contains(t))
            })
        {
            return false;
        }
        true
    }
}

impl CompactionPicker for ManualCompactionPicker {
    fn pick_compaction(
        &self,
        levels: &Levels,
        level_handlers: &[LevelHandler],
    ) -> Option<CompactionInput> {
        if self.option.level == 0 {
            if !self.option.sst_ids.is_empty() {
                return self.pick_l0_to_sub_level(levels.l0.as_ref().unwrap(), level_handlers);
            } else if self.target_level > 0 {
                return self.pick_l0_to_base_level(levels, level_handlers);
            } else {
                return None;
            }
        }
        let mut hint_sst_ids: HashSet<u64> = HashSet::new();
        hint_sst_ids.extend(self.option.sst_ids.iter());
        let mut tmp_sst_info = SstableInfo::default();
        let mut range_overlap_info = RangeOverlapInfo::default();
        tmp_sst_info.key_range = Some(self.option.key_range.clone());
        range_overlap_info.update(&tmp_sst_info);
        let mut select_input_ssts = vec![];
        let level = self.option.level;
        let target_level = self.target_level;
        assert!(
            self.option.level == self.target_level || self.option.level + 1 == self.target_level
        );
        let level_table_infos: Vec<SstableInfo> = levels
            .get_level(self.option.level)
            .table_infos
            .iter()
            .filter(|sst_info| hint_sst_ids.is_empty() || hint_sst_ids.contains(&sst_info.id))
            .filter(|sst_info| range_overlap_info.check_overlap(sst_info))
            .filter(|sst_info| {
                if self.option.internal_table_id.is_empty() {
                    return true;
                }

                // to collect internal_table_id from sst_info
                let table_id_in_sst: Vec<u32> =
                    sst_info.get_table_ids().iter().cloned().collect_vec();

                // to filter sst_file by table_id
                for table_id in &table_id_in_sst {
                    if self.option.internal_table_id.contains(table_id) {
                        return true;
                    }
                }
                false
            })
            .cloned()
            .collect();

        if level_table_infos.is_empty() {
            return None;
        }

        for table in &level_table_infos {
            if level_handlers[self.option.level].is_pending_compact(&table.id) {
                continue;
            }

            if target_level != level {
                let overlap_files = self.overlap_strategy.check_base_level_overlap(
                    &[table.clone()],
                    &levels.levels[target_level - 1].table_infos,
                );

                if overlap_files
                    .iter()
                    .any(|table| level_handlers[target_level].is_pending_compact(&table.id))
                {
                    continue;
                }
            }

            select_input_ssts.push(table.clone());
        }

        let target_input_ssts = if target_level == level {
            vec![]
        } else {
            self.overlap_strategy.check_base_level_overlap(
                &select_input_ssts,
                &levels.get_level(target_level).table_infos,
            )
        };

        if target_input_ssts
            .iter()
            .any(|table| level_handlers[target_level].is_pending_compact(&table.id))
        {
            return None;
        }

        Some(CompactionInput {
            input_levels: vec![
                InputLevel {
                    level_idx: level as u32,
                    level_type: levels.levels[level - 1].level_type,
                    table_infos: select_input_ssts,
                },
                InputLevel {
                    level_idx: target_level as u32,
                    level_type: levels.levels[target_level - 1].level_type,
                    table_infos: target_input_ssts,
                },
            ],
            target_level,
            target_sub_level_id: 0,
        })
    }
}

#[cfg(test)]
pub mod tests {
    use std::collections::HashSet;

    pub use risingwave_pb::hummock::{KeyRange, Level, LevelType};

    use super::*;
    use crate::hummock::compaction::level_selector::tests::{
        generate_l0_nonoverlapping_sublevels, generate_l0_overlapping_sublevels, generate_table,
    };
    use crate::hummock::compaction::overlap_strategy::RangeOverlapStrategy;
    use crate::hummock::test_utils::iterator_test_key_of_epoch;

    fn clean_task_state(level_handler: &mut LevelHandler) {
        for pending_task_id in &level_handler.pending_tasks_ids() {
            level_handler.remove_task(*pending_task_id);
        }
    }

    fn is_l0_to_lbase(compaction_input: &CompactionInput) -> bool {
        compaction_input
            .input_levels
            .iter()
            .take(compaction_input.input_levels.len() - 1)
            .all(|i| i.level_idx == 0)
            && compaction_input
                .input_levels
                .iter()
                .last()
                .unwrap()
                .level_idx as usize
                == compaction_input.target_level
            && compaction_input.target_level > 0
    }

    fn is_l0_to_l0(compaction_input: &CompactionInput) -> bool {
        compaction_input
            .input_levels
            .iter()
            .all(|i| i.level_idx == 0)
            && compaction_input.target_level == 0
    }

    #[test]
    fn test_manual_compaction_picker() {
        let levels = vec![
            Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(0, 1, 0, 100, 1),
                    generate_table(1, 1, 101, 200, 1),
                    generate_table(2, 1, 222, 300, 1),
                ],
                total_file_size: 0,
                sub_level_id: 0,
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
                total_file_size: 0,
                sub_level_id: 0,
            },
        ];
        let mut levels = Levels {
            levels,
            l0: Some(generate_l0_nonoverlapping_sublevels(vec![])),
        };
        let mut levels_handler = vec![
            LevelHandler::new(0),
            LevelHandler::new(1),
            LevelHandler::new(2),
        ];

        {
            // test key_range option
            let option = ManualCompactionOption {
                level: 1,
                key_range: KeyRange {
                    left: iterator_test_key_of_epoch(1, 0, 1),
                    right: iterator_test_key_of_epoch(1, 201, 1),
                    inf: false,
                },
                ..Default::default()
            };

            let target_level = option.level + 1;
            let picker = ManualCompactionPicker::new(
                Arc::new(RangeOverlapStrategy::default()),
                option,
                target_level,
            );
            let result = picker.pick_compaction(&levels, &levels_handler).unwrap();
            result.add_pending_task(0, &mut levels_handler);

            assert_eq!(2, result.input_levels[0].table_infos.len());
            assert_eq!(3, result.input_levels[1].table_infos.len());
        }

        {
            clean_task_state(&mut levels_handler[1]);
            clean_task_state(&mut levels_handler[2]);

            // test all key range
            let option = ManualCompactionOption::default();
            let target_level = option.level + 1;
            let picker = ManualCompactionPicker::new(
                Arc::new(RangeOverlapStrategy::default()),
                option,
                target_level,
            );
            let result = picker.pick_compaction(&levels, &levels_handler).unwrap();
            result.add_pending_task(0, &mut levels_handler);

            assert_eq!(3, result.input_levels[0].table_infos.len());
            assert_eq!(3, result.input_levels[1].table_infos.len());
        }

        {
            clean_task_state(&mut levels_handler[1]);
            clean_task_state(&mut levels_handler[2]);

            let level_table_info = &mut levels.levels[0].table_infos;
            let table_info_1 = &mut level_table_info[1];
            table_info_1.table_ids.resize(2, 0);
            table_info_1.table_ids[0] = 1;
            table_info_1.table_ids[1] = 2;

            // test internal_table_id
            let option = ManualCompactionOption {
                level: 1,
                internal_table_id: HashSet::from([2]),
                ..Default::default()
            };

            let target_level = option.level + 1;
            let picker = ManualCompactionPicker::new(
                Arc::new(RangeOverlapStrategy::default()),
                option,
                target_level,
            );

            let result = picker.pick_compaction(&levels, &levels_handler).unwrap();
            result.add_pending_task(0, &mut levels_handler);

            assert_eq!(1, result.input_levels[0].table_infos.len());
            assert_eq!(2, result.input_levels[1].table_infos.len());
        }

        {
            clean_task_state(&mut levels_handler[1]);
            clean_task_state(&mut levels_handler[2]);

            // include all table_info
            let level_table_info = &mut levels.levels[0].table_infos;
            for table_info in level_table_info {
                table_info.table_ids.resize(2, 0);
                table_info.table_ids[0] = 1;
                table_info.table_ids[1] = 2;
            }

            // test key range filter first
            let option = ManualCompactionOption {
                sst_ids: vec![],
                level: 1,
                key_range: KeyRange {
                    left: iterator_test_key_of_epoch(1, 101, 1),
                    right: iterator_test_key_of_epoch(1, 199, 1),
                    inf: false,
                },
                internal_table_id: HashSet::from([2]),
            };

            let target_level = option.level + 1;
            let picker = ManualCompactionPicker::new(
                Arc::new(RangeOverlapStrategy::default()),
                option,
                target_level,
            );

            let result = picker.pick_compaction(&levels, &levels_handler).unwrap();

            assert_eq!(1, result.input_levels[0].table_infos.len());
            assert_eq!(2, result.input_levels[1].table_infos.len());
        }
    }

    fn generate_test_levels() -> (Levels, Vec<LevelHandler>) {
        let mut l0 = generate_l0_overlapping_sublevels(vec![
            vec![
                generate_table(5, 1, 0, 500, 2),
                generate_table(6, 2, 600, 1000, 2),
            ],
            vec![
                generate_table(7, 1, 0, 500, 3),
                generate_table(8, 2, 600, 1000, 3),
            ],
            vec![
                generate_table(9, 1, 300, 500, 4),
                generate_table(10, 2, 600, 1000, 4),
            ],
        ]);
        // Set a nonoverlapping sub_level.
        l0.sub_levels[1].level_type = LevelType::Nonoverlapping as _;
        assert_eq!(l0.sub_levels.len(), 3);
        let mut levels = vec![
            Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(3, 1, 0, 100, 1),
                    generate_table(4, 2, 2000, 3000, 1),
                ],
                total_file_size: 0,
                sub_level_id: 0,
            },
            Level {
                level_idx: 2,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(1, 1, 0, 100, 1),
                    generate_table(2, 2, 2000, 3000, 1),
                ],
                total_file_size: 0,
                sub_level_id: 0,
            },
        ];
        // Set internal_table_ids.
        assert_eq!(levels.len(), 2);
        for iter in [l0.sub_levels.iter_mut(), levels.iter_mut()] {
            for (idx, l) in iter.enumerate() {
                for t in &mut l.table_infos {
                    if idx == 0 {
                        t.table_ids.push(((t.id % 2) + 1) as _);
                    } else {
                        t.table_ids.push(3);
                    }
                }
            }
        }
        let levels = Levels {
            levels,
            l0: Some(l0),
        };

        let levels_handler = vec![
            LevelHandler::new(0),
            LevelHandler::new(1),
            LevelHandler::new(2),
        ];
        (levels, levels_handler)
    }

    #[test]
    fn test_l0_empty() {
        let l0 = generate_l0_nonoverlapping_sublevels(vec![]);
        let levels = vec![Level {
            level_idx: 1,
            level_type: LevelType::Nonoverlapping as i32,
            table_infos: vec![],
            total_file_size: 0,
            sub_level_id: 0,
        }];
        let levels = Levels {
            levels,
            l0: Some(l0),
        };
        let levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];
        let option = ManualCompactionOption {
            sst_ids: vec![1],
            level: 0,
            key_range: KeyRange {
                left: vec![],
                right: vec![],
                inf: true,
            },
            internal_table_id: HashSet::default(),
        };
        let picker =
            ManualCompactionPicker::new(Arc::new(RangeOverlapStrategy::default()), option, 0);
        assert!(picker.pick_compaction(&levels, &levels_handler).is_none());
    }

    #[test]
    fn test_l0_basic() {
        let (levels, levels_handler) = generate_test_levels();

        // target_level == 0, None
        let option = ManualCompactionOption {
            sst_ids: vec![],
            level: 0,
            key_range: KeyRange {
                left: vec![],
                right: vec![],
                inf: true,
            },
            internal_table_id: HashSet::default(),
        };
        let picker = ManualCompactionPicker::new(
            Arc::new(RangeOverlapStrategy::default()),
            option.clone(),
            0,
        );
        assert!(picker.pick_compaction(&levels, &levels_handler).is_none());

        // pick_l0_to_base_level
        let picker =
            ManualCompactionPicker::new(Arc::new(RangeOverlapStrategy::default()), option, 1);
        let mut expected = vec![vec![5, 6], vec![7, 8], vec![9, 10]];
        expected.reverse();
        let result = picker.pick_compaction(&levels, &levels_handler).unwrap();
        assert_eq!(result.input_levels.len(), 4);
        assert!(is_l0_to_lbase(&result));
        assert_eq!(result.target_level, 1);
        for (l, e) in expected.iter().enumerate().take(3) {
            assert_eq!(
                result.input_levels[l]
                    .table_infos
                    .iter()
                    .map(|s| s.id)
                    .collect_vec(),
                *e
            );
        }
        assert_eq!(
            result.input_levels[3].table_infos,
            vec![levels.levels[0].table_infos[0].clone()]
        );

        // pick_l0_to_base_level, filtered by key_range
        let option = ManualCompactionOption {
            sst_ids: vec![],
            level: 0,
            key_range: KeyRange {
                left: iterator_test_key_of_epoch(1, 0, 2),
                right: iterator_test_key_of_epoch(1, 200, 2),
                inf: false,
            },
            internal_table_id: HashSet::default(),
        };
        let picker =
            ManualCompactionPicker::new(Arc::new(RangeOverlapStrategy::default()), option, 1);
        let mut expected = vec![vec![5, 6], vec![7, 8]];
        expected.reverse();
        let result = picker.pick_compaction(&levels, &levels_handler).unwrap();
        assert_eq!(result.input_levels.len(), 3);
        assert!(is_l0_to_lbase(&result));
        assert_eq!(result.target_level, 1);
        for (l, e) in expected.iter().enumerate().take(2) {
            assert_eq!(
                result.input_levels[l]
                    .table_infos
                    .iter()
                    .map(|s| s.id)
                    .collect_vec(),
                *e
            );
        }
        assert_eq!(
            result.input_levels[2].table_infos,
            vec![levels.levels[0].table_infos[0].clone()]
        );
    }

    #[test]
    fn test_l0_to_l0_option_sst_ids() {
        let (levels, levels_handler) = generate_test_levels();
        // (input_level, sst_id_filter, expected_result_input_level_ssts)
        let sst_id_filters = vec![
            (0, vec![6], vec![vec![5, 6]]),
            (0, vec![7], vec![vec![7, 8]]),
            (0, vec![9], vec![vec![9, 10]]),
            (0, vec![6, 9], vec![vec![5, 6], vec![7, 8], vec![9, 10]]),
            (0, vec![8, 9], vec![vec![7, 8], vec![9, 10]]),
            (0, vec![6, 8, 9], vec![vec![5, 6], vec![7, 8], vec![9, 10]]),
        ];
        for (input_level, sst_id_filter, expected) in &sst_id_filters {
            let expected = expected.iter().rev().cloned().collect_vec();
            let option = ManualCompactionOption {
                sst_ids: sst_id_filter.clone(),
                level: *input_level as _,
                key_range: KeyRange {
                    left: vec![],
                    right: vec![],
                    inf: true,
                },
                internal_table_id: HashSet::default(),
            };
            let picker = ManualCompactionPicker::new(
                Arc::new(RangeOverlapStrategy::default()),
                option.clone(),
                // l0 to l0 will ignore target_level
                input_level + 1,
            );
            let result = picker.pick_compaction(&levels, &levels_handler).unwrap();
            assert!(is_l0_to_l0(&result));
            assert_eq!(result.input_levels.len(), expected.len());
            for (i, e) in expected.iter().enumerate().take(result.input_levels.len()) {
                assert_eq!(
                    result.input_levels[i]
                        .table_infos
                        .iter()
                        .map(|s| s.id)
                        .collect_vec(),
                    *e
                );
            }
        }
    }

    #[test]
    fn test_l0_to_lbase_option_internal_table() {
        let (levels, mut levels_handler) = generate_test_levels();
        let input_level = 0;
        let target_level = input_level + 1;
        {
            let option = ManualCompactionOption {
                sst_ids: vec![],
                level: input_level,
                key_range: KeyRange {
                    left: vec![],
                    right: vec![],
                    inf: true,
                },
                // No matching internal table id.
                internal_table_id: HashSet::from([100]),
            };
            let picker = ManualCompactionPicker::new(
                Arc::new(RangeOverlapStrategy::default()),
                option,
                target_level,
            );
            assert!(picker.pick_compaction(&levels, &levels_handler).is_none())
        }

        {
            let option = ManualCompactionOption {
                sst_ids: vec![],
                level: input_level,
                key_range: KeyRange {
                    left: vec![],
                    right: vec![],
                    inf: true,
                },
                // Include all sub level's table ids
                internal_table_id: HashSet::from([1, 2, 3]),
            };
            let picker = ManualCompactionPicker::new(
                Arc::new(RangeOverlapStrategy::default()),
                option,
                target_level,
            );
            let result = picker.pick_compaction(&levels, &levels_handler).unwrap();
            assert_eq!(result.input_levels.len(), 4);
            assert!(is_l0_to_lbase(&result));
            assert_eq!(result.target_level, 1);
            assert!(is_l0_to_lbase(&result));
            assert_eq!(
                result
                    .input_levels
                    .iter()
                    .take(3)
                    .flat_map(|s| s.table_infos.clone())
                    .map(|s| s.id)
                    .collect_vec(),
                vec![9, 10, 7, 8, 5, 6]
            );
            assert_eq!(
                result.input_levels[3]
                    .table_infos
                    .iter()
                    .map(|s| s.id)
                    .collect_vec(),
                vec![3]
            );
        }

        {
            let option = ManualCompactionOption {
                sst_ids: vec![],
                level: input_level,
                key_range: KeyRange {
                    left: vec![],
                    right: vec![],
                    inf: true,
                },
                // Only include bottom sub level's table id
                internal_table_id: HashSet::from([3]),
            };
            let picker = ManualCompactionPicker::new(
                Arc::new(RangeOverlapStrategy::default()),
                option,
                target_level,
            );
            let result = picker.pick_compaction(&levels, &levels_handler).unwrap();
            assert_eq!(result.input_levels.len(), 4);
            assert!(is_l0_to_lbase(&result));
            assert_eq!(
                result
                    .input_levels
                    .iter()
                    .take(3)
                    .flat_map(|s| s.table_infos.clone())
                    .map(|s| s.id)
                    .collect_vec(),
                vec![9, 10, 7, 8, 5, 6]
            );
            assert_eq!(
                result.input_levels[3]
                    .table_infos
                    .iter()
                    .map(|s| s.id)
                    .collect_vec(),
                vec![3]
            );
            assert_eq!(result.target_level, 1);
        }

        {
            let option = ManualCompactionOption {
                sst_ids: vec![],
                level: input_level,
                key_range: KeyRange {
                    left: vec![],
                    right: vec![],
                    inf: true,
                },
                // Only include partial top sub level's table id, but the whole top sub level is
                // picked.
                internal_table_id: HashSet::from([1]),
            };
            let picker = ManualCompactionPicker::new(
                Arc::new(RangeOverlapStrategy::default()),
                option,
                target_level,
            );
            let result = picker.pick_compaction(&levels, &levels_handler).unwrap();
            result.add_pending_task(0, &mut levels_handler);
            assert_eq!(result.input_levels.len(), 2);
            assert!(is_l0_to_lbase(&result));
            assert_eq!(result.target_level, 1);
            assert_eq!(
                result
                    .input_levels
                    .iter()
                    .take(1)
                    .flat_map(|s| s.table_infos.clone())
                    .map(|s| s.id)
                    .collect_vec(),
                vec![5, 6]
            );
            assert_eq!(
                result.input_levels[1]
                    .table_infos
                    .iter()
                    .map(|s| s.id)
                    .collect_vec(),
                vec![3]
            );

            // Pick bottom sub level while top sub level is pending
            let option = ManualCompactionOption {
                sst_ids: vec![],
                level: input_level,
                key_range: KeyRange {
                    left: vec![],
                    right: vec![],
                    inf: true,
                },
                // Only include bottom sub level's table id
                internal_table_id: HashSet::from([3]),
            };
            let picker = ManualCompactionPicker::new(
                Arc::new(RangeOverlapStrategy::default()),
                option,
                target_level,
            );
            // Because top sub-level is pending.
            assert!(picker.pick_compaction(&levels, &levels_handler).is_none());

            clean_task_state(&mut levels_handler[0]);
            clean_task_state(&mut levels_handler[1]);
        }
    }

    #[test]
    fn test_ln_to_lnext_option_internal_table() {
        let (levels, levels_handler) = generate_test_levels();
        let input_level = 1;
        let target_level = input_level + 1;
        {
            let option = ManualCompactionOption {
                sst_ids: vec![],
                level: input_level,
                key_range: KeyRange {
                    left: vec![],
                    right: vec![],
                    inf: true,
                },
                // No matching internal table id.
                internal_table_id: HashSet::from([100]),
            };
            let picker = ManualCompactionPicker::new(
                Arc::new(RangeOverlapStrategy::default()),
                option,
                target_level,
            );
            assert!(picker.pick_compaction(&levels, &levels_handler).is_none())
        }

        {
            let expected_input_level_sst_ids = vec![vec![4], vec![2]];
            let option = ManualCompactionOption {
                sst_ids: vec![],
                level: input_level,
                key_range: KeyRange {
                    left: vec![],
                    right: vec![],
                    inf: true,
                },
                // Only include partial input level's table id
                internal_table_id: HashSet::from([1]),
            };
            let picker = ManualCompactionPicker::new(
                Arc::new(RangeOverlapStrategy::default()),
                option,
                target_level,
            );
            let result = picker.pick_compaction(&levels, &levels_handler).unwrap();
            assert_eq!(
                result.input_levels.len(),
                expected_input_level_sst_ids.len()
            );
            assert_eq!(result.target_level, target_level);
            for (l, e) in expected_input_level_sst_ids
                .iter()
                .enumerate()
                .take(result.input_levels.len())
            {
                assert_eq!(
                    result.input_levels[l]
                        .table_infos
                        .iter()
                        .map(|s| s.id)
                        .collect_vec(),
                    *e
                );
            }
        }
    }

    #[test]
    fn test_ln_to_lnext_option_sst_ids() {
        let (levels, levels_handler) = generate_test_levels();
        // (input_level, sst_id_filter, expected_result_input_level_ssts)
        let sst_id_filters = vec![
            (1, vec![3], vec![vec![3], vec![1]]),
            (1, vec![4], vec![vec![4], vec![2]]),
            (1, vec![3, 4], vec![vec![3, 4], vec![1, 2]]),
        ];
        for (input_level, sst_id_filter, expected) in &sst_id_filters {
            let option = ManualCompactionOption {
                sst_ids: sst_id_filter.clone(),
                level: *input_level as _,
                key_range: KeyRange {
                    left: vec![],
                    right: vec![],
                    inf: true,
                },
                internal_table_id: HashSet::default(),
            };
            let picker = ManualCompactionPicker::new(
                Arc::new(RangeOverlapStrategy::default()),
                option.clone(),
                input_level + 1,
            );
            let result = picker.pick_compaction(&levels, &levels_handler).unwrap();
            assert_eq!(result.input_levels.len(), expected.len());
            for (i, e) in expected.iter().enumerate().take(result.input_levels.len()) {
                assert_eq!(
                    result.input_levels[i]
                        .table_infos
                        .iter()
                        .map(|s| s.id)
                        .collect_vec(),
                    *e
                );
            }
        }
    }

    #[test]
    fn test_ln_to_ln() {
        let (levels, levels_handler) = generate_test_levels();
        // (input_level, sst_id_filter, expected_result_input_level_ssts)
        let sst_id_filters = vec![
            (1, vec![3], vec![vec![3], vec![]]),
            (1, vec![4], vec![vec![4], vec![]]),
            (1, vec![3, 4], vec![vec![3, 4], vec![]]),
        ];
        for (input_level, sst_id_filter, expected) in &sst_id_filters {
            let option = ManualCompactionOption {
                sst_ids: sst_id_filter.clone(),
                level: *input_level as _,
                key_range: KeyRange {
                    left: vec![],
                    right: vec![],
                    inf: true,
                },
                internal_table_id: HashSet::default(),
            };
            let picker = ManualCompactionPicker::new(
                Arc::new(RangeOverlapStrategy::default()),
                option.clone(),
                *input_level as _,
            );
            let result = picker.pick_compaction(&levels, &levels_handler).unwrap();
            assert_eq!(result.input_levels.len(), expected.len());
            for (i, e) in expected.iter().enumerate().take(result.input_levels.len()) {
                assert_eq!(
                    result.input_levels[i]
                        .table_infos
                        .iter()
                        .map(|s| s.id)
                        .collect_vec(),
                    *e
                );
            }
        }
    }
}

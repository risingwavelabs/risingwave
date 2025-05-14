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

use std::collections::HashSet;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_hummock_sdk::HummockSstableId;
use risingwave_hummock_sdk::level::{InputLevel, Level, Levels, OverlappingLevel};
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_pb::hummock::LevelType;

use super::{CompactionInput, CompactionPicker, LocalPickerStatistic};
use crate::hummock::compaction::overlap_strategy::{
    OverlapInfo, OverlapStrategy, RangeOverlapInfo,
};
use crate::hummock::compaction::selector::ManualCompactionOption;
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
            ..Default::default()
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
        let l0 = &levels.l0;
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
                info.update(&table.key_range);
            }
            input_levels.push(InputLevel {
                level_idx: 0,
                level_type: l0.sub_levels[idx].level_type,
                table_infos: l0.sub_levels[idx].table_infos.clone(),
            })
        }
        let target_input_ssts_range =
            info.check_multiple_overlap(&levels.levels[self.target_level - 1].table_infos);
        let target_input_ssts = if target_input_ssts_range.is_empty() {
            vec![]
        } else {
            levels.levels[self.target_level - 1].table_infos[target_input_ssts_range].to_vec()
        };
        if target_input_ssts
            .iter()
            .any(|table| level_handlers[self.target_level].is_pending_compact(&table.sst_id))
        {
            return None;
        }
        if input_levels.is_empty() {
            return None;
        }
        input_levels.reverse();
        input_levels.push(InputLevel {
            level_idx: self.target_level as u32,
            level_type: LevelType::Nonoverlapping,
            table_infos: target_input_ssts,
        });

        Some(CompactionInput {
            input_levels,
            target_level: self.target_level,
            target_sub_level_id: 0,
            ..Default::default()
        })
    }

    /// Returns false if the given `sst` is rejected by filter defined by `option`.
    /// Otherwise returns true.
    fn filter_level_by_option(&self, level: &Level) -> bool {
        let mut hint_sst_ids: HashSet<HummockSstableId> = HashSet::new();
        hint_sst_ids.extend(self.option.sst_ids.iter());
        if self
            .overlap_strategy
            .check_overlap_with_range(&self.option.key_range, &level.table_infos)
            .is_empty()
        {
            return false;
        }
        if !hint_sst_ids.is_empty()
            && !level
                .table_infos
                .iter()
                .any(|t| hint_sst_ids.contains(&t.sst_id))
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
        &mut self,
        levels: &Levels,
        level_handlers: &[LevelHandler],
        _stats: &mut LocalPickerStatistic,
    ) -> Option<CompactionInput> {
        if self.option.level == 0 {
            if !self.option.sst_ids.is_empty() {
                return self.pick_l0_to_sub_level(&levels.l0, level_handlers);
            } else if self.target_level > 0 {
                return self.pick_l0_to_base_level(levels, level_handlers);
            } else {
                return None;
            }
        }
        let mut hint_sst_ids: HashSet<HummockSstableId> = HashSet::new();
        hint_sst_ids.extend(self.option.sst_ids.iter());
        let mut range_overlap_info = RangeOverlapInfo::default();
        range_overlap_info.update(&self.option.key_range);
        let level = self.option.level;
        let target_level = self.target_level;
        assert!(
            self.option.level == self.target_level || self.option.level + 1 == self.target_level
        );
        // We either include all `select_input_ssts` as input, or return None.
        let mut select_input_ssts: Vec<SstableInfo> = levels
            .get_level(self.option.level)
            .table_infos
            .iter()
            .filter(|sst_info| hint_sst_ids.is_empty() || hint_sst_ids.contains(&sst_info.sst_id))
            .filter(|sst_info| range_overlap_info.check_overlap(sst_info))
            .filter(|sst_info| {
                if self.option.internal_table_id.is_empty() {
                    return true;
                }

                // to filter sst_file by table_id
                for table_id in &sst_info.table_ids {
                    if self.option.internal_table_id.contains(table_id) {
                        return true;
                    }
                }
                false
            })
            .cloned()
            .collect();
        if select_input_ssts.is_empty() {
            return None;
        }
        let target_input_ssts = if target_level == level {
            // For intra level compaction, input SSTs must be consecutive.
            let (left, _) = levels
                .get_level(level)
                .table_infos
                .iter()
                .find_position(|p| p.sst_id == select_input_ssts.first().unwrap().sst_id)
                .unwrap();
            let (right, _) = levels
                .get_level(level)
                .table_infos
                .iter()
                .find_position(|p| p.sst_id == select_input_ssts.last().unwrap().sst_id)
                .unwrap();
            select_input_ssts = levels.get_level(level).table_infos[left..=right].to_vec();
            vec![]
        } else {
            self.overlap_strategy.check_base_level_overlap(
                &select_input_ssts,
                &levels.get_level(target_level).table_infos,
            )
        };
        if select_input_ssts
            .iter()
            .any(|table| level_handlers[level].is_pending_compact(&table.sst_id))
        {
            return None;
        }
        if target_input_ssts
            .iter()
            .any(|table| level_handlers[target_level].is_pending_compact(&table.sst_id))
        {
            return None;
        }

        Some(CompactionInput {
            select_input_size: select_input_ssts.iter().map(|sst| sst.sst_size).sum(),
            target_input_size: target_input_ssts.iter().map(|sst| sst.sst_size).sum(),
            total_file_count: (select_input_ssts.len() + target_input_ssts.len()) as u64,
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
            ..Default::default()
        })
    }
}

#[cfg(test)]
pub mod tests {
    use std::collections::{BTreeSet, HashMap};

    use bytes::Bytes;
    use risingwave_hummock_sdk::key_range::KeyRange;
    use risingwave_hummock_sdk::version::HummockVersionStateTableInfo;
    use risingwave_pb::hummock::compact_task;

    use super::*;
    use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
    use crate::hummock::compaction::overlap_strategy::RangeOverlapStrategy;
    use crate::hummock::compaction::selector::tests::{
        assert_compaction_task, generate_l0_nonoverlapping_sublevels,
        generate_l0_overlapping_sublevels, generate_level, generate_table,
    };
    use crate::hummock::compaction::selector::{CompactionSelector, ManualCompactionSelector};
    use crate::hummock::compaction::{CompactionDeveloperConfig, LocalSelectorStatistic};
    use crate::hummock::model::CompactionGroup;
    use crate::hummock::test_utils::{compaction_selector_context, iterator_test_key_of_epoch};

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
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![
                    generate_table(0, 1, 0, 100, 1),
                    generate_table(1, 1, 101, 200, 1),
                    generate_table(2, 1, 222, 300, 1),
                ],
                ..Default::default()
            },
            Level {
                level_idx: 2,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![
                    generate_table(4, 1, 0, 100, 1),
                    generate_table(5, 1, 101, 150, 1),
                    generate_table(6, 1, 151, 201, 1),
                    generate_table(7, 1, 501, 800, 1),
                    generate_table(8, 2, 301, 400, 1),
                ],
                ..Default::default()
            },
        ];
        let mut levels = Levels {
            levels,
            l0: generate_l0_nonoverlapping_sublevels(vec![]),
            ..Default::default()
        };
        let mut levels_handler = vec![
            LevelHandler::new(0),
            LevelHandler::new(1),
            LevelHandler::new(2),
        ];
        let mut local_stats = LocalPickerStatistic::default();

        {
            // test key_range option
            let option = ManualCompactionOption {
                level: 1,
                key_range: KeyRange {
                    left: Bytes::from(iterator_test_key_of_epoch(1, 0, 1)),
                    right: Bytes::from(iterator_test_key_of_epoch(1, 201, 1)),
                    right_exclusive: false,
                },
                ..Default::default()
            };

            let target_level = option.level + 1;
            let mut picker = ManualCompactionPicker::new(
                Arc::new(RangeOverlapStrategy::default()),
                option,
                target_level,
            );
            let result = picker
                .pick_compaction(&levels, &levels_handler, &mut local_stats)
                .unwrap();
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
            let mut picker = ManualCompactionPicker::new(
                Arc::new(RangeOverlapStrategy::default()),
                option,
                target_level,
            );
            let result = picker
                .pick_compaction(&levels, &levels_handler, &mut local_stats)
                .unwrap();
            result.add_pending_task(0, &mut levels_handler);

            assert_eq!(3, result.input_levels[0].table_infos.len());
            assert_eq!(3, result.input_levels[1].table_infos.len());
        }

        {
            clean_task_state(&mut levels_handler[1]);
            clean_task_state(&mut levels_handler[2]);

            let level_table_info = &mut levels.levels[0].table_infos;
            let table_info_1 = &mut level_table_info[1];
            let mut t_inner = table_info_1.get_inner();
            t_inner.table_ids.resize(2, 0);
            t_inner.table_ids[0] = 1;
            t_inner.table_ids[1] = 2;
            *table_info_1 = t_inner.into();

            // test internal_table_id
            let option = ManualCompactionOption {
                level: 1,
                internal_table_id: HashSet::from([2]),
                ..Default::default()
            };

            let target_level = option.level + 1;
            let mut picker = ManualCompactionPicker::new(
                Arc::new(RangeOverlapStrategy::default()),
                option,
                target_level,
            );

            let result = picker
                .pick_compaction(&levels, &levels_handler, &mut local_stats)
                .unwrap();
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
                let mut t_inner = table_info.get_inner();
                t_inner.table_ids.resize(2, 0);
                t_inner.table_ids[0] = 1;
                t_inner.table_ids[1] = 2;
                *table_info = t_inner.into();
            }

            // test key range filter first
            let option = ManualCompactionOption {
                sst_ids: vec![],
                level: 1,
                key_range: KeyRange {
                    left: Bytes::from(iterator_test_key_of_epoch(1, 101, 1)),
                    right: Bytes::from(iterator_test_key_of_epoch(1, 199, 1)),
                    right_exclusive: false,
                },
                internal_table_id: HashSet::from([2]),
            };

            let target_level = option.level + 1;
            let mut picker = ManualCompactionPicker::new(
                Arc::new(RangeOverlapStrategy::default()),
                option,
                target_level,
            );

            let result = picker
                .pick_compaction(&levels, &levels_handler, &mut local_stats)
                .unwrap();

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
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![
                    generate_table(3, 1, 0, 100, 1),
                    generate_table(4, 2, 2000, 3000, 1),
                ],
                ..Default::default()
            },
            Level {
                level_idx: 2,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![
                    generate_table(1, 1, 0, 100, 1),
                    generate_table(2, 2, 2000, 3000, 1),
                ],
                ..Default::default()
            },
        ];
        // Set internal_table_ids.
        assert_eq!(levels.len(), 2);
        for iter in [l0.sub_levels.iter_mut(), levels.iter_mut()] {
            for (idx, l) in iter.enumerate() {
                for t in &mut l.table_infos {
                    let mut t_inner = t.get_inner();
                    t_inner.table_ids.clear();
                    if idx == 0 {
                        t_inner.table_ids.push(((t.sst_id.inner() % 2) + 1) as _);
                    } else {
                        t_inner.table_ids.push(3);
                    }
                    *t = t_inner.into();
                }
            }
        }
        let levels = Levels {
            levels,
            l0,
            ..Default::default()
        };

        let levels_handler = vec![
            LevelHandler::new(0),
            LevelHandler::new(1),
            LevelHandler::new(2),
        ];
        (levels, levels_handler)
    }

    fn generate_intra_test_levels() -> (Levels, Vec<LevelHandler>) {
        let l0 = generate_l0_overlapping_sublevels(vec![]);
        let levels = vec![Level {
            level_idx: 1,
            level_type: LevelType::Nonoverlapping,
            table_infos: vec![
                generate_table(1, 1, 0, 100, 1),
                generate_table(2, 2, 100, 200, 1),
                generate_table(3, 2, 200, 300, 1),
                generate_table(4, 2, 300, 400, 1),
            ],
            ..Default::default()
        }];
        let levels = Levels {
            levels,
            l0,
            ..Default::default()
        };

        let levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];
        (levels, levels_handler)
    }

    #[test]
    fn test_l0_empty() {
        let l0 = generate_l0_nonoverlapping_sublevels(vec![]);
        let levels = vec![Level {
            level_idx: 1,
            level_type: LevelType::Nonoverlapping,
            table_infos: vec![],
            total_file_size: 0,
            sub_level_id: 0,
            uncompressed_file_size: 0,
            ..Default::default()
        }];
        let levels = Levels {
            levels,
            l0,
            ..Default::default()
        };
        let levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];
        let option = ManualCompactionOption {
            sst_ids: vec![1.into()],
            level: 0,
            key_range: KeyRange {
                left: Bytes::default(),
                right: Bytes::default(),
                right_exclusive: false,
            },
            internal_table_id: HashSet::default(),
        };
        let mut picker =
            ManualCompactionPicker::new(Arc::new(RangeOverlapStrategy::default()), option, 0);
        assert!(
            picker
                .pick_compaction(
                    &levels,
                    &levels_handler,
                    &mut LocalPickerStatistic::default()
                )
                .is_none()
        );
    }

    #[test]
    fn test_l0_basic() {
        let (levels, levels_handler) = generate_test_levels();

        // target_level == 0, None
        let option = ManualCompactionOption {
            sst_ids: vec![],
            level: 0,
            key_range: KeyRange {
                left: Bytes::default(),
                right: Bytes::default(),
                right_exclusive: false,
            },
            internal_table_id: HashSet::default(),
        };
        let mut picker = ManualCompactionPicker::new(
            Arc::new(RangeOverlapStrategy::default()),
            option.clone(),
            0,
        );
        let mut local_stats = LocalPickerStatistic::default();
        assert!(
            picker
                .pick_compaction(&levels, &levels_handler, &mut local_stats)
                .is_none()
        );

        // pick_l0_to_base_level
        let mut picker =
            ManualCompactionPicker::new(Arc::new(RangeOverlapStrategy::default()), option, 1);
        let mut expected = [vec![5, 6], vec![7, 8], vec![9, 10]];
        expected.reverse();
        let result = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        assert_eq!(result.input_levels.len(), 4);
        assert!(is_l0_to_lbase(&result));
        assert_eq!(result.target_level, 1);
        for (l, e) in expected.iter().enumerate().take(3) {
            assert_eq!(
                result.input_levels[l]
                    .table_infos
                    .iter()
                    .map(|s| s.sst_id)
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
                left: Bytes::from(iterator_test_key_of_epoch(1, 0, 2)),
                right: Bytes::from(iterator_test_key_of_epoch(1, 200, 2)),
                right_exclusive: false,
            },
            internal_table_id: HashSet::default(),
        };
        let mut picker =
            ManualCompactionPicker::new(Arc::new(RangeOverlapStrategy::default()), option, 1);
        let mut expected = [vec![5, 6], vec![7, 8]];
        expected.reverse();
        let result = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        assert_eq!(result.input_levels.len(), 3);
        assert!(is_l0_to_lbase(&result));
        assert_eq!(result.target_level, 1);
        for (l, e) in expected.iter().enumerate().take(2) {
            assert_eq!(
                result.input_levels[l]
                    .table_infos
                    .iter()
                    .map(|s| s.sst_id)
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
        let mut local_stats = LocalPickerStatistic::default();
        for (input_level, sst_id_filter, expected) in &sst_id_filters {
            let expected = expected.iter().rev().cloned().collect_vec();
            let option = ManualCompactionOption {
                sst_ids: sst_id_filter.iter().cloned().map(Into::into).collect(),
                level: *input_level as _,
                key_range: KeyRange {
                    left: Bytes::default(),
                    right: Bytes::default(),
                    right_exclusive: false,
                },
                internal_table_id: HashSet::default(),
            };
            let mut picker = ManualCompactionPicker::new(
                Arc::new(RangeOverlapStrategy::default()),
                option.clone(),
                // l0 to l0 will ignore target_level
                input_level + 1,
            );
            let result = picker
                .pick_compaction(&levels, &levels_handler, &mut local_stats)
                .unwrap();
            assert!(is_l0_to_l0(&result));
            assert_eq!(result.input_levels.len(), expected.len());
            for (i, e) in expected.iter().enumerate().take(result.input_levels.len()) {
                assert_eq!(
                    result.input_levels[i]
                        .table_infos
                        .iter()
                        .map(|s| s.sst_id)
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
        let mut local_stats = LocalPickerStatistic::default();
        {
            let option = ManualCompactionOption {
                sst_ids: vec![],
                level: input_level,
                key_range: KeyRange {
                    left: Bytes::default(),
                    right: Bytes::default(),
                    right_exclusive: false,
                },
                // No matching internal table id.
                internal_table_id: HashSet::from([100]),
            };
            let mut picker = ManualCompactionPicker::new(
                Arc::new(RangeOverlapStrategy::default()),
                option,
                target_level,
            );
            assert!(
                picker
                    .pick_compaction(&levels, &levels_handler, &mut local_stats)
                    .is_none()
            )
        }

        {
            let option = ManualCompactionOption {
                sst_ids: vec![],
                level: input_level,
                key_range: KeyRange {
                    left: Bytes::default(),
                    right: Bytes::default(),
                    right_exclusive: false,
                },
                // Include all sub level's table ids
                internal_table_id: HashSet::from([1, 2, 3]),
            };
            let mut picker = ManualCompactionPicker::new(
                Arc::new(RangeOverlapStrategy::default()),
                option,
                target_level,
            );
            let result = picker
                .pick_compaction(&levels, &levels_handler, &mut local_stats)
                .unwrap();
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
                    .map(|s| s.sst_id)
                    .collect_vec(),
                vec![9, 10, 7, 8, 5, 6]
            );
            assert_eq!(
                result.input_levels[3]
                    .table_infos
                    .iter()
                    .map(|s| s.sst_id)
                    .collect_vec(),
                vec![3]
            );
        }

        {
            let option = ManualCompactionOption {
                sst_ids: vec![],
                level: input_level,
                key_range: KeyRange {
                    left: Bytes::default(),
                    right: Bytes::default(),
                    right_exclusive: false,
                },
                // Only include bottom sub level's table id
                internal_table_id: HashSet::from([3]),
            };
            let mut picker = ManualCompactionPicker::new(
                Arc::new(RangeOverlapStrategy::default()),
                option,
                target_level,
            );
            let result = picker
                .pick_compaction(&levels, &levels_handler, &mut local_stats)
                .unwrap();
            assert_eq!(result.input_levels.len(), 4);
            assert!(is_l0_to_lbase(&result));
            assert_eq!(
                result
                    .input_levels
                    .iter()
                    .take(3)
                    .flat_map(|s| s.table_infos.clone())
                    .map(|s| s.sst_id)
                    .collect_vec(),
                vec![9, 10, 7, 8, 5, 6]
            );
            assert_eq!(
                result.input_levels[3]
                    .table_infos
                    .iter()
                    .map(|s| s.sst_id)
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
                    left: Bytes::default(),
                    right: Bytes::default(),
                    right_exclusive: false,
                },
                // Only include partial top sub level's table id, but the whole top sub level is
                // picked.
                internal_table_id: HashSet::from([1]),
            };
            let mut picker = ManualCompactionPicker::new(
                Arc::new(RangeOverlapStrategy::default()),
                option,
                target_level,
            );
            let result = picker
                .pick_compaction(&levels, &levels_handler, &mut local_stats)
                .unwrap();
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
                    .map(|s| s.sst_id)
                    .collect_vec(),
                vec![5, 6]
            );
            assert_eq!(
                result.input_levels[1]
                    .table_infos
                    .iter()
                    .map(|s| s.sst_id)
                    .collect_vec(),
                vec![3]
            );

            // Pick bottom sub level while top sub level is pending
            let option = ManualCompactionOption {
                sst_ids: vec![],
                level: input_level,
                key_range: KeyRange {
                    left: Bytes::default(),
                    right: Bytes::default(),
                    right_exclusive: false,
                },
                // Only include bottom sub level's table id
                internal_table_id: HashSet::from([3]),
            };
            let mut picker = ManualCompactionPicker::new(
                Arc::new(RangeOverlapStrategy::default()),
                option,
                target_level,
            );
            // Because top sub-level is pending.
            assert!(
                picker
                    .pick_compaction(&levels, &levels_handler, &mut local_stats)
                    .is_none()
            );

            clean_task_state(&mut levels_handler[0]);
            clean_task_state(&mut levels_handler[1]);
        }
    }

    #[test]
    fn test_ln_to_lnext_option_internal_table() {
        let (levels, levels_handler) = generate_test_levels();
        let input_level = 1;
        let target_level = input_level + 1;
        let mut local_stats = LocalPickerStatistic::default();
        {
            let option = ManualCompactionOption {
                sst_ids: vec![],
                level: input_level,
                key_range: KeyRange {
                    left: Bytes::default(),
                    right: Bytes::default(),
                    right_exclusive: false,
                },
                // No matching internal table id.
                internal_table_id: HashSet::from([100]),
            };
            let mut picker = ManualCompactionPicker::new(
                Arc::new(RangeOverlapStrategy::default()),
                option,
                target_level,
            );
            assert!(
                picker
                    .pick_compaction(&levels, &levels_handler, &mut local_stats)
                    .is_none()
            )
        }

        {
            let expected_input_level_sst_ids = [vec![4], vec![2]];
            let option = ManualCompactionOption {
                sst_ids: vec![],
                level: input_level,
                key_range: KeyRange {
                    left: Bytes::default(),
                    right: Bytes::default(),
                    right_exclusive: false,
                },
                // Only include partial input level's table id
                internal_table_id: HashSet::from([1]),
            };
            let mut picker = ManualCompactionPicker::new(
                Arc::new(RangeOverlapStrategy::default()),
                option,
                target_level,
            );
            let result = picker
                .pick_compaction(&levels, &levels_handler, &mut local_stats)
                .unwrap();
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
                        .map(|s| s.sst_id)
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
        let mut local_stats = LocalPickerStatistic::default();
        for (input_level, sst_id_filter, expected) in &sst_id_filters {
            let option = ManualCompactionOption {
                sst_ids: sst_id_filter.iter().cloned().map(Into::into).collect(),
                level: *input_level as _,
                key_range: KeyRange {
                    left: Bytes::default(),
                    right: Bytes::default(),
                    right_exclusive: false,
                },
                internal_table_id: HashSet::default(),
            };
            let mut picker = ManualCompactionPicker::new(
                Arc::new(RangeOverlapStrategy::default()),
                option.clone(),
                input_level + 1,
            );
            let result = picker
                .pick_compaction(&levels, &levels_handler, &mut local_stats)
                .unwrap();
            assert_eq!(result.input_levels.len(), expected.len());
            for (i, e) in expected.iter().enumerate().take(result.input_levels.len()) {
                assert_eq!(
                    result.input_levels[i]
                        .table_infos
                        .iter()
                        .map(|s| s.sst_id)
                        .collect_vec(),
                    *e
                );
            }
        }
    }

    #[test]
    fn test_ln_to_ln() {
        let (levels, levels_handler) = generate_intra_test_levels();
        // (input_level, sst_id_filter, expected_result_input_level_ssts)
        let sst_id_filters = vec![
            (1, vec![1], vec![vec![1], vec![]]),
            (1, vec![3], vec![vec![3], vec![]]),
            (1, vec![4], vec![vec![4], vec![]]),
            (1, vec![3, 4], vec![vec![3, 4], vec![]]),
            (1, vec![1, 4], vec![vec![1, 2, 3, 4], vec![]]),
            (1, vec![2, 4], vec![vec![2, 3, 4], vec![]]),
            (1, vec![1, 3], vec![vec![1, 2, 3], vec![]]),
        ];
        for (input_level, sst_id_filter, expected) in &sst_id_filters {
            let option = ManualCompactionOption {
                sst_ids: sst_id_filter.iter().cloned().map(Into::into).collect(),
                level: *input_level as _,
                key_range: KeyRange {
                    left: Bytes::default(),
                    right: Bytes::default(),
                    right_exclusive: false,
                },
                internal_table_id: HashSet::default(),
            };
            let mut picker = ManualCompactionPicker::new(
                Arc::new(RangeOverlapStrategy::default()),
                option.clone(),
                *input_level as _,
            );
            let result = picker
                .pick_compaction(
                    &levels,
                    &levels_handler,
                    &mut LocalPickerStatistic::default(),
                )
                .unwrap();
            assert_eq!(result.input_levels.len(), expected.len());
            for (i, e) in expected.iter().enumerate().take(result.input_levels.len()) {
                assert_eq!(
                    result.input_levels[i]
                        .table_infos
                        .iter()
                        .map(|s| s.sst_id)
                        .collect_vec(),
                    *e
                );
            }
        }
    }

    #[test]
    fn test_manual_compaction_selector_l0() {
        let config = CompactionConfigBuilder::new().max_level(4).build();
        let group_config = CompactionGroup::new(1, config);
        let l0 = generate_l0_nonoverlapping_sublevels(vec![
            generate_table(0, 1, 0, 500, 1),
            generate_table(1, 1, 0, 500, 1),
        ]);
        assert_eq!(l0.sub_levels.len(), 2);
        let levels = vec![
            generate_level(1, vec![]),
            generate_level(2, vec![]),
            generate_level(3, vec![]),
            Level {
                level_idx: 4,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![
                    generate_table(2, 1, 0, 100, 1),
                    generate_table(3, 1, 101, 200, 1),
                    generate_table(4, 1, 222, 300, 1),
                ],
                ..Default::default()
            },
        ];
        assert_eq!(levels.len(), 4);
        let levels = Levels {
            levels,
            l0,
            ..Default::default()
        };
        let mut levels_handler = (0..5).map(LevelHandler::new).collect_vec();
        let mut local_stats = LocalSelectorStatistic::default();

        // pick_l0_to_sub_level
        {
            let option = ManualCompactionOption {
                sst_ids: [0, 1].iter().cloned().map(Into::into).collect(),
                key_range: KeyRange {
                    left: Bytes::default(),
                    right: Bytes::default(),
                    right_exclusive: false,
                },
                internal_table_id: HashSet::default(),
                level: 0,
            };
            let mut selector = ManualCompactionSelector::new(option);
            let task = selector
                .pick_compaction(
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
                )
                .unwrap();
            assert_compaction_task(&task, &levels_handler);
            assert_eq!(task.input.input_levels.len(), 2);
            assert_eq!(task.input.input_levels[0].level_idx, 0);
            assert_eq!(task.input.input_levels[1].level_idx, 0);
            assert_eq!(task.input.target_level, 0);
        }

        for level_handler in &mut levels_handler {
            for pending_task_id in &level_handler.pending_tasks_ids() {
                level_handler.remove_task(*pending_task_id);
            }
        }

        // pick_l0_to_base_level
        {
            let option = ManualCompactionOption {
                sst_ids: vec![],
                key_range: KeyRange {
                    left: Bytes::default(),
                    right: Bytes::default(),
                    right_exclusive: false,
                },
                internal_table_id: HashSet::default(),
                level: 0,
            };
            let mut selector = ManualCompactionSelector::new(option);
            let task = selector
                .pick_compaction(
                    2,
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
                )
                .unwrap();
            assert_compaction_task(&task, &levels_handler);
            assert_eq!(task.input.input_levels.len(), 3);
            assert_eq!(task.input.input_levels[0].level_idx, 0);
            assert_eq!(task.input.input_levels[1].level_idx, 0);
            assert_eq!(task.input.input_levels[2].level_idx, 4);
            assert_eq!(task.input.target_level, 4);
        }
    }

    /// tests `DynamicLevelSelector::manual_pick_compaction`
    #[test]
    fn test_manual_compaction_selector() {
        let config = CompactionConfigBuilder::new().max_level(4).build();
        let group_config = CompactionGroup::new(1, config);
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
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![
                    generate_table(2, 1, 0, 100, 1),
                    generate_table(3, 1, 101, 200, 1),
                    generate_table(4, 1, 222, 300, 1),
                    generate_table(5, 1, 333, 400, 1),
                    generate_table(6, 1, 444, 500, 1),
                    generate_table(7, 1, 555, 600, 1),
                ],
                ..Default::default()
            },
        ];
        assert_eq!(levels.len(), 4);
        let levels = Levels {
            levels,
            l0,
            ..Default::default()
        };
        let mut levels_handler = (0..5).map(LevelHandler::new).collect_vec();
        let mut local_stats = LocalSelectorStatistic::default();

        // pick l3 -> l4
        {
            let option = ManualCompactionOption {
                sst_ids: [0, 1].iter().cloned().map(Into::into).collect(),
                key_range: KeyRange {
                    left: Bytes::default(),
                    right: Bytes::default(),
                    right_exclusive: false,
                },
                internal_table_id: HashSet::default(),
                level: 3,
            };
            let mut selector = ManualCompactionSelector::new(option);
            let task = selector
                .pick_compaction(
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
                )
                .unwrap();
            assert_compaction_task(&task, &levels_handler);
            assert_eq!(task.input.input_levels.len(), 2);
            assert_eq!(task.input.input_levels[0].level_idx, 3);
            assert_eq!(task.input.input_levels[0].table_infos.len(), 2);
            assert_eq!(task.input.input_levels[1].level_idx, 4);
            assert_eq!(task.input.input_levels[1].table_infos.len(), 2);
            assert_eq!(task.input.target_level, 4);
        }

        for level_handler in &mut levels_handler {
            for pending_task_id in &level_handler.pending_tasks_ids() {
                level_handler.remove_task(*pending_task_id);
            }
        }

        // pick l4 -> l4
        {
            let option = ManualCompactionOption {
                sst_ids: vec![],
                key_range: KeyRange {
                    left: Bytes::default(),
                    right: Bytes::default(),
                    right_exclusive: false,
                },
                internal_table_id: HashSet::default(),
                level: 4,
            };
            let mut selector = ManualCompactionSelector::new(option);
            let task = selector
                .pick_compaction(
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
                )
                .unwrap();
            assert_compaction_task(&task, &levels_handler);
            assert_eq!(task.input.input_levels.len(), 2);
            assert_eq!(task.input.input_levels[0].level_idx, 4);
            assert_eq!(task.input.input_levels[0].table_infos.len(), 6);
            assert_eq!(task.input.input_levels[1].level_idx, 4);
            assert_eq!(task.input.input_levels[1].table_infos.len(), 0);
            assert_eq!(task.input.target_level, 4);
            assert!(matches!(
                task.compaction_task_type,
                compact_task::TaskType::Manual
            ));
        }
    }
}

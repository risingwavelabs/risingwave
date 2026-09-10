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

use std::cell::RefCell;
use std::collections::HashSet;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::config::meta::default::compaction_config;
use risingwave_hummock_sdk::level::{InputLevel, Level, Levels, OverlappingLevel};
use risingwave_pb::hummock::{CompactionConfig, LevelType};

use super::non_overlap_sub_level_picker::{NonOverlapSubLevelPicker, SubLevelSstables};
use super::{
    CompactionInput, CompactionPicker, CompactionTaskValidator, L0PickerMode, LocalPickerStatistic,
    PartitionL0GrowthOutcome, ValidationRuleType,
};
use crate::hummock::compaction::picker::TrivialMovePicker;
use crate::hummock::compaction::{CompactionDeveloperConfig, create_overlap_strategy};
use crate::hummock::level_handler::LevelHandler;

std::thread_local! {
    static LOG_COUNTER: RefCell<usize> = const { RefCell::new(0) };
}

pub struct LevelCompactionPicker {
    target_level: usize,
    mode: L0PickerMode,
    config: Arc<CompactionConfig>,
    compaction_task_validator: Arc<CompactionTaskValidator>,
    developer_config: Arc<CompactionDeveloperConfig>,
}

impl CompactionPicker for LevelCompactionPicker {
    fn pick_compaction(
        &mut self,
        levels: &Levels,
        level_handlers: &[LevelHandler],
        stats: &mut LocalPickerStatistic,
    ) -> Option<CompactionInput> {
        self.pick_compaction_with_output_conflict_check(levels, level_handlers, stats, |_| false)
    }
}

impl LevelCompactionPicker {
    /// The selector supplies its output-conflict check only for partition ToBase growth. The
    /// initial task still goes through the normal outer check; a failed growth never replaces it.
    pub(crate) fn pick_compaction_with_output_conflict_check(
        &mut self,
        levels: &Levels,
        level_handlers: &[LevelHandler],
        stats: &mut LocalPickerStatistic,
        has_output_conflict: impl Fn(&CompactionInput) -> bool,
    ) -> Option<CompactionInput> {
        let l0 = &levels.l0;
        if l0.sub_levels.is_empty() {
            return None;
        }
        if l0.sub_levels[0].level_type != LevelType::Nonoverlapping
            && l0.sub_levels[0].table_infos.len() > 1
        {
            stats.skip_by_overlapping += 1;
            return None;
        }

        let is_l0_pending_compact =
            level_handlers[0].is_level_all_pending_compact(&l0.sub_levels[0]);

        // Preserve the legacy early return. A partition-local view can contain a disjoint,
        // runnable stack in newer sub-levels; let the existing picker check its overlap closure.
        if is_l0_pending_compact && !self.mode.is_single_table_partition() {
            stats.skip_by_pending_files += 1;
            return None;
        }

        if let Some(mut ret) = self.pick_base_trivial_move(
            l0,
            levels.get_level(self.target_level),
            level_handlers,
            stats,
        ) {
            ret.vnode_partition_count = self.config.split_weight_by_vnode;
            stats.is_trivial_move = self.mode.is_single_table_partition();
            return Some(ret);
        }

        if self.mode.is_trivial_move_only() {
            return None;
        }

        debug_assert!(self.target_level == levels.get_level(self.target_level).level_idx as usize);
        if let Some(ret) = self.pick_multi_level_to_base(
            l0,
            levels.get_level(self.target_level),
            self.config.split_weight_by_vnode,
            level_handlers,
            stats,
            &has_output_conflict,
        ) {
            return Some(ret);
        }

        None
    }
}

impl LevelCompactionPicker {
    #[cfg(test)]
    pub fn new(
        target_level: usize,
        config: Arc<CompactionConfig>,
        developer_config: Arc<CompactionDeveloperConfig>,
    ) -> LevelCompactionPicker {
        LevelCompactionPicker {
            target_level,
            mode: L0PickerMode::Legacy,
            compaction_task_validator: Arc::new(CompactionTaskValidator::new(config.clone())),
            config,
            developer_config,
        }
    }

    pub fn new_with_validator(
        target_level: usize,
        config: Arc<CompactionConfig>,
        compaction_task_validator: Arc<CompactionTaskValidator>,
        developer_config: Arc<CompactionDeveloperConfig>,
    ) -> LevelCompactionPicker {
        Self::new_with_mode(
            target_level,
            config,
            compaction_task_validator,
            developer_config,
            L0PickerMode::Legacy,
        )
    }

    pub(crate) fn new_with_mode(
        target_level: usize,
        config: Arc<CompactionConfig>,
        compaction_task_validator: Arc<CompactionTaskValidator>,
        developer_config: Arc<CompactionDeveloperConfig>,
        mode: L0PickerMode,
    ) -> LevelCompactionPicker {
        LevelCompactionPicker {
            target_level,
            mode,
            config,
            compaction_task_validator,
            developer_config,
        }
    }

    fn pick_base_trivial_move(
        &self,
        l0: &OverlappingLevel,
        target_level: &Level,
        level_handlers: &[LevelHandler],
        stats: &mut LocalPickerStatistic,
    ) -> Option<CompactionInput> {
        if !self.developer_config.enable_trivial_move {
            return None;
        }

        let overlap_strategy = create_overlap_strategy(self.config.compaction_mode());
        let trivial_move_picker = TrivialMovePicker::new(
            0,
            self.target_level,
            overlap_strategy.clone(),
            if self.mode.is_single_table_partition() {
                0
            } else if self.compaction_task_validator.is_enable() {
                self.config.sst_allowed_trivial_move_min_size.unwrap_or(0)
            } else {
                0
            },
            self.config
                .sst_allowed_trivial_move_max_count
                .unwrap_or(compaction_config::sst_allowed_trivial_move_max_count())
                as usize,
        );

        trivial_move_picker.pick_trivial_move_task(
            &l0.sub_levels[0].table_infos,
            &target_level.table_infos,
            level_handlers,
            stats,
        )
    }

    fn pick_multi_level_to_base(
        &self,
        l0: &OverlappingLevel,
        target_level: &Level,
        vnode_partition_count: u32,
        level_handlers: &[LevelHandler],
        stats: &mut LocalPickerStatistic,
        has_output_conflict: &impl Fn(&CompactionInput) -> bool,
    ) -> Option<CompactionInput> {
        let overlap_strategy = create_overlap_strategy(self.config.compaction_mode());
        let max_l0_compaction_bytes = std::cmp::max(
            self.config.max_bytes_for_level_base,
            self.config.max_compaction_bytes / 2,
        );
        let max_l0_level_count =
            self.config
                .max_l0_compact_level_count
                .unwrap_or(compaction_config::max_l0_compact_level_count()) as usize;
        let min_expected_level_count = match self.mode {
            L0PickerMode::SingleTablePartition { min_l0_level_count } => min_l0_level_count.max(1),
            _ => 1,
        };
        let non_overlap_sub_level_picker = NonOverlapSubLevelPicker::new(
            if self.mode.is_single_table_partition() {
                0
            } else {
                self.config.sub_level_max_compaction_bytes
            },
            max_l0_compaction_bytes,
            min_expected_level_count,
            self.config.level0_max_compact_file_number,
            overlap_strategy.clone(),
            self.developer_config.enable_check_task_level_overlap,
            max_l0_level_count,
            self.config
                .enable_optimize_l0_interval_selection
                .unwrap_or(compaction_config::enable_optimize_l0_interval_selection()),
        );

        let candidate_levels = if self.mode.is_single_table_partition() {
            // The fixed view already validates vnode boundaries. Stop before an overlapping
            // sub-level: the non-overlapping closure builder must not cross that boundary.
            let count = l0
                .sub_levels
                .iter()
                .take_while(|level| level.level_type == LevelType::Nonoverlapping)
                .count();
            &l0.sub_levels[..count]
        } else {
            let mut max_vnode_partition_idx = 0;
            for (idx, level) in l0.sub_levels.iter().enumerate() {
                if level.vnode_partition_count < vnode_partition_count {
                    break;
                }
                max_vnode_partition_idx = idx;
            }
            &l0.sub_levels[..=max_vnode_partition_idx]
        };
        if candidate_levels.is_empty() {
            return None;
        }
        let candidate_l0_plans = non_overlap_sub_level_picker
            .pick_l0_multi_non_overlap_level(candidate_levels, &level_handlers[0]);
        if candidate_l0_plans.is_empty() {
            stats.skip_by_pending_files += 1;
            return None;
        }

        let mut skipped_pending_target = false;
        let mut candidates = candidate_l0_plans.into_iter();
        while let Some(input) = candidates.next() {
            // Partition depth is only admission pressure. A chosen range must independently
            // satisfy the depth contract, including after the legacy builder's truncation.
            if self.mode.is_single_table_partition()
                && input.sstable_infos.len() < min_expected_level_count
            {
                stats.skip_by_count_limit += 1;
                continue;
            }
            let l0_select_tables = input
                .sstable_infos
                .iter()
                .flat_map(|(_, select_tables)| select_tables.clone())
                .collect_vec();
            let target_level_files = overlap_strategy
                .check_base_level_overlap(&l0_select_tables, &target_level.table_infos);
            if target_level_files.iter().any(|sst| {
                level_handlers[target_level.level_idx as usize].is_pending_compact(&sst.sst_id)
            }) {
                skipped_pending_target = true;
                continue;
            }
            let target_input_size = target_level_files.iter().map(|sst| sst.sst_size).sum();
            let target_file_count = target_level_files.len();

            let mut input_levels = input
                .sstable_infos
                .into_iter()
                .map(|(_, table_infos)| InputLevel {
                    level_idx: 0,
                    level_type: LevelType::Nonoverlapping,
                    table_infos,
                })
                .collect_vec();
            input_levels.reverse();
            input_levels.push(InputLevel {
                level_idx: target_level.level_idx,
                level_type: target_level.level_type,
                table_infos: target_level_files,
            });

            let mut result = CompactionInput {
                input_levels,
                target_level: self.target_level,
                select_input_size: input.total_file_size,
                target_input_size,
                total_file_count: (input.total_file_count + target_file_count) as u64,
                vnode_partition_count,
                ..Default::default()
            };
            if !self.compaction_task_validator.valid_compact_task(
                &result,
                ValidationRuleType::ToBase,
                stats,
            ) {
                if l0.total_file_size > target_level.total_file_size * 8 {
                    let log_counter = LOG_COUNTER.with_borrow_mut(|counter| {
                        *counter += 1;
                        *counter
                    });
                    if log_counter.is_multiple_of(100) {
                        tracing::warn!(
                            "skip task with level count: {}, file count: {}, select size: {}, target size: {}, target level size: {}",
                            result.input_levels.len(),
                            result.total_file_count,
                            result.select_input_size,
                            result.target_input_size,
                            target_level.total_file_size,
                        );
                    }
                }
                continue;
            }
            if self.mode.is_single_table_partition() {
                stats.partition_l0_growth.initial_l0_size = result.select_input_size;
                // Reuse only the remaining, already correctness-closed plans. Donors need not
                // satisfy the initial seed depth. Do not grow moves or an output-conflicting seed.
                if target_file_count > 0 && !has_output_conflict(&result) {
                    for donor in candidates {
                        let outcome = match self.try_grow_partition_l0_input(
                            &result,
                            &donor,
                            candidate_levels,
                            target_level,
                        ) {
                            Ok(grown) if has_output_conflict(&grown) => {
                                PartitionL0GrowthOutcome::OutputConflict
                            }
                            Ok(grown) => {
                                result = grown;
                                PartitionL0GrowthOutcome::Accepted
                            }
                            Err(reason) => reason,
                        };
                        stats.partition_l0_growth.outcomes[outcome as usize] += 1;
                    }
                }
            }
            return Some(result);
        }

        if skipped_pending_target {
            stats.skip_by_pending_files += 1;
        }
        None
    }

    /// Union independently closed candidates from one fixed partition, preserving the Base set.
    /// This is optional amortization, not a new closure builder. Pending checks have already run
    /// on both L0 closures and on the frozen Base inputs. Do not verify the union as one L0 hull:
    /// disjoint valid closures may leave an unselected key-range hole between them.
    fn try_grow_partition_l0_input(
        &self,
        selected: &CompactionInput,
        donor: &SubLevelSstables,
        candidate_levels: &[Level],
        target_level: &Level,
    ) -> Result<CompactionInput, PartitionL0GrowthOutcome> {
        use PartitionL0GrowthOutcome as Rejected;
        let mut ids = selected
            .input_levels
            .iter()
            .filter(|level| level.level_idx == 0)
            .flat_map(|level| level.table_infos.iter().map(|sst| sst.sst_id))
            .collect::<HashSet<_>>();
        let old_count = ids.len();
        ids.extend(
            donor
                .sstable_infos
                .iter()
                .flat_map(|(_, ssts)| ssts.iter().map(|sst| sst.sst_id)),
        );
        if ids.len() == old_count {
            return Err(Rejected::NoNewSst);
        }
        if ids.len() as u64 > self.config.level0_max_compact_file_number {
            return Err(Rejected::Files);
        }
        let mut input_levels = vec![];
        let mut l0_size = 0;
        for level in candidate_levels {
            let table_infos = level
                .table_infos
                .iter()
                .filter(|sst| ids.contains(&sst.sst_id))
                .cloned()
                .collect_vec();
            if !table_infos.is_empty() {
                l0_size += table_infos.iter().map(|sst| sst.sst_size).sum::<u64>();
                input_levels.push(InputLevel {
                    level_idx: 0,
                    level_type: LevelType::Nonoverlapping,
                    table_infos,
                });
            }
        }
        let max_levels =
            self.config
                .max_l0_compact_level_count
                .unwrap_or(compaction_config::max_l0_compact_level_count()) as usize;
        if input_levels.len() > max_levels {
            return Err(Rejected::Levels);
        }
        let max_l0_bytes = self
            .config
            .max_bytes_for_level_base
            .max(self.config.max_compaction_bytes / 2);
        if l0_size > max_l0_bytes
            || l0_size.saturating_add(selected.target_input_size) > self.config.max_compaction_bytes
        {
            return Err(Rejected::Bytes);
        }
        let l0_ssts = input_levels
            .iter()
            .flat_map(|level| level.table_infos.iter().cloned())
            .collect_vec();
        let target_ssts = create_overlap_strategy(self.config.compaction_mode())
            .check_base_level_overlap(&l0_ssts, &target_level.table_infos);
        let selected_base = selected.input_levels.last().unwrap();
        if !target_ssts
            .iter()
            .map(|sst| sst.sst_id)
            .eq(selected_base.table_infos.iter().map(|sst| sst.sst_id))
        {
            return Err(Rejected::BaseSetChange);
        }
        input_levels.reverse();
        input_levels.push(selected_base.clone());
        Ok(CompactionInput {
            input_levels,
            target_level: self.target_level,
            select_input_size: l0_size,
            target_input_size: selected.target_input_size,
            total_file_count: (ids.len() + target_ssts.len()) as u64,
            vnode_partition_count: selected.vnode_partition_count,
            ..Default::default()
        })
    }
}

#[cfg(test)]
pub mod tests {
    use risingwave_common::util::iter_util::ZipEqFast;

    use super::*;
    use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
    use crate::hummock::compaction::selector::tests::*;
    use crate::hummock::compaction::{CompactionMode, TierCompactionPicker};

    fn create_compaction_picker_for_test() -> LevelCompactionPicker {
        let config = Arc::new(
            CompactionConfigBuilder::new()
                .level0_tier_compact_file_number(2)
                .level0_sub_level_compact_level_count(1)
                .build(),
        );
        LevelCompactionPicker::new(1, config, Arc::new(CompactionDeveloperConfig::default()))
    }

    #[test]
    fn test_small_trivial_moves_ignore_legacy_min_size_and_count_all_files() {
        let config = Arc::new(CompactionConfig {
            split_weight_by_vnode: 8,
            ..CompactionConfigBuilder::new()
                .sst_allowed_trivial_move_min_size(Some(u64::MAX))
                .sst_allowed_trivial_move_max_count(Some(10))
                .build()
        });
        let mut picker = LevelCompactionPicker::new_with_mode(
            1,
            config,
            Arc::new(CompactionTaskValidator::unused()),
            Arc::new(CompactionDeveloperConfig::default()),
            L0PickerMode::SingleTablePartition {
                min_l0_level_count: 1,
            },
        );
        let levels = Levels {
            l0: generate_l0_nonoverlapping_multi_sublevels(vec![vec![
                generate_table(1, 1, 0, 10, 1),
                generate_table(2, 1, 20, 30, 1),
            ]]),
            levels: vec![generate_level(1, vec![])],
            ..Default::default()
        };
        let mut handlers = vec![LevelHandler::new(0), LevelHandler::new(1)];
        let ret = picker
            .pick_compaction(&levels, &handlers, &mut LocalPickerStatistic::default())
            .unwrap();
        assert_eq!(ret.input_levels[0].table_infos.len(), 2);
        assert!(ret.input_levels[1].table_infos.is_empty());
        assert_eq!(ret.total_file_count, 1);
        ret.add_pending_task(1, &mut handlers);
        assert!(
            picker
                .pick_compaction(&levels, &handlers, &mut LocalPickerStatistic::default())
                .is_none()
        );
    }

    #[test]
    fn test_trivial_move_only_does_not_fall_back_to_rewrite() {
        let config = Arc::new(CompactionConfigBuilder::new().build());
        let levels = Levels {
            l0: generate_l0_nonoverlapping_multi_sublevels(vec![vec![generate_table(
                1, 1, 0, 10, 1,
            )]]),
            levels: vec![generate_level(1, vec![generate_table(10, 1, 0, 10, 0)])],
            ..Default::default()
        };
        let handlers = vec![LevelHandler::new(0), LevelHandler::new(1)];

        let mut rewrite_picker = LevelCompactionPicker::new_with_mode(
            1,
            config.clone(),
            Arc::new(CompactionTaskValidator::unused()),
            Arc::new(CompactionDeveloperConfig::default()),
            L0PickerMode::SingleTablePartition {
                min_l0_level_count: 1,
            },
        );
        assert!(
            rewrite_picker
                .pick_compaction(&levels, &handlers, &mut LocalPickerStatistic::default())
                .is_some()
        );

        let mut trivial_move_picker = LevelCompactionPicker::new_with_mode(
            1,
            config,
            Arc::new(CompactionTaskValidator::unused()),
            Arc::new(CompactionDeveloperConfig::default()),
            L0PickerMode::SingleTablePartitionTrivialMove,
        );
        assert!(
            trivial_move_picker
                .pick_compaction(&levels, &handlers, &mut LocalPickerStatistic::default())
                .is_none()
        );
    }

    #[test]
    fn test_to_base_ignores_partition_markers_but_stops_at_overlapping_level() {
        let config = Arc::new(CompactionConfig {
            split_weight_by_vnode: 8,
            ..CompactionConfigBuilder::new().build()
        });
        let mut picker = LevelCompactionPicker::new_with_mode(
            1,
            config,
            Arc::new(CompactionTaskValidator::unused()),
            Arc::new(CompactionDeveloperConfig::default()),
            L0PickerMode::SingleTablePartition {
                min_l0_level_count: 1,
            },
        );
        let mut levels = Levels {
            l0: generate_l0_nonoverlapping_sublevels(
                (1..=3).map(|id| generate_table(id, 1, 0, 10, id)).collect(),
            ),
            levels: vec![generate_level(1, vec![generate_table(10, 1, 0, 10, 0)])],
            ..Default::default()
        };
        for (level, count) in levels.l0.sub_levels.iter_mut().zip_eq_fast([0, 4, 16]) {
            level.vnode_partition_count = count;
        }
        let handlers = vec![LevelHandler::new(0), LevelHandler::new(1)];
        let ret = picker
            .pick_compaction(&levels, &handlers, &mut LocalPickerStatistic::default())
            .unwrap();
        assert_eq!(ret.input_levels.len(), 4);
        assert_eq!(ret.input_levels.last().unwrap().table_infos[0].sst_id, 10);

        levels.l0.sub_levels[1].level_type = LevelType::Overlapping;
        let ret = picker
            .pick_compaction(&levels, &handlers, &mut LocalPickerStatistic::default())
            .unwrap();
        assert_eq!(ret.input_levels.len(), 2);
        assert_eq!(ret.input_levels[0].table_infos[0].sst_id, 1);
    }

    #[test]
    fn test_compact_l0_to_l1() {
        let mut picker = create_compaction_picker_for_test();
        let l0 = generate_level(
            0,
            vec![
                generate_table(5, 1, 100, 200, 2),
                generate_table(4, 1, 201, 300, 2),
            ],
        );
        let mut levels = Levels {
            l0: OverlappingLevel {
                total_file_size: l0.total_file_size,
                uncompressed_file_size: l0.total_file_size,
                sub_levels: vec![l0],
            },
            levels: vec![generate_level(
                1,
                vec![
                    generate_table(3, 1, 1, 100, 1),
                    generate_table(2, 1, 101, 150, 1),
                    generate_table(1, 1, 201, 210, 1),
                ],
            )],
            ..Default::default()
        };
        let mut local_stats = LocalPickerStatistic::default();
        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];

        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        assert_eq!(ret.input_levels[0].table_infos.len(), 1);
        assert_eq!(ret.input_levels[0].table_infos[0].sst_id, 4);
        assert_eq!(ret.input_levels[1].table_infos[0].sst_id, 1);

        ret.add_pending_task(0, &mut levels_handler);
        {
            push_table_level0_nonoverlapping(&mut levels, generate_table(6, 1, 100, 200, 2));
            push_table_level0_nonoverlapping(&mut levels, generate_table(7, 1, 301, 333, 4));
            let ret2 = picker
                .pick_compaction(&levels, &levels_handler, &mut local_stats)
                .unwrap();

            assert_eq!(ret2.input_levels[0].table_infos.len(), 1);
            assert_eq!(ret2.input_levels[0].table_infos[0].sst_id, 6);
            assert_eq!(ret2.input_levels[1].table_infos[0].sst_id, 5);
        }

        levels.l0.sub_levels[0]
            .table_infos
            .retain(|table| table.sst_id != 4);
        levels.l0.total_file_size -= ret.input_levels[0].table_infos[0].file_size;

        levels_handler[0].remove_task(0);
        levels_handler[1].remove_task(0);

        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        assert_eq!(ret.input_levels.len(), 3);
        assert_eq!(ret.input_levels[0].table_infos[0].sst_id, 6);
        assert_eq!(ret.input_levels[1].table_infos[0].sst_id, 5);
        assert_eq!(ret.input_levels[2].table_infos.len(), 2);
        assert_eq!(ret.input_levels[2].table_infos[0].sst_id, 3);
        assert_eq!(ret.input_levels[2].table_infos[1].sst_id, 2);
        ret.add_pending_task(1, &mut levels_handler);

        let mut local_stats = LocalPickerStatistic::default();
        // Cannot pick because no idle table in sub-level[0]. (And sub-level[0] is pending
        // actually).
        push_table_level0_overlapping(&mut levels, generate_table(8, 1, 199, 233, 3));
        let ret = picker.pick_compaction(&levels, &levels_handler, &mut local_stats);
        assert!(ret.is_none());

        // Don't pick overlapping sub-level 8
        levels_handler[0].remove_task(1);
        levels_handler[1].remove_task(1);
        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        assert_eq!(ret.input_levels.len(), 3);
        assert_eq!(ret.input_levels[0].table_infos[0].sst_id, 6);
        assert_eq!(ret.input_levels[1].table_infos[0].sst_id, 5);
        assert_eq!(ret.input_levels[2].table_infos.len(), 2);
    }

    #[test]
    fn test_selecting_key_range_overlap() {
        // When picking L0->L1, all L1 files overlapped with selecting_key_range should be picked.
        let config = Arc::new(
            CompactionConfigBuilder::new()
                .level0_tier_compact_file_number(2)
                .compaction_mode(CompactionMode::Range as i32)
                .level0_sub_level_compact_level_count(1)
                .enable_optimize_l0_interval_selection(Some(false))
                .build(),
        );

        let config_enable_optimize_l0_interval_selection = Arc::new(
            CompactionConfigBuilder::new()
                .level0_tier_compact_file_number(2)
                .compaction_mode(CompactionMode::Range as i32)
                .level0_sub_level_compact_level_count(1)
                .enable_optimize_l0_interval_selection(Some(true))
                .build(),
        );

        let mut picker =
            LevelCompactionPicker::new(1, config, Arc::new(CompactionDeveloperConfig::default()));

        let mut picker_enable_optimize_l0_interval_selection = LevelCompactionPicker::new(
            1,
            config_enable_optimize_l0_interval_selection,
            Arc::new(CompactionDeveloperConfig::default()),
        );

        let levels = vec![Level {
            level_idx: 1,
            level_type: LevelType::Nonoverlapping,
            table_infos: vec![
                generate_table(3, 1, 0, 50, 1),
                generate_table(4, 1, 150, 180, 1),
                generate_table(5, 1, 250, 300, 1),
            ],
            ..Default::default()
        }];
        let mut levels = Levels {
            levels,
            l0: OverlappingLevel {
                sub_levels: vec![],
                total_file_size: 0,
                uncompressed_file_size: 0,
            },
            ..Default::default()
        };
        push_tables_level0_nonoverlapping(&mut levels, vec![generate_table(1, 1, 50, 140, 2)]);
        push_tables_level0_nonoverlapping(
            &mut levels,
            vec![
                generate_table(7, 1, 200, 250, 2),
                generate_table(8, 1, 400, 500, 2),
            ],
        );

        {
            let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];

            let mut local_stats = LocalPickerStatistic::default();
            let ret = picker_enable_optimize_l0_interval_selection
                .pick_compaction(&levels, &levels_handler, &mut local_stats)
                .unwrap();

            // pick
            // l0 [sst_8]
            assert_eq!(ret.input_levels.len(), 2);
            assert_eq!(
                ret.input_levels[0]
                    .table_infos
                    .iter()
                    .map(|t| t.sst_id)
                    .collect_vec(),
                vec![8]
            );
            // trivial_move
            assert!(ret.input_levels[1].table_infos.is_empty());

            ret.add_pending_task(0, &mut levels_handler);

            let ret = picker_enable_optimize_l0_interval_selection
                .pick_compaction(&levels, &levels_handler, &mut local_stats)
                .unwrap();

            assert_eq!(ret.input_levels.len(), 2);
            assert_eq!(
                ret.input_levels[0]
                    .table_infos
                    .iter()
                    .map(|t| t.sst_id)
                    .collect_vec(),
                vec![7]
            );

            assert_eq!(
                ret.input_levels[1]
                    .table_infos
                    .iter()
                    .map(|t| t.sst_id)
                    .collect_vec(),
                vec![5]
            );
        }

        {
            let levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];

            let mut local_stats = LocalPickerStatistic::default();
            let ret = picker
                .pick_compaction(&levels, &levels_handler, &mut local_stats)
                .unwrap();

            // pick
            assert_eq!(ret.input_levels.len(), 2);
            assert_eq!(
                ret.input_levels[0]
                    .table_infos
                    .iter()
                    .map(|t| t.sst_id.as_raw_id())
                    .collect_vec(),
                vec![1]
            );

            assert_eq!(
                ret.input_levels[1]
                    .table_infos
                    .iter()
                    .map(|t| t.sst_id.as_raw_id())
                    .collect_vec(),
                vec![3]
            );
        }
    }

    #[test]
    fn test_l0_to_l1_compact_conflict() {
        // When picking L0->L1, L0's selecting_key_range should not be overlapped with L0's
        // compacting_key_range.
        let mut picker = create_compaction_picker_for_test();
        let levels = vec![Level {
            level_idx: 1,
            level_type: LevelType::Nonoverlapping,
            table_infos: vec![],
            total_file_size: 0,
            sub_level_id: 0,
            uncompressed_file_size: 0,
            ..Default::default()
        }];
        let mut levels = Levels {
            levels,
            l0: OverlappingLevel {
                sub_levels: vec![],
                total_file_size: 0,
                uncompressed_file_size: 0,
            },
            ..Default::default()
        };
        push_tables_level0_nonoverlapping(
            &mut levels,
            vec![
                generate_table(1, 1, 100, 300, 2),
                generate_table(2, 1, 350, 500, 2),
            ],
        );
        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];

        let mut local_stats = LocalPickerStatistic::default();
        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        // trivial_move
        ret.add_pending_task(0, &mut levels_handler); // pending only for test
        push_tables_level0_nonoverlapping(&mut levels, vec![generate_table(3, 1, 250, 300, 3)]);
        let config: CompactionConfig = CompactionConfigBuilder::new()
            .level0_tier_compact_file_number(2)
            .max_compaction_bytes(1000)
            .sub_level_max_compaction_bytes(150)
            .max_bytes_for_level_multiplier(1)
            .level0_sub_level_compact_level_count(3)
            .build();
        let mut picker = TierCompactionPicker::new(Arc::new(config));

        let ret: Option<CompactionInput> =
            picker.pick_compaction(&levels, &levels_handler, &mut local_stats);
        assert!(ret.is_none());
    }

    #[test]
    fn test_skip_compact_write_amplification_limit() {
        let config: CompactionConfig = CompactionConfigBuilder::new()
            .level0_tier_compact_file_number(2)
            .max_compaction_bytes(1000)
            .sub_level_max_compaction_bytes(150)
            .max_bytes_for_level_multiplier(1)
            .level0_sub_level_compact_level_count(2)
            .build();
        let mut picker = LevelCompactionPicker::new(
            1,
            Arc::new(config),
            Arc::new(CompactionDeveloperConfig::default()),
        );
        let mut levels = Levels {
            levels: vec![Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![
                    generate_table(1, 1, 100, 399, 2),
                    generate_table(2, 1, 400, 699, 2),
                    generate_table(3, 1, 700, 999, 2),
                ],
                total_file_size: 900,
                sub_level_id: 0,
                uncompressed_file_size: 900,
                ..Default::default()
            }],
            l0: generate_l0_nonoverlapping_sublevels(vec![]),
            ..Default::default()
        };
        push_tables_level0_nonoverlapping(
            &mut levels,
            vec![
                generate_table(4, 1, 100, 180, 2),
                generate_table(5, 1, 400, 450, 2),
                generate_table(6, 1, 600, 700, 2),
            ],
        );

        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];
        levels_handler[0].add_pending_task(1, 4, &levels.l0.sub_levels[0].table_infos);
        let ret = picker.pick_compaction(
            &levels,
            &levels_handler,
            &mut LocalPickerStatistic::default(),
        );
        assert!(ret.is_none());
    }

    #[test]
    fn test_to_base_does_not_reject_by_write_amplification() {
        let config: CompactionConfig = CompactionConfigBuilder::new()
            .level0_tier_compact_file_number(2)
            .max_compaction_bytes(1000)
            .sub_level_max_compaction_bytes(150)
            .max_bytes_for_level_multiplier(1)
            .level0_sub_level_compact_level_count(2)
            .build();
        let mut picker = LevelCompactionPicker::new_with_mode(
            1,
            Arc::new(config),
            Arc::new(CompactionTaskValidator::unused()),
            Arc::new(CompactionDeveloperConfig::default()),
            L0PickerMode::SingleTablePartition {
                min_l0_level_count: 2,
            },
        );

        let mut levels = Levels {
            levels: vec![Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![
                    generate_table(1, 1, 100, 399, 2),
                    generate_table(2, 1, 400, 699, 2),
                    generate_table(3, 1, 700, 999, 2),
                ],
                total_file_size: 900,
                sub_level_id: 0,
                uncompressed_file_size: 900,
                ..Default::default()
            }],
            l0: generate_l0_nonoverlapping_sublevels(vec![]),
            ..Default::default()
        };
        push_tables_level0_nonoverlapping(
            &mut levels,
            vec![
                generate_table(4, 1, 100, 180, 2),
                generate_table(5, 1, 400, 450, 2),
                generate_table(6, 1, 600, 700, 2),
            ],
        );
        push_tables_level0_nonoverlapping(
            &mut levels,
            vec![
                generate_table(7, 1, 100, 180, 3),
                generate_table(8, 1, 400, 450, 3),
                generate_table(9, 1, 600, 700, 3),
            ],
        );

        let levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];
        let mut local_stats = LocalPickerStatistic::default();
        let ret = picker.pick_compaction(&levels, &levels_handler, &mut local_stats);
        assert!(ret.is_some());
        assert_eq!(local_stats.skip_by_write_amp_limit, 0);
    }

    #[test]
    fn test_to_base_defensively_rejects_too_few_selected_levels() {
        let config = Arc::new(
            CompactionConfigBuilder::new()
                .level0_sub_level_compact_level_count(3)
                .build(),
        );
        let developer_config = Arc::new(CompactionDeveloperConfig {
            enable_trivial_move: false,
            ..Default::default()
        });
        let validator = Arc::new(CompactionTaskValidator::new(config.clone()));
        let mut picker = LevelCompactionPicker::new_with_mode(
            1,
            config,
            validator,
            developer_config,
            L0PickerMode::SingleTablePartition {
                min_l0_level_count: 3,
            },
        );
        let levels = Levels {
            levels: vec![generate_level(1, vec![])],
            l0: generate_l0_nonoverlapping_sublevels(vec![
                generate_table(1, 1, 0, 10, 1),
                generate_table(2, 1, 20, 30, 2),
                generate_table(3, 1, 40, 50, 3),
            ]),
            ..Default::default()
        };
        let handlers = vec![LevelHandler::new(0), LevelHandler::new(1)];
        let mut stats = LocalPickerStatistic::default();

        assert!(
            picker
                .pick_compaction(&levels, &handlers, &mut stats)
                .is_none()
        );
        assert!(stats.skip_by_count_limit > 0);
    }

    #[test]
    fn partition_to_base_retries_after_deepest_seed_hits_pending_base() {
        let config = Arc::new(
            CompactionConfigBuilder::new()
                .max_compaction_bytes(10_000)
                .max_bytes_for_level_base(1_000)
                .level0_sub_level_compact_level_count(3)
                .build(),
        );
        let mut picker = LevelCompactionPicker::new_with_mode(
            1,
            config,
            Arc::new(CompactionTaskValidator::unused()),
            Arc::new(CompactionDeveloperConfig {
                enable_trivial_move: false,
                ..Default::default()
            }),
            L0PickerMode::SingleTablePartition {
                min_l0_level_count: 3,
            },
        );
        let levels = Levels {
            levels: vec![generate_level(
                1,
                vec![
                    generate_table(100, 1, 0, 10, 0),
                    generate_table(101, 1, 20, 30, 0),
                ],
            )],
            l0: generate_l0_nonoverlapping_multi_sublevels(vec![
                vec![
                    generate_table(1, 1, 0, 10, 1),
                    generate_table(2, 1, 20, 30, 1),
                ],
                vec![
                    generate_table(3, 1, 0, 10, 2),
                    generate_table(4, 1, 20, 30, 2),
                ],
                vec![
                    generate_table(5, 1, 0, 10, 3),
                    generate_table(6, 1, 20, 30, 3),
                ],
                vec![generate_table(7, 1, 0, 10, 4)],
            ]),
            ..Default::default()
        };
        let mut handlers = vec![LevelHandler::new(0), LevelHandler::new(1)];
        handlers[1].add_pending_task(99, 2, levels.levels[0].table_infos.iter().take(1));

        let input = picker
            .pick_compaction(&levels, &handlers, &mut LocalPickerStatistic::default())
            .expect("a later valid seed must be tried after the deepest seed hits pending Base");
        let selected_l0_ids = input
            .input_levels
            .iter()
            .filter(|level| level.level_idx == 0)
            .flat_map(|level| level.table_infos.iter())
            .map(|sst| sst.sst_id.as_raw_id())
            .sorted()
            .collect_vec();

        assert_eq!(selected_l0_ids, vec![2, 4, 6]);
    }

    #[test]
    fn partition_to_base_ignores_fully_pending_oldest_disjoint_sub_level() {
        let config = Arc::new(
            CompactionConfigBuilder::new()
                .max_compaction_bytes(10_000)
                .max_bytes_for_level_base(1_000)
                .level0_sub_level_compact_level_count(3)
                .build(),
        );
        let mut picker = LevelCompactionPicker::new_with_mode(
            1,
            config,
            Arc::new(CompactionTaskValidator::unused()),
            Arc::new(CompactionDeveloperConfig {
                enable_trivial_move: false,
                ..Default::default()
            }),
            L0PickerMode::SingleTablePartition {
                min_l0_level_count: 3,
            },
        );
        let levels = Levels {
            levels: vec![generate_level(
                1,
                vec![
                    generate_table(100, 1, 0, 10, 0),
                    generate_table(101, 1, 20, 30, 0),
                ],
            )],
            l0: generate_l0_nonoverlapping_multi_sublevels(vec![
                vec![generate_table(1, 1, 0, 10, 1)],
                vec![generate_table(2, 1, 20, 30, 2)],
                vec![generate_table(3, 1, 20, 30, 3)],
                vec![generate_table(4, 1, 20, 30, 4)],
                vec![generate_table(5, 1, 20, 30, 5)],
            ]),
            ..Default::default()
        };
        let mut handlers = vec![LevelHandler::new(0), LevelHandler::new(1)];
        handlers[0].add_pending_task(99, 1, &levels.l0.sub_levels[0].table_infos);

        let input = picker
            .pick_compaction(&levels, &handlers, &mut LocalPickerStatistic::default())
            .expect("a pending disjoint oldest sub-level must not block a runnable newer stack");
        let selected_l0_ids = input
            .input_levels
            .iter()
            .filter(|level| level.level_idx == 0)
            .flat_map(|level| level.table_infos.iter())
            .map(|sst| sst.sst_id.as_raw_id())
            .sorted()
            .collect_vec();

        assert_eq!(selected_l0_ids, vec![2, 3, 4, 5]);

        picker.mode = L0PickerMode::Legacy;
        assert!(
            picker
                .pick_compaction(&levels, &handlers, &mut LocalPickerStatistic::default())
                .is_none()
        );

        // Skipping the early return must not skip a pending SST in the actual closure.
        picker.mode = L0PickerMode::SingleTablePartition {
            min_l0_level_count: 3,
        };
        let mut overlapping = levels.clone();
        overlapping.l0.sub_levels[0].table_infos[0] = generate_table(1, 1, 20, 30, 1);
        assert!(
            picker
                .pick_compaction(
                    &overlapping,
                    &handlers,
                    &mut LocalPickerStatistic::default()
                )
                .is_none()
        );
    }

    #[test]
    fn partition_growth_amortizes_base_and_preserves_seed_on_rejection() {
        use risingwave_hummock_sdk::compact_task::{CompactTask, CompactTaskAssignment};

        use crate::hummock::compaction::InProgressCompactionView;

        for scenario in [
            "accepted",
            "accepted-then-rejected",
            "shallow-donor",
            "base-set-change",
            "output-conflict",
            "legacy",
        ] {
            let mut l0 = vec![
                vec![
                    generate_table(1, 1, 10, 20, 1),
                    generate_table(2, 1, 60, 70, 1),
                ],
                vec![
                    generate_table(3, 1, 10, 20, 2),
                    generate_table(4, 1, 60, 70, 2),
                ],
            ];
            if scenario == "shallow-donor" {
                l0[1].pop();
            }
            if scenario == "accepted-then-rejected" {
                l0[0].push(generate_table(5, 1, 100, 110, 1));
                l0[1].push(generate_table(6, 1, 100, 110, 2));
            }
            let narrow_base = matches!(scenario, "base-set-change" | "output-conflict");
            let mut base = vec![if narrow_base {
                generate_table(100, 1, 10, 20, 0)
            } else {
                // Both disjoint L0 closures would otherwise rewrite this same Base SST.
                generate_table(100, 1, 0, 90, 0)
            }];
            if scenario == "base-set-change" {
                base.push(generate_table(101, 1, 60, 70, 0));
            }
            if scenario == "accepted-then-rejected" {
                base.push(generate_table(101, 1, 100, 110, 0));
            }
            let levels = Levels {
                levels: vec![generate_level(1, base)],
                l0: generate_l0_nonoverlapping_multi_sublevels(l0),
                ..Default::default()
            };
            let config = CompactionConfig {
                // Growth within the existing levels is allowed even at the level limit.
                max_l0_compact_level_count: Some(2),
                ..CompactionConfigBuilder::new()
                    .max_compaction_bytes(10_000)
                    .build()
            };
            let mut picker = LevelCompactionPicker::new_with_mode(
                1,
                Arc::new(config),
                Arc::new(CompactionTaskValidator::unused()),
                Arc::new(CompactionDeveloperConfig {
                    enable_trivial_move: false,
                    ..Default::default()
                }),
                if scenario == "legacy" {
                    L0PickerMode::Legacy
                } else {
                    L0PickerMode::SingleTablePartition {
                        min_l0_level_count: 2,
                    }
                },
            );
            // The pending output lies in the hole between the two L0 closures, and touches
            // neither Base SST. Only the grown output hull conflicts with it.
            let assignments = [CompactTaskAssignment {
                compact_task: CompactTask {
                    task_id: 99,
                    compaction_group_id: 1.into(),
                    target_level: 1,
                    input_ssts: vec![InputLevel {
                        level_idx: 0,
                        level_type: LevelType::Nonoverlapping,
                        table_infos: vec![generate_table(999, 1, 40, 50, 0)],
                    }],
                    ..Default::default()
                },
                context_id: 1.into(),
            }];
            let in_progress = InProgressCompactionView::for_group(&assignments, 1.into());
            let handlers = vec![LevelHandler::new(0), LevelHandler::new(1)];
            let mut stats = LocalPickerStatistic::default();
            let input = picker
                .pick_compaction_with_output_conflict_check(
                    &levels,
                    &handlers,
                    &mut stats,
                    |input| {
                        scenario == "output-conflict" && in_progress.has_conflict_with_input(input)
                    },
                )
                .unwrap();
            let expected_l0_files = match scenario {
                "accepted" | "accepted-then-rejected" => 4,
                "shallow-donor" => 3,
                _ => 2,
            };
            assert_eq!(input.total_file_count, expected_l0_files + 1, "{scenario}");
            assert_eq!(
                input.select_input_size,
                expected_l0_files * 11,
                "{scenario}"
            );
            assert_eq!(
                input.target_input_size,
                if narrow_base { 11 } else { 91 },
                "{scenario}"
            );
            assert_eq!(
                input.input_levels.last().unwrap().table_infos[0].sst_id,
                100
            );
            if scenario != "legacy" {
                assert_eq!(stats.partition_l0_growth.initial_l0_size, 22);
                let outcome = match scenario {
                    "accepted" | "accepted-then-rejected" | "shallow-donor" => {
                        PartitionL0GrowthOutcome::Accepted
                    }
                    "base-set-change" => PartitionL0GrowthOutcome::BaseSetChange,
                    _ => PartitionL0GrowthOutcome::OutputConflict,
                };
                assert_eq!(
                    stats.partition_l0_growth.outcomes[outcome as usize], 1,
                    "{scenario}"
                );
                if scenario == "accepted-then-rejected" {
                    assert_eq!(
                        stats.partition_l0_growth.outcomes
                            [PartitionL0GrowthOutcome::BaseSetChange as usize],
                        1
                    );
                }
            } else {
                assert_eq!(stats.partition_l0_growth.outcomes, [0; 7]);
            }
        }
    }

    #[test]
    fn partition_growth_distinguishes_duplicate_and_limit_rejections() {
        use PartitionL0GrowthOutcome as Outcome;
        let levels = generate_l0_nonoverlapping_multi_sublevels(vec![
            vec![
                generate_table(1, 1, 10, 20, 1),
                generate_table(2, 1, 60, 70, 1),
            ],
            vec![generate_table(3, 1, 10, 20, 2)],
            vec![generate_table(4, 1, 60, 70, 3)],
        ]);
        let base = generate_level(1, vec![generate_table(100, 1, 10, 20, 0)]);
        let selected = CompactionInput {
            input_levels: vec![
                InputLevel {
                    level_idx: 0,
                    level_type: LevelType::Nonoverlapping,
                    table_infos: vec![levels.sub_levels[1].table_infos[0].clone()],
                },
                InputLevel {
                    level_idx: 0,
                    level_type: LevelType::Nonoverlapping,
                    table_infos: vec![levels.sub_levels[0].table_infos[0].clone()],
                },
                InputLevel {
                    level_idx: 1,
                    level_type: LevelType::Nonoverlapping,
                    table_infos: base.table_infos.clone(),
                },
            ],
            select_input_size: 22,
            target_input_size: 11,
            total_file_count: 3,
            target_level: 1,
            ..Default::default()
        };
        for outcome in [
            Outcome::NoNewSst,
            Outcome::Files,
            Outcome::Levels,
            Outcome::Bytes,
        ] {
            let mut picker = create_compaction_picker_for_test();
            let config = Arc::make_mut(&mut picker.config);
            config.max_l0_compact_level_count = Some(if matches!(outcome, Outcome::Levels) {
                2
            } else {
                10
            });
            config.level0_max_compact_file_number = if matches!(outcome, Outcome::Files) {
                3
            } else {
                100
            };
            config.max_compaction_bytes = if matches!(outcome, Outcome::Bytes) {
                44
            } else {
                10_000
            };
            let donor = SubLevelSstables {
                sstable_infos: if matches!(outcome, Outcome::NoNewSst) {
                    vec![(
                        levels.sub_levels[0].sub_level_id,
                        vec![levels.sub_levels[0].table_infos[0].clone()],
                    )]
                } else {
                    vec![
                        (
                            levels.sub_levels[0].sub_level_id,
                            vec![levels.sub_levels[0].table_infos[1].clone()],
                        ),
                        (
                            levels.sub_levels[2].sub_level_id,
                            vec![levels.sub_levels[2].table_infos[0].clone()],
                        ),
                    ]
                },
                ..Default::default()
            };
            let reason = picker
                .try_grow_partition_l0_input(&selected, &donor, &levels.sub_levels, &base)
                .unwrap_err();
            assert_eq!(reason as usize, outcome as usize);
            assert_eq!(selected.select_input_size, 22);
            assert_eq!(selected.total_file_count, 3);
        }
    }

    #[test]
    fn partition_to_base_order_is_independent_of_legacy_sub_level_bytes() {
        let levels = Levels {
            levels: vec![generate_level(
                1,
                vec![
                    generate_table(100, 1, 0, 99, 0),
                    generate_table(101, 1, 200, 209, 0),
                ],
            )],
            l0: generate_l0_nonoverlapping_multi_sublevels(vec![
                vec![
                    generate_table(1, 1, 0, 99, 1),
                    generate_table(2, 1, 200, 209, 1),
                ],
                vec![
                    generate_table(3, 1, 0, 99, 2),
                    generate_table(4, 1, 200, 209, 2),
                ],
                vec![generate_table(5, 1, 200, 209, 3)],
            ]),
            ..Default::default()
        };
        let handlers = vec![LevelHandler::new(0), LevelHandler::new(1)];

        for sub_level_bytes in [128, 512] {
            let config = Arc::new(
                CompactionConfigBuilder::new()
                    .max_compaction_bytes(10_000)
                    .sub_level_max_compaction_bytes(sub_level_bytes)
                    .max_bytes_for_level_base(1_000)
                    .level0_sub_level_compact_level_count(2)
                    .build(),
            );
            let mut picker = LevelCompactionPicker::new_with_mode(
                1,
                config,
                Arc::new(CompactionTaskValidator::unused()),
                Arc::new(CompactionDeveloperConfig {
                    enable_trivial_move: false,
                    ..Default::default()
                }),
                L0PickerMode::SingleTablePartition {
                    min_l0_level_count: 2,
                },
            );

            let input = picker
                .pick_compaction(&levels, &handlers, &mut LocalPickerStatistic::default())
                .unwrap();
            let selected_ids = input
                .input_levels
                .iter()
                .filter(|level| level.level_idx == 0)
                .flat_map(|level| level.table_infos.iter())
                .map(|sst| sst.sst_id.as_raw_id())
                .collect_vec();
            assert_eq!(selected_ids, vec![5, 4, 2]);
        }
    }

    #[test]
    fn test_l0_to_l1_break_on_exceed_compaction_size() {
        let mut local_stats = LocalPickerStatistic::default();
        let mut l0 = generate_l0_overlapping_sublevels(vec![
            vec![
                generate_table(4, 1, 10, 90, 1),
                generate_table(5, 1, 210, 220, 1),
            ],
            vec![generate_table(6, 1, 0, 100000, 1)],
            vec![generate_table(7, 1, 0, 100000, 1)],
        ]);
        // We can set level_type only because the input above is valid.
        for s in &mut l0.sub_levels {
            s.level_type = LevelType::Nonoverlapping;
        }
        let levels = Levels {
            l0,
            levels: vec![generate_level(1, vec![generate_table(3, 1, 0, 100000, 1)])],
            ..Default::default()
        };
        let levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];

        // Pick with large max_compaction_bytes results all sub levels included in input.
        let config = Arc::new(
            CompactionConfigBuilder::new()
                .max_compaction_bytes(500000)
                .sub_level_max_compaction_bytes(50000)
                .max_bytes_for_level_base(500000)
                .level0_sub_level_compact_level_count(1)
                .build(),
        );
        // Only include sub-level 0 results will violate MAX_WRITE_AMPLIFICATION.
        // So all sub-levels are included to make write amplification < MAX_WRITE_AMPLIFICATION.
        let mut picker =
            LevelCompactionPicker::new(1, config, Arc::new(CompactionDeveloperConfig::default()));
        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        assert_eq!(ret.input_levels[0].table_infos[0].sst_id, 7);
        assert_eq!(
            3,
            ret.input_levels.iter().filter(|l| l.level_idx == 0).count()
        );
        assert_eq!(
            4,
            ret.input_levels
                .iter()
                .filter(|l| l.level_idx == 0)
                .map(|l| l.table_infos.len())
                .sum::<usize>()
        );

        // Pick with small max_compaction_bytes results partial sub levels included in input.
        let config = Arc::new(
            CompactionConfigBuilder::new()
                .max_compaction_bytes(100010)
                .max_bytes_for_level_base(512)
                .level0_sub_level_compact_level_count(1)
                .build(),
        );
        let mut picker =
            LevelCompactionPicker::new(1, config, Arc::new(CompactionDeveloperConfig::default()));

        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        assert_eq!(ret.input_levels[0].table_infos[0].sst_id, 6);
        assert_eq!(
            2,
            ret.input_levels.iter().filter(|l| l.level_idx == 0).count()
        );
        assert_eq!(
            3,
            ret.input_levels
                .iter()
                .filter(|l| l.level_idx == 0)
                .map(|l| l.table_infos.len())
                .sum::<usize>()
        );
    }

    #[test]
    fn test_l0_to_l1_break_on_pending_sub_level() {
        let l0 = generate_l0_nonoverlapping_multi_sublevels(vec![
            vec![
                generate_table(4, 1, 10, 90, 1),
                generate_table(5, 1, 210, 220, 1),
            ],
            vec![generate_table(6, 1, 0, 100000, 1)],
            vec![generate_table(7, 1, 0, 100000, 1)],
        ]);

        let levels = Levels {
            l0,
            levels: vec![generate_level(1, vec![generate_table(3, 1, 0, 100000, 1)])],
            ..Default::default()
        };
        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];
        let mut local_stats = LocalPickerStatistic::default();

        // Create a pending sub-level.
        let pending_level = levels.l0.sub_levels[1].clone();
        assert_eq!(pending_level.sub_level_id, 1);
        let tier_task_input = CompactionInput {
            input_levels: vec![InputLevel {
                level_idx: 0,
                level_type: pending_level.level_type,
                table_infos: pending_level.table_infos.clone(),
            }],
            target_level: 1,
            target_sub_level_id: pending_level.sub_level_id,
            ..Default::default()
        };
        assert!(!levels_handler[0].is_level_pending_compact(&pending_level));
        tier_task_input.add_pending_task(1, &mut levels_handler);
        assert!(levels_handler[0].is_level_pending_compact(&pending_level));

        // Pick with large max_compaction_bytes results all sub levels included in input.
        let config = Arc::new(
            CompactionConfigBuilder::new()
                .max_compaction_bytes(500000)
                .level0_sub_level_compact_level_count(2)
                .build(),
        );

        let mut picker = LevelCompactionPicker::new(
            1,
            config.clone(),
            Arc::new(CompactionDeveloperConfig::default()),
        );
        assert!(
            picker
                .pick_compaction(&levels, &levels_handler, &mut local_stats)
                .is_none()
        );

        // Free the pending sub-level.
        for pending_task_id in &levels_handler[0].pending_tasks_ids() {
            levels_handler[0].remove_task(*pending_task_id);
        }

        // No more pending sub-level so we can get a task now.
        let mut picker =
            LevelCompactionPicker::new(1, config, Arc::new(CompactionDeveloperConfig::default()));
        picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
    }

    #[test]
    fn test_l0_to_base_when_all_base_pending() {
        let l0 = generate_l0_nonoverlapping_multi_sublevels(vec![
            vec![
                generate_table(4, 1, 10, 90, 1),
                generate_table(5, 1, 1000, 2000, 1),
            ],
            vec![generate_table(6, 1, 10, 90, 1)],
        ]);

        let levels = Levels {
            l0,
            levels: vec![generate_level(1, vec![generate_table(3, 1, 1, 100, 1)])],
            ..Default::default()
        };
        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];
        let mut local_stats = LocalPickerStatistic::default();

        let config = Arc::new(
            CompactionConfigBuilder::new()
                .max_compaction_bytes(500000)
                .level0_sub_level_compact_level_count(2)
                .sub_level_max_compaction_bytes(1000)
                .build(),
        );

        let mut picker =
            LevelCompactionPicker::new(1, config, Arc::new(CompactionDeveloperConfig::default()));
        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        // 1. trivial_move
        assert_eq!(2, ret.input_levels.len());
        assert!(ret.input_levels[1].table_infos.is_empty());
        assert_eq!(5, ret.input_levels[0].table_infos[0].sst_id);
        ret.add_pending_task(0, &mut levels_handler);

        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        assert_eq!(3, ret.input_levels.len());
        assert_eq!(6, ret.input_levels[0].table_infos[0].sst_id);
    }
}

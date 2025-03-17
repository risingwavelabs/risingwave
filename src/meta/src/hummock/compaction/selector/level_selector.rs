//  Copyright 2025 RisingWave Labs
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).
use std::sync::Arc;

use risingwave_hummock_sdk::HummockCompactionTaskId;
use risingwave_hummock_sdk::level::Levels;
use risingwave_pb::hummock::compact_task::PbTaskType;
use risingwave_pb::hummock::{CompactionConfig, LevelType};

use super::{
    CompactionSelector, LevelCompactionPicker, TierCompactionPicker, create_compaction_task,
};
use crate::hummock::compaction::overlap_strategy::OverlapStrategy;
use crate::hummock::compaction::picker::{
    CompactionPicker, CompactionTaskValidator, IntraCompactionPicker, LocalPickerStatistic,
    MinOverlappingPicker,
};
use crate::hummock::compaction::selector::CompactionSelectorContext;
use crate::hummock::compaction::{
    CompactionDeveloperConfig, CompactionTask, create_overlap_strategy,
};
use crate::hummock::level_handler::LevelHandler;

pub const SCORE_BASE: u64 = 100;

#[derive(Debug, Default, Clone)]
pub enum PickerType {
    Tier,
    Intra,
    ToBase,
    #[default]
    BottomLevel,
}

impl std::fmt::Display for PickerType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            PickerType::Tier => "Tier",
            PickerType::Intra => "Intra",
            PickerType::ToBase => "ToBase",
            PickerType::BottomLevel => "BottomLevel",
        })
    }
}

#[derive(Default, Debug)]
pub struct PickerInfo {
    pub score: u64,
    pub select_level: usize,
    pub target_level: usize,
    pub picker_type: PickerType,
}

#[derive(Default, Debug)]
pub struct SelectContext {
    pub level_max_bytes: Vec<u64>,

    // All data will be placed in the last level. When the cluster is empty, the files in L0 will
    // be compact to `max_level`, and the `max_level` would be `base_level`. When the total
    // size of the files in  `base_level` reaches its capacity, we will place data in a higher
    // level, which equals to `base_level -= 1;`.
    pub base_level: usize,
    pub score_levels: Vec<PickerInfo>,
}

pub struct DynamicLevelSelectorCore {
    config: Arc<CompactionConfig>,
    developer_config: Arc<CompactionDeveloperConfig>,
}

#[derive(Default)]
pub struct DynamicLevelSelector {}

impl DynamicLevelSelectorCore {
    pub fn new(
        config: Arc<CompactionConfig>,
        developer_config: Arc<CompactionDeveloperConfig>,
    ) -> Self {
        Self {
            config,
            developer_config,
        }
    }

    pub fn get_config(&self) -> &CompactionConfig {
        self.config.as_ref()
    }

    fn create_compaction_picker(
        &self,
        picker_info: &PickerInfo,
        overlap_strategy: Arc<dyn OverlapStrategy>,
        compaction_task_validator: Arc<CompactionTaskValidator>,
    ) -> Box<dyn CompactionPicker> {
        match picker_info.picker_type {
            PickerType::Tier => Box::new(TierCompactionPicker::new_with_validator(
                self.config.clone(),
                compaction_task_validator,
            )),
            PickerType::ToBase => Box::new(LevelCompactionPicker::new_with_validator(
                picker_info.target_level,
                self.config.clone(),
                compaction_task_validator,
                self.developer_config.clone(),
            )),
            PickerType::Intra => Box::new(IntraCompactionPicker::new_with_validator(
                self.config.clone(),
                compaction_task_validator,
                self.developer_config.clone(),
            )),
            PickerType::BottomLevel => {
                assert_eq!(picker_info.select_level + 1, picker_info.target_level);
                Box::new(MinOverlappingPicker::new(
                    picker_info.select_level,
                    picker_info.target_level,
                    self.config.max_bytes_for_level_base / 2,
                    self.config.split_weight_by_vnode,
                    overlap_strategy,
                ))
            }
        }
    }

    // TODO: calculate this scores in apply compact result.
    /// `calculate_level_base_size` calculate base level and the base size of LSM tree build for
    /// current dataset. In other words,  `level_max_bytes` is our compaction goal which shall
    /// reach. This algorithm refers to the implementation in  [`https://github.com/facebook/rocksdb/blob/v7.2.2/db/version_set.cc#L3706`]
    pub fn calculate_level_base_size(&self, levels: &Levels) -> SelectContext {
        let mut first_non_empty_level = 0;
        let mut max_level_size = 0;
        let mut ctx = SelectContext::default();

        for level in &levels.levels {
            if level.total_file_size > 0 && first_non_empty_level == 0 {
                first_non_empty_level = level.level_idx as usize;
            }
            max_level_size = std::cmp::max(max_level_size, level.total_file_size);
        }

        ctx.level_max_bytes
            .resize(self.config.max_level as usize + 1, u64::MAX);

        if max_level_size == 0 {
            // Use the bottommost level.
            ctx.base_level = self.config.max_level as usize;
            return ctx;
        }

        let base_bytes_max = self.config.max_bytes_for_level_base;
        let base_bytes_min = base_bytes_max / self.config.max_bytes_for_level_multiplier;

        let mut cur_level_size = max_level_size;
        for _ in first_non_empty_level..self.config.max_level as usize {
            cur_level_size /= self.config.max_bytes_for_level_multiplier;
        }

        let base_level_size = if cur_level_size <= base_bytes_min {
            // Case 1. If we make target size of last level to be max_level_size,
            // target size of the first non-empty level would be smaller than
            // base_bytes_min. We set it be base_bytes_min.
            ctx.base_level = first_non_empty_level;
            base_bytes_min + 1
        } else {
            ctx.base_level = first_non_empty_level;
            while ctx.base_level > 1 && cur_level_size > base_bytes_max {
                ctx.base_level -= 1;
                cur_level_size /= self.config.max_bytes_for_level_multiplier;
            }
            std::cmp::min(base_bytes_max, cur_level_size)
        };

        let level_multiplier = self.config.max_bytes_for_level_multiplier as f64;
        let mut level_size = base_level_size;
        for i in ctx.base_level..=self.config.max_level as usize {
            // Don't set any level below base_bytes_max. Otherwise, the LSM can
            // assume an hourglass shape where L1+ sizes are smaller than L0. This
            // causes compaction scoring, which depends on level sizes, to favor L1+
            // at the expense of L0, which may fill up and stall.
            ctx.level_max_bytes[i] = std::cmp::max(level_size, base_bytes_max);
            level_size = (level_size as f64 * level_multiplier) as u64;
        }
        ctx
    }

    pub(crate) fn get_priority_levels(
        &self,
        levels: &Levels,
        handlers: &[LevelHandler],
    ) -> SelectContext {
        let mut ctx = self.calculate_level_base_size(levels);

        let l0_file_count = levels
            .l0
            .sub_levels
            .iter()
            .map(|sub_level| sub_level.table_infos.len())
            .sum::<usize>();

        let idle_file_count = match l0_file_count.checked_sub(handlers[0].pending_file_count()) {
            Some(count) => count,
            None => {
                // If the number of files in L0 is less than the number of pending files, it means
                // that may be encountered some issue, we can work around it.
                tracing::warn!(
                    "The number of files in L0 {} is less than the number of pending files {} group {} pending_tasks_ids {:?} compacting_files {:?}",
                    l0_file_count,
                    handlers[0].pending_file_count(),
                    levels.group_id,
                    handlers[0].pending_tasks_ids(),
                    handlers[0].compacting_files()
                );

                0
            }
        };

        if idle_file_count > 0 {
            // trigger l0 compaction when the number of files is too large.

            // The read query at the overlapping level needs to merge all the ssts, so the number of
            // ssts is the most important factor affecting the read performance, we use file count
            // to calculate the score
            let overlapping_file_count = levels
                .l0
                .sub_levels
                .iter()
                .filter(|level| level.level_type == LevelType::Overlapping)
                .map(|level| level.table_infos.len())
                .sum::<usize>();
            if overlapping_file_count > 0 {
                // FIXME: use overlapping idle file count
                let l0_overlapping_score =
                    std::cmp::min(idle_file_count, overlapping_file_count) as u64 * SCORE_BASE
                        / self.config.level0_tier_compact_file_number;
                // Reduce the level num of l0 overlapping sub_level
                ctx.score_levels.push(PickerInfo {
                    score: std::cmp::max(l0_overlapping_score, SCORE_BASE + 1),
                    select_level: 0,
                    target_level: 0,
                    picker_type: PickerType::Tier,
                })
            }

            // The read query at the non-overlapping level only selects ssts that match the query
            // range at each level, so the number of levels is the most important factor affecting
            // the read performance. At the same time, the size factor is also added to the score
            // calculation rule to avoid unbalanced compact task due to large size.
            let total_size = levels
                .l0
                .sub_levels
                .iter()
                .filter(|level| {
                    level.vnode_partition_count == self.config.split_weight_by_vnode
                        && level.level_type == LevelType::Nonoverlapping
                })
                .map(|level| level.total_file_size)
                .sum::<u64>()
                .saturating_sub(handlers[0].pending_output_file_size(ctx.base_level as u32));
            let base_level_size = levels.get_level(ctx.base_level).total_file_size;
            let base_level_sst_count = levels.get_level(ctx.base_level).table_infos.len() as u64;

            // size limit
            let non_overlapping_size_score = total_size * SCORE_BASE
                / std::cmp::max(self.config.max_bytes_for_level_base, base_level_size);
            // level count limit
            let non_overlapping_level_count = levels
                .l0
                .sub_levels
                .iter()
                .filter(|level| level.level_type == LevelType::Nonoverlapping)
                .count() as u64;
            let non_overlapping_level_score = non_overlapping_level_count * SCORE_BASE
                / std::cmp::max(
                    base_level_sst_count / 16,
                    self.config.level0_sub_level_compact_level_count as u64,
                );

            let non_overlapping_score =
                std::cmp::max(non_overlapping_size_score, non_overlapping_level_score);

            // Reduce the level num of l0 non-overlapping sub_level
            if non_overlapping_size_score > SCORE_BASE {
                ctx.score_levels.push(PickerInfo {
                    score: non_overlapping_score + 1,
                    select_level: 0,
                    target_level: ctx.base_level,
                    picker_type: PickerType::ToBase,
                });
            }

            if non_overlapping_level_score > SCORE_BASE {
                // FIXME: more accurate score calculation algorithm will be introduced (#11903)
                ctx.score_levels.push(PickerInfo {
                    score: non_overlapping_score,
                    select_level: 0,
                    target_level: 0,
                    picker_type: PickerType::Intra,
                });
            }
        }

        // The bottommost level can not be input level.
        for level in &levels.levels {
            let level_idx = level.level_idx as usize;
            if level_idx < ctx.base_level || level_idx >= self.config.max_level as usize {
                continue;
            }
            let output_file_size =
                handlers[level_idx].pending_output_file_size(level.level_idx + 1);
            let total_size = level.total_file_size.saturating_sub(output_file_size);
            if total_size == 0 {
                continue;
            }

            ctx.score_levels.push({
                PickerInfo {
                    score: total_size * SCORE_BASE / ctx.level_max_bytes[level_idx],
                    select_level: level_idx,
                    target_level: level_idx + 1,
                    picker_type: PickerType::BottomLevel,
                }
            });
        }

        // sort reverse to pick the largest one.
        ctx.score_levels.sort_by(|a, b| {
            b.score
                .cmp(&a.score)
                .then_with(|| a.target_level.cmp(&b.target_level))
        });
        ctx
    }

    /// `compact_pending_bytes_needed` calculates the number of compact bytes needed to balance the
    /// LSM Tree from the current state of each level in the LSM Tree in combination with
    /// `compaction_config`
    /// This algorithm refers to the implementation in  [`https://github.com/facebook/rocksdb/blob/main/db/version_set.cc#L3141`]
    pub fn compact_pending_bytes_needed(&self, levels: &Levels) -> u64 {
        let ctx = self.calculate_level_base_size(levels);
        self.compact_pending_bytes_needed_with_ctx(levels, &ctx)
    }

    pub fn compact_pending_bytes_needed_with_ctx(
        &self,
        levels: &Levels,
        ctx: &SelectContext,
    ) -> u64 {
        // l0
        let mut compact_pending_bytes = 0;
        let mut compact_to_next_level_bytes = 0;
        let l0_size = levels
            .l0
            .sub_levels
            .iter()
            .map(|sub_level| sub_level.total_file_size)
            .sum::<u64>();

        let mut l0_compaction_trigger = false;
        if l0_size > self.config.max_bytes_for_level_base {
            compact_pending_bytes = l0_size;
            compact_to_next_level_bytes = l0_size;
            l0_compaction_trigger = true;
        }

        // l1 and up
        let mut level_bytes;
        let mut next_level_bytes = 0;
        for level in &levels.levels[ctx.base_level - 1..levels.levels.len()] {
            let level_index = level.level_idx as usize;

            if next_level_bytes > 0 {
                level_bytes = next_level_bytes;
                next_level_bytes = 0;
            } else {
                level_bytes = level.total_file_size;
            }

            if level_index == ctx.base_level && l0_compaction_trigger {
                compact_pending_bytes += level_bytes;
            }

            level_bytes += compact_to_next_level_bytes;
            compact_to_next_level_bytes = 0;
            let level_target = ctx.level_max_bytes[level_index];
            if level_bytes > level_target {
                compact_to_next_level_bytes = level_bytes - level_target;

                // Estimate the actual compaction fan-out ratio as size ratio between
                // the two levels.
                assert_eq!(0, next_level_bytes);
                if level_index + 1 < ctx.level_max_bytes.len() {
                    let next_level = level_index + 1;
                    next_level_bytes = levels.levels[next_level - 1].total_file_size;
                }

                if next_level_bytes > 0 {
                    compact_pending_bytes += (compact_to_next_level_bytes as f64
                        * (next_level_bytes as f64 / level_bytes as f64 + 1.0))
                        as u64;
                }
            }
        }

        compact_pending_bytes
    }
}

impl CompactionSelector for DynamicLevelSelector {
    fn pick_compaction(
        &mut self,
        task_id: HummockCompactionTaskId,
        context: CompactionSelectorContext<'_>,
    ) -> Option<CompactionTask> {
        let CompactionSelectorContext {
            group: compaction_group,
            levels,
            level_handlers,
            selector_stats,
            developer_config,
            ..
        } = context;
        let dynamic_level_core = DynamicLevelSelectorCore::new(
            compaction_group.compaction_config.clone(),
            developer_config,
        );
        let overlap_strategy =
            create_overlap_strategy(compaction_group.compaction_config.compaction_mode());
        let ctx = dynamic_level_core.get_priority_levels(levels, level_handlers);
        // TODO: Determine which rule to enable by write limit
        let compaction_task_validator = Arc::new(CompactionTaskValidator::new(
            compaction_group.compaction_config.clone(),
        ));
        for picker_info in &ctx.score_levels {
            if picker_info.score <= SCORE_BASE {
                return None;
            }
            let mut picker = dynamic_level_core.create_compaction_picker(
                picker_info,
                overlap_strategy.clone(),
                compaction_task_validator.clone(),
            );

            let mut stats = LocalPickerStatistic::default();
            if let Some(ret) = picker.pick_compaction(levels, level_handlers, &mut stats) {
                ret.add_pending_task(task_id, level_handlers);
                return Some(create_compaction_task(
                    dynamic_level_core.get_config(),
                    ret,
                    ctx.base_level,
                    self.task_type(),
                ));
            }
            selector_stats.skip_picker.push((
                picker_info.select_level,
                picker_info.target_level,
                stats,
            ));
        }
        None
    }

    fn name(&self) -> &'static str {
        "DynamicLevelSelector"
    }

    fn task_type(&self) -> PbTaskType {
        PbTaskType::Dynamic
    }
}

#[cfg(test)]
pub mod tests {
    use std::collections::{BTreeSet, HashMap};
    use std::sync::Arc;

    use itertools::Itertools;
    use risingwave_common::constants::hummock::CompactionFilterFlag;
    use risingwave_hummock_sdk::level::Levels;
    use risingwave_hummock_sdk::version::HummockVersionStateTableInfo;
    use risingwave_pb::hummock::compaction_config::CompactionMode;

    use crate::hummock::compaction::CompactionDeveloperConfig;
    use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
    use crate::hummock::compaction::selector::tests::{
        assert_compaction_task, generate_l0_nonoverlapping_sublevels, generate_level,
        generate_tables, push_tables_level0_nonoverlapping,
    };
    use crate::hummock::compaction::selector::{
        CompactionSelector, DynamicLevelSelector, DynamicLevelSelectorCore, LocalSelectorStatistic,
    };
    use crate::hummock::level_handler::LevelHandler;
    use crate::hummock::model::CompactionGroup;
    use crate::hummock::test_utils::compaction_selector_context;

    #[test]
    fn test_dynamic_level() {
        let config = CompactionConfigBuilder::new()
            .max_bytes_for_level_base(100)
            .max_level(4)
            .max_bytes_for_level_multiplier(5)
            .max_compaction_bytes(1)
            .level0_tier_compact_file_number(2)
            .compaction_mode(CompactionMode::Range as i32)
            .build();
        let selector = DynamicLevelSelectorCore::new(
            Arc::new(config),
            Arc::new(CompactionDeveloperConfig::default()),
        );
        let levels = vec![
            generate_level(1, vec![]),
            generate_level(2, generate_tables(0..5, 0..1000, 3, 10)),
            generate_level(3, generate_tables(5..10, 0..1000, 2, 50)),
            generate_level(4, generate_tables(10..15, 0..1000, 1, 200)),
        ];
        let mut levels = Levels {
            levels,
            l0: generate_l0_nonoverlapping_sublevels(vec![]),
            ..Default::default()
        };
        let ctx = selector.calculate_level_base_size(&levels);
        assert_eq!(ctx.base_level, 2);
        assert_eq!(ctx.level_max_bytes[2], 100);
        assert_eq!(ctx.level_max_bytes[3], 200);
        assert_eq!(ctx.level_max_bytes[4], 1000);

        levels.levels[3]
            .table_infos
            .append(&mut generate_tables(15..20, 2000..3000, 1, 400));
        levels.levels[3].total_file_size = levels.levels[3]
            .table_infos
            .iter()
            .map(|sst| sst.sst_size)
            .sum::<u64>();

        let ctx = selector.calculate_level_base_size(&levels);
        // data size increase, so we need increase one level to place more data.
        assert_eq!(ctx.base_level, 1);
        assert_eq!(ctx.level_max_bytes[1], 100);
        assert_eq!(ctx.level_max_bytes[2], 120);
        assert_eq!(ctx.level_max_bytes[3], 600);
        assert_eq!(ctx.level_max_bytes[4], 3000);

        // append a large data to L0 but it does not change the base size of LSM tree.
        push_tables_level0_nonoverlapping(&mut levels, generate_tables(20..26, 0..1000, 1, 100));

        let ctx = selector.calculate_level_base_size(&levels);
        assert_eq!(ctx.base_level, 1);
        assert_eq!(ctx.level_max_bytes[1], 100);
        assert_eq!(ctx.level_max_bytes[2], 120);
        assert_eq!(ctx.level_max_bytes[3], 600);
        assert_eq!(ctx.level_max_bytes[4], 3000);

        levels.l0.sub_levels.clear();
        levels.l0.total_file_size = 0;
        levels.levels[0].table_infos = generate_tables(26..32, 0..1000, 1, 100);
        levels.levels[0].total_file_size = levels.levels[0]
            .table_infos
            .iter()
            .map(|sst| sst.sst_size)
            .sum::<u64>();

        let ctx = selector.calculate_level_base_size(&levels);
        assert_eq!(ctx.base_level, 1);
        assert_eq!(ctx.level_max_bytes[1], 100);
        assert_eq!(ctx.level_max_bytes[2], 120);
        assert_eq!(ctx.level_max_bytes[3], 600);
        assert_eq!(ctx.level_max_bytes[4], 3000);
    }

    #[test]
    fn test_pick_compaction() {
        let config = CompactionConfigBuilder::new()
            .max_bytes_for_level_base(200)
            .max_level(4)
            .max_bytes_for_level_multiplier(5)
            .target_file_size_base(5)
            .max_compaction_bytes(10000)
            .level0_tier_compact_file_number(4)
            .compaction_mode(CompactionMode::Range as i32)
            .level0_sub_level_compact_level_count(3)
            .build();
        let group_config = CompactionGroup::new(1, config.clone());
        let levels = vec![
            generate_level(1, vec![]),
            generate_level(2, generate_tables(0..5, 0..1000, 3, 10)),
            generate_level(3, generate_tables(5..10, 0..1000, 2, 50)),
            generate_level(4, generate_tables(10..15, 0..1000, 1, 200)),
        ];
        let mut levels = Levels {
            levels,
            l0: generate_l0_nonoverlapping_sublevels(generate_tables(15..25, 0..600, 3, 10)),
            ..Default::default()
        };

        let mut selector = DynamicLevelSelector::default();
        let mut levels_handlers = (0..5).map(LevelHandler::new).collect_vec();
        let mut local_stats = LocalSelectorStatistic::default();
        let compaction = selector
            .pick_compaction(
                1,
                compaction_selector_context(
                    &group_config,
                    &levels,
                    &BTreeSet::new(),
                    &mut levels_handlers,
                    &mut local_stats,
                    &HashMap::default(),
                    Arc::new(CompactionDeveloperConfig::default()),
                    &Default::default(),
                    &HummockVersionStateTableInfo::empty(),
                ),
            )
            .unwrap();
        assert_compaction_task(&compaction, &levels_handlers);

        let compaction_filter_flag = CompactionFilterFlag::STATE_CLEAN | CompactionFilterFlag::TTL;
        let config = CompactionConfigBuilder::with_config(config)
            .max_bytes_for_level_base(100)
            .sub_level_max_compaction_bytes(50)
            .target_file_size_base(20)
            .compaction_filter_mask(compaction_filter_flag.into())
            .build();
        let group_config = CompactionGroup::new(1, config.clone());
        let mut selector = DynamicLevelSelector::default();

        levels.l0.sub_levels.clear();
        levels.l0.total_file_size = 0;
        push_tables_level0_nonoverlapping(&mut levels, generate_tables(15..25, 0..600, 3, 20));
        let mut levels_handlers = (0..5).map(LevelHandler::new).collect_vec();
        let compaction = selector
            .pick_compaction(
                1,
                compaction_selector_context(
                    &group_config,
                    &levels,
                    &BTreeSet::new(),
                    &mut levels_handlers,
                    &mut local_stats,
                    &HashMap::default(),
                    Arc::new(CompactionDeveloperConfig::default()),
                    &Default::default(),
                    &HummockVersionStateTableInfo::empty(),
                ),
            )
            .unwrap();
        assert_compaction_task(&compaction, &levels_handlers);
        assert_eq!(compaction.input.input_levels[0].level_idx, 0);
        assert_eq!(compaction.input.target_level, 2);

        levels_handlers[0].remove_task(1);
        levels_handlers[2].remove_task(1);
        levels.l0.sub_levels.clear();
        levels.levels[1].table_infos = generate_tables(20..30, 0..1000, 3, 10);
        let compaction = selector
            .pick_compaction(
                2,
                compaction_selector_context(
                    &group_config,
                    &levels,
                    &BTreeSet::new(),
                    &mut levels_handlers,
                    &mut local_stats,
                    &HashMap::default(),
                    Arc::new(CompactionDeveloperConfig::default()),
                    &Default::default(),
                    &HummockVersionStateTableInfo::empty(),
                ),
            )
            .unwrap();
        assert_compaction_task(&compaction, &levels_handlers);
        assert_eq!(compaction.input.input_levels[0].level_idx, 3);
        assert_eq!(compaction.input.target_level, 4);
        assert_eq!(
            compaction.input.input_levels[0]
                .table_infos
                .iter()
                .map(|sst| sst.sst_id)
                .collect_vec(),
            vec![5]
        );
        assert_eq!(
            compaction.input.input_levels[1]
                .table_infos
                .iter()
                .map(|sst| sst.sst_id)
                .collect_vec(),
            vec![10]
        );
        assert_eq!(
            compaction.target_file_size,
            config.target_file_size_base * 2
        );
        assert_eq!(compaction.compression_algorithm.as_str(), "Lz4",);
        // no compaction need to be scheduled because we do not calculate the size of pending files
        // to score.
        let compaction = selector.pick_compaction(
            2,
            compaction_selector_context(
                &group_config,
                &levels,
                &BTreeSet::new(),
                &mut levels_handlers,
                &mut local_stats,
                &HashMap::default(),
                Arc::new(CompactionDeveloperConfig::default()),
                &Default::default(),
                &HummockVersionStateTableInfo::empty(),
            ),
        );
        assert!(compaction.is_none());
    }

    #[test]
    fn test_compact_pending_bytes() {
        let config = CompactionConfigBuilder::new()
            .max_bytes_for_level_base(100)
            .max_level(4)
            .max_bytes_for_level_multiplier(5)
            .compaction_mode(CompactionMode::Range as i32)
            .build();
        let levels = vec![
            generate_level(1, vec![]),
            generate_level(2, generate_tables(0..50, 0..1000, 3, 500)),
            generate_level(3, generate_tables(30..60, 0..1000, 2, 500)),
            generate_level(4, generate_tables(60..70, 0..1000, 1, 1000)),
        ];
        let levels = Levels {
            levels,
            l0: generate_l0_nonoverlapping_sublevels(generate_tables(15..25, 0..600, 3, 100)),
            ..Default::default()
        };

        let dynamic_level_core = DynamicLevelSelectorCore::new(
            Arc::new(config),
            Arc::new(CompactionDeveloperConfig::default()),
        );
        let ctx = dynamic_level_core.calculate_level_base_size(&levels);
        assert_eq!(1, ctx.base_level);
        assert_eq!(1000, levels.l0.total_file_size); // l0
        assert_eq!(0, levels.levels.first().unwrap().total_file_size); // l1
        assert_eq!(25000, levels.levels.get(1).unwrap().total_file_size); // l2
        assert_eq!(15000, levels.levels.get(2).unwrap().total_file_size); // l3
        assert_eq!(10000, levels.levels.get(3).unwrap().total_file_size); // l4

        assert_eq!(100, ctx.level_max_bytes[1]); // l1
        assert_eq!(500, ctx.level_max_bytes[2]); // l2
        assert_eq!(2500, ctx.level_max_bytes[3]); // l3
        assert_eq!(12500, ctx.level_max_bytes[4]); // l4

        // l1 pending = (0 + 1000 - 100) * ((25000 / 1000) + 1) + 1000 = 24400
        // l2 pending = (25000 + 900 - 500) * ((15000 / (25000 + 900)) + 1) = 40110
        // l3 pending = (15000 + 25400 - 2500) * ((10000 / (15000 + 25400) + 1)) = 47281

        let compact_pending_bytes = dynamic_level_core.compact_pending_bytes_needed(&levels);
        assert_eq!(24400 + 40110 + 47281, compact_pending_bytes);
    }
}

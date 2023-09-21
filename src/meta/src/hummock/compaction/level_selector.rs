//  Copyright 2023 RisingWave Labs
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
use std::collections::HashMap;
use std::sync::Arc;

use risingwave_common::catalog::TableOption;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockLevelsExt;
use risingwave_hummock_sdk::HummockCompactionTaskId;
use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::{compact_task, CompactionConfig, LevelType};

use super::picker::{
    CompactionTaskValidator, EmergencyCompactionPicker, IntraCompactionPicker,
    SpaceReclaimCompactionPicker, SpaceReclaimPickerState, TtlPickerState,
    TtlReclaimCompactionPicker,
};
use super::{
    create_compaction_task, ManualCompactionOption, ManualCompactionPicker,
    PartitionLevelCompactionPicker, TierCompactionPicker,
};
use crate::hummock::compaction::overlap_strategy::OverlapStrategy;
use crate::hummock::compaction::picker::{
    partition_level, partition_sub_levels, CompactionPicker, LevelCompactionPicker, LevelPartition,
    LocalPickerStatistic, MinOverlappingPicker, PartitionIntraSubLevelPicker,
    PartitionMinOverlappingPicker,
};
use crate::hummock::compaction::{create_overlap_strategy, CompactionTask, LocalSelectorStatistic};
use crate::hummock::level_handler::LevelHandler;
use crate::hummock::model::CompactionGroup;
use crate::rpc::metrics::MetaMetrics;

pub const SCORE_BASE: u64 = 100;

#[derive(Debug, Default, Clone)]
pub enum PickerType {
    L0Tier,
    PartitionBaseLevel(Vec<LevelPartition>),
    L0ToBase,
    PartitionIntralSubLevel(Vec<LevelPartition>),
    IntralSubLevel,
    #[default]
    BottomLevelCompaction,
    BaseLevelCompaction(Vec<LevelPartition>),
}

#[derive(Default, Debug)]
pub struct PickerInfo {
    score: u64,
    select_level: usize,
    target_level: usize,
    picker_type: PickerType,
}

pub trait LevelSelector: Sync + Send {
    fn pick_compaction(
        &mut self,
        task_id: HummockCompactionTaskId,
        group: &CompactionGroup,
        levels: &Levels,
        level_handlers: &mut [LevelHandler],
        selector_stats: &mut LocalSelectorStatistic,
        table_id_to_options: HashMap<u32, TableOption>,
    ) -> Option<CompactionTask>;

    fn report_statistic_metrics(&self, _metrics: &MetaMetrics) {}

    fn name(&self) -> &'static str;

    fn task_type(&self) -> compact_task::TaskType;
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
    pub target_partitions: Vec<LevelPartition>,
    pub max_bytes_for_level_base: u64,
}

pub struct DynamicLevelSelectorCore {
    config: Arc<CompactionConfig>,
}

#[derive(Default)]
pub struct DynamicLevelSelector {}

impl DynamicLevelSelectorCore {
    pub fn new(config: Arc<CompactionConfig>) -> Self {
        Self { config }
    }

    pub fn get_config(&self) -> &CompactionConfig {
        self.config.as_ref()
    }

    fn create_compaction_picker(
        &self,
        picker_info: PickerInfo,
        overlap_strategy: Arc<dyn OverlapStrategy>,
        compaction_task_validator: Arc<CompactionTaskValidator>,
    ) -> Box<dyn CompactionPicker> {
        match picker_info.picker_type {
            PickerType::L0Tier => Box::new(TierCompactionPicker::new_with_validator(
                self.config.clone(),
                compaction_task_validator,
            )),
            PickerType::PartitionBaseLevel(parts) => Box::new(PartitionLevelCompactionPicker::new(
                picker_info.target_level,
                self.config.clone(),
                parts,
            )),
            PickerType::L0ToBase => Box::new(LevelCompactionPicker::new_with_validator(
                picker_info.target_level,
                self.config.clone(),
                compaction_task_validator,
            )),
            PickerType::BottomLevelCompaction => {
                assert_eq!(picker_info.select_level + 1, picker_info.target_level);
                Box::new(MinOverlappingPicker::new(
                    picker_info.select_level,
                    picker_info.target_level,
                    self.config.max_bytes_for_level_base,
                    self.config.split_by_state_table,
                    overlap_strategy,
                ))
            }
            PickerType::BaseLevelCompaction(partitions) => {
                Box::new(PartitionMinOverlappingPicker::new(
                    picker_info.select_level,
                    picker_info.target_level,
                    self.config.max_bytes_for_level_base,
                    overlap_strategy,
                    partitions,
                ))
            }
            PickerType::PartitionIntralSubLevel(partitions) => Box::new(
                PartitionIntraSubLevelPicker::new(self.config.clone(), partitions),
            ),
            PickerType::IntralSubLevel => Box::new(IntraCompactionPicker::new_with_validator(
                self.config.clone(),
                compaction_task_validator,
            )),
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

        if levels.can_partition_by_vnode() {
            ctx.target_partitions =
                vec![LevelPartition::default(); levels.vnode_partition_count as usize];
        }

        for level in &levels.levels {
            if level.total_file_size > 0 && first_non_empty_level == 0 {
                first_non_empty_level = level.level_idx as usize;
            }
            max_level_size = std::cmp::max(max_level_size, level.total_file_size);
        }

        ctx.level_max_bytes
            .resize(self.config.max_level as usize + 1, u64::MAX);

        ctx.max_bytes_for_level_base = self.config.max_bytes_for_level_base;
        if levels.vnode_partition_count > 4 {
            ctx.max_bytes_for_level_base *= levels.vnode_partition_count as u64 / 4;
        }

        if max_level_size == 0 {
            // Use the bottommost level.
            ctx.base_level = self.config.max_level as usize;
            return ctx;
        }

        let base_bytes_max = ctx.max_bytes_for_level_base;
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

        if levels.can_partition_by_vnode()
            && !partition_level(
                levels.member_table_ids[0],
                levels.vnode_partition_count as usize,
                levels.get_level(ctx.base_level),
                &mut ctx.target_partitions,
            )
        {
            ctx.target_partitions.clear();
        }

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

    fn get_priority_levels(&self, levels: &Levels, handlers: &[LevelHandler]) -> SelectContext {
        let mut ctx = self.calculate_level_base_size(levels);

        let idle_file_count = levels
            .l0
            .as_ref()
            .unwrap()
            .sub_levels
            .iter()
            .map(|level| level.table_infos.len())
            .sum::<usize>()
            - handlers[0].get_pending_file_count();

        if idle_file_count > 0 {
            // trigger l0 compaction when the number of files is too large.

            // The read query at the overlapping level needs to merge all the ssts, so the number of
            // ssts is the most important factor affecting the read performance, we use file count
            // to calculate the score
            let overlapping_file_count = levels
                .l0
                .as_ref()
                .unwrap()
                .sub_levels
                .iter()
                .filter(|level| level.level_type() == LevelType::Overlapping)
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
                    picker_type: PickerType::L0Tier,
                });
            }

            // The read query at the non-overlapping level only selects ssts that match the query
            // range at each level, so the number of levels is the most important factor affecting
            // the read performance. At the same time, the size factor is also added to the score
            // calculation rule to avoid unbalanced compact task due to large size.
            let total_size = levels.l0.as_ref().unwrap().total_file_size
                - handlers[0].get_pending_output_file_size(ctx.base_level as u32);
            let base_level_size = levels.get_level(ctx.base_level).total_file_size;

            // size limit
            let mut non_overlapping_size_score = total_size * SCORE_BASE
                / std::cmp::max(ctx.max_bytes_for_level_base, base_level_size);

            // level count limit
            let non_overlapping_level_count = levels
                .l0
                .as_ref()
                .unwrap()
                .sub_levels
                .iter()
                .filter(|level| level.level_type() == LevelType::Nonoverlapping)
                .count() as u64;
            let non_overlapping_level_score = non_overlapping_level_count * SCORE_BASE
                / self.config.level0_sub_level_compact_level_count as u64;
            let partitions = if non_overlapping_level_score > SCORE_BASE
                && levels.can_partition_by_vnode()
            {
                // Reduce the level num of l0 non-overlapping sub_level
                let partitions = partition_sub_levels(levels);
                for part in &partitions {
                    non_overlapping_size_score = std::cmp::max(
                        non_overlapping_size_score,
                        part.total_file_size * SCORE_BASE / self.config.max_bytes_for_level_base,
                    );
                }
                partitions
            } else {
                vec![]
            };

            if non_overlapping_size_score > SCORE_BASE {
                if levels.can_partition_by_vnode() {
                    ctx.score_levels.push(PickerInfo {
                        score: non_overlapping_size_score,
                        select_level: 0,
                        target_level: ctx.base_level,
                        picker_type: PickerType::PartitionBaseLevel(ctx.target_partitions.clone()),
                    });
                } else {
                    ctx.score_levels.push(PickerInfo {
                        score: non_overlapping_size_score,
                        select_level: 0,
                        target_level: ctx.base_level,
                        picker_type: PickerType::L0ToBase,
                    });
                }
            } else if non_overlapping_level_score > SCORE_BASE {
                if levels.can_partition_by_vnode() {
                    ctx.score_levels.push(PickerInfo {
                        score: non_overlapping_level_score,
                        select_level: 0,
                        target_level: 0,
                        picker_type: PickerType::PartitionIntralSubLevel(partitions),
                    });
                } else {
                    ctx.score_levels.push(PickerInfo {
                        score: non_overlapping_level_score,
                        select_level: 0,
                        target_level: 0,
                        picker_type: PickerType::IntralSubLevel,
                    });
                }
            }
        }

        // The bottommost level can not be input level.
        for level in &levels.levels {
            let level_idx = level.level_idx as usize;
            if level_idx < ctx.base_level || level_idx >= self.config.max_level as usize {
                continue;
            }
            let upper_level = if level_idx == ctx.base_level {
                0
            } else {
                level_idx - 1
            };
            let total_size = level.total_file_size
                + handlers[upper_level].get_pending_output_file_size(level.level_idx)
                - handlers[level_idx].get_pending_output_file_size(level.level_idx + 1);
            if total_size == 0 {
                continue;
            }

            let mut score = total_size * SCORE_BASE / ctx.level_max_bytes[level_idx];
            if level.level_idx as usize == ctx.base_level
                && level.vnode_partition_count != 0
                && levels.can_partition_by_vnode()
                && !ctx.target_partitions.is_empty()
            {
                let max_size = ctx
                    .target_partitions
                    .iter()
                    .map(|part| part.total_file_size)
                    .max()
                    .unwrap();
                score = std::cmp::max(
                    score,
                    max_size * SCORE_BASE * (level.vnode_partition_count as u64)
                        / ctx.level_max_bytes[level_idx],
                );
                ctx.score_levels.push(PickerInfo {
                    score,
                    select_level: level_idx,
                    target_level: level_idx + 1,
                    picker_type: PickerType::BaseLevelCompaction(ctx.target_partitions.clone()),
                });
            } else {
                ctx.score_levels.push(PickerInfo {
                    score,
                    select_level: level_idx,
                    target_level: level_idx + 1,
                    picker_type: PickerType::BottomLevelCompaction,
                });
            }
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
            .as_ref()
            .unwrap()
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
            let level_index = level.get_level_idx() as usize;

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

impl LevelSelector for DynamicLevelSelector {
    fn pick_compaction(
        &mut self,
        task_id: HummockCompactionTaskId,
        compaction_group: &CompactionGroup,
        levels: &Levels,
        level_handlers: &mut [LevelHandler],
        selector_stats: &mut LocalSelectorStatistic,
        _table_id_to_options: HashMap<u32, TableOption>,
    ) -> Option<CompactionTask> {
        let dynamic_level_core =
            DynamicLevelSelectorCore::new(compaction_group.compaction_config.clone());
        let overlap_strategy =
            create_overlap_strategy(compaction_group.compaction_config.compaction_mode());
        let ctx = dynamic_level_core.get_priority_levels(levels, level_handlers);
        let compaction_task_validator = Arc::new(CompactionTaskValidator::new(
            compaction_group.compaction_config.clone(),
        ));
        for info in ctx.score_levels {
            if info.score <= SCORE_BASE {
                return None;
            }
            let select_level = info.select_level;
            let target_level = info.target_level;
            let mut picker = dynamic_level_core.create_compaction_picker(
                info,
                overlap_strategy.clone(),
                compaction_task_validator.clone(),
            );

            let mut stats = LocalPickerStatistic::default();
            if let Some(mut ret) = picker.pick_compaction(levels, level_handlers, &mut stats) {
                ret.add_pending_task(task_id, level_handlers);
                if !levels.can_partition_by_vnode() {
                    ret.vnode_partition_count = 0;
                }
                if stats.use_vnode_partition {
                    selector_stats.vnode_partition_task_count += 1;
                }
                return Some(create_compaction_task(
                    task_id,
                    dynamic_level_core.get_config(),
                    ret,
                    ctx.base_level,
                    self.task_type(),
                ));
            }
            selector_stats
                .skip_picker
                .push((select_level, target_level, stats));
        }
        None
    }

    fn name(&self) -> &'static str {
        "DynamicLevelSelector"
    }

    fn task_type(&self) -> compact_task::TaskType {
        compact_task::TaskType::Dynamic
    }
}

pub struct ManualCompactionSelector {
    option: ManualCompactionOption,
}

impl ManualCompactionSelector {
    pub fn new(option: ManualCompactionOption) -> Self {
        Self { option }
    }
}

impl LevelSelector for ManualCompactionSelector {
    fn pick_compaction(
        &mut self,
        task_id: HummockCompactionTaskId,
        group: &CompactionGroup,
        levels: &Levels,
        level_handlers: &mut [LevelHandler],
        _selector_stats: &mut LocalSelectorStatistic,
        _table_id_to_options: HashMap<u32, TableOption>,
    ) -> Option<CompactionTask> {
        let overlap_strategy = create_overlap_strategy(group.compaction_config.compaction_mode());
        let dynamic_level_core = DynamicLevelSelectorCore::new(group.compaction_config.clone());
        let base_level = dynamic_level_core
            .calculate_level_base_size(levels)
            .base_level;
        let target_level = if self.option.level == 0 {
            base_level
        } else if self.option.level == group.compaction_config.max_level as usize {
            self.option.level
        } else {
            self.option.level + 1
        };
        let mut picker =
            ManualCompactionPicker::new(overlap_strategy, self.option.clone(), target_level);
        let compaction_input =
            picker.pick_compaction(levels, level_handlers, &mut LocalPickerStatistic::default())?;
        compaction_input.add_pending_task(task_id, level_handlers);

        Some(create_compaction_task(
            task_id,
            group.compaction_config.as_ref(),
            compaction_input,
            base_level,
            self.task_type(),
        ))
    }

    fn name(&self) -> &'static str {
        "ManualCompactionSelector"
    }

    fn task_type(&self) -> compact_task::TaskType {
        compact_task::TaskType::Manual
    }
}

#[derive(Default)]
pub struct SpaceReclaimCompactionSelector {
    state: HashMap<u64, SpaceReclaimPickerState>,
}

impl LevelSelector for SpaceReclaimCompactionSelector {
    fn pick_compaction(
        &mut self,
        task_id: HummockCompactionTaskId,
        group: &CompactionGroup,
        levels: &Levels,
        level_handlers: &mut [LevelHandler],
        _selector_stats: &mut LocalSelectorStatistic,
        _table_id_to_options: HashMap<u32, TableOption>,
    ) -> Option<CompactionTask> {
        let dynamic_level_core = DynamicLevelSelectorCore::new(group.compaction_config.clone());
        let mut picker = SpaceReclaimCompactionPicker::new(
            group.compaction_config.max_space_reclaim_bytes,
            levels.member_table_ids.iter().cloned().collect(),
        );
        let base_level = dynamic_level_core
            .calculate_level_base_size(levels)
            .base_level;
        let state = self.state.entry(group.group_id).or_default();

        let compaction_input = picker.pick_compaction(levels, level_handlers, state)?;
        compaction_input.add_pending_task(task_id, level_handlers);

        Some(create_compaction_task(
            task_id,
            dynamic_level_core.get_config(),
            compaction_input,
            base_level,
            self.task_type(),
        ))
    }

    fn name(&self) -> &'static str {
        "SpaceReclaimCompaction"
    }

    fn task_type(&self) -> compact_task::TaskType {
        compact_task::TaskType::SpaceReclaim
    }
}

#[derive(Default)]
pub struct TtlCompactionSelector {
    state: HashMap<u64, TtlPickerState>,
}

impl LevelSelector for TtlCompactionSelector {
    fn pick_compaction(
        &mut self,
        task_id: HummockCompactionTaskId,
        group: &CompactionGroup,
        levels: &Levels,
        level_handlers: &mut [LevelHandler],
        _selector_stats: &mut LocalSelectorStatistic,
        table_id_to_options: HashMap<u32, TableOption>,
    ) -> Option<CompactionTask> {
        let mut base_level = 1;
        while base_level < levels.levels.len()
            && levels.get_level(base_level).table_infos.is_empty()
        {
            base_level += 1;
        }
        let picker = TtlReclaimCompactionPicker::new(
            group.compaction_config.max_space_reclaim_bytes,
            table_id_to_options,
        );
        let state = self.state.entry(group.group_id).or_default();
        let compaction_input = picker.pick_compaction(levels, level_handlers, state)?;
        compaction_input.add_pending_task(task_id, level_handlers);

        Some(create_compaction_task(
            task_id,
            group.compaction_config.as_ref(),
            compaction_input,
            base_level,
            self.task_type(),
        ))
    }

    fn name(&self) -> &'static str {
        "TtlCompaction"
    }

    fn task_type(&self) -> compact_task::TaskType {
        compact_task::TaskType::Ttl
    }
}

pub fn default_level_selector() -> Box<dyn LevelSelector> {
    Box::<DynamicLevelSelector>::default()
}

#[derive(Default)]
pub struct EmergencySelector {}

impl LevelSelector for EmergencySelector {
    fn pick_compaction(
        &mut self,
        task_id: HummockCompactionTaskId,
        group: &CompactionGroup,
        levels: &Levels,
        level_handlers: &mut [LevelHandler],
        selector_stats: &mut LocalSelectorStatistic,
        _table_id_to_options: HashMap<u32, TableOption>,
    ) -> Option<CompactionTask> {
        let dynamic_level_core = DynamicLevelSelectorCore::new(group.compaction_config.clone());
        let ctx = dynamic_level_core.calculate_level_base_size(levels);
        let picker =
            EmergencyCompactionPicker::new(ctx.base_level, group.compaction_config.clone());

        let mut stats = LocalPickerStatistic::default();
        if let Some(compaction_input) = picker.pick_compaction(levels, level_handlers, &mut stats) {
            compaction_input.add_pending_task(task_id, level_handlers);

            return Some(create_compaction_task(
                task_id,
                group.compaction_config.as_ref(),
                compaction_input,
                ctx.base_level,
                self.task_type(),
            ));
        }

        selector_stats.skip_picker.push((0, ctx.base_level, stats));

        None
    }

    fn name(&self) -> &'static str {
        "EmergencyCompaction"
    }

    fn task_type(&self) -> compact_task::TaskType {
        compact_task::TaskType::Emergency
    }
}

#[cfg(test)]
pub mod tests {
    use std::ops::Range;

    use itertools::Itertools;
    use risingwave_common::constants::hummock::CompactionFilterFlag;
    use risingwave_pb::hummock::compaction_config::CompactionMode;
    use risingwave_pb::hummock::{KeyRange, Level, LevelType, OverlappingLevel, SstableInfo};

    use super::*;
    use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
    use crate::hummock::test_utils::iterator_test_key_of_epoch;

    pub fn push_table_level0_overlapping(levels: &mut Levels, sst: SstableInfo) {
        levels.l0.as_mut().unwrap().total_file_size += sst.file_size;
        levels.l0.as_mut().unwrap().sub_levels.push(Level {
            level_idx: 0,
            level_type: LevelType::Overlapping as i32,
            total_file_size: sst.file_size,
            uncompressed_file_size: sst.uncompressed_file_size,
            sub_level_id: sst.get_sst_id(),
            table_infos: vec![sst],
            vnode_partition_count: 0,
        });
    }

    pub fn push_table_level0_nonoverlapping(levels: &mut Levels, sst: SstableInfo) {
        push_table_level0_overlapping(levels, sst);
        levels
            .l0
            .as_mut()
            .unwrap()
            .sub_levels
            .last_mut()
            .unwrap()
            .level_type = LevelType::Nonoverlapping as i32;
    }

    pub fn push_tables_level0_nonoverlapping(levels: &mut Levels, table_infos: Vec<SstableInfo>) {
        let total_file_size = table_infos.iter().map(|table| table.file_size).sum::<u64>();
        let uncompressed_file_size = table_infos
            .iter()
            .map(|table| table.uncompressed_file_size)
            .sum();
        let sub_level_id = table_infos[0].get_sst_id();
        levels.l0.as_mut().unwrap().total_file_size += total_file_size;
        levels.l0.as_mut().unwrap().sub_levels.push(Level {
            level_idx: 0,
            level_type: LevelType::Nonoverlapping as i32,
            total_file_size,
            sub_level_id,
            table_infos,
            uncompressed_file_size,
            vnode_partition_count: 0,
        });
    }

    pub fn generate_table(
        id: u64,
        table_prefix: u64,
        left: usize,
        right: usize,
        epoch: u64,
    ) -> SstableInfo {
        SstableInfo {
            object_id: id,
            sst_id: id,
            key_range: Some(KeyRange {
                left: iterator_test_key_of_epoch(table_prefix, left, epoch),
                right: iterator_test_key_of_epoch(table_prefix, right, epoch),
                right_exclusive: false,
            }),
            file_size: (right - left + 1) as u64,
            table_ids: vec![table_prefix as u32],
            uncompressed_file_size: (right - left + 1) as u64,
            total_key_count: (right - left + 1) as u64,
            ..Default::default()
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn generate_table_with_ids_and_epochs(
        id: u64,
        table_prefix: u64,
        left: usize,
        right: usize,
        epoch: u64,
        table_ids: Vec<u32>,
        min_epoch: u64,
        max_epoch: u64,
    ) -> SstableInfo {
        SstableInfo {
            object_id: id,
            sst_id: id,
            key_range: Some(KeyRange {
                left: iterator_test_key_of_epoch(table_prefix, left, epoch),
                right: iterator_test_key_of_epoch(table_prefix, right, epoch),
                right_exclusive: false,
            }),
            file_size: (right - left + 1) as u64,
            table_ids,
            uncompressed_file_size: (right - left + 1) as u64,
            min_epoch,
            max_epoch,
            ..Default::default()
        }
    }

    pub fn generate_tables(
        ids: Range<u64>,
        keys: Range<usize>,
        epoch: u64,
        file_size: u64,
    ) -> Vec<SstableInfo> {
        let step = (keys.end - keys.start) / (ids.end - ids.start) as usize;
        let mut start = keys.start;
        let mut tables = vec![];
        for id in ids {
            let mut table = generate_table(id, 1, start, start + step - 1, epoch);
            table.file_size = file_size;
            tables.push(table);
            start += step;
        }
        tables
    }

    pub fn generate_level(level_idx: u32, table_infos: Vec<SstableInfo>) -> Level {
        let total_file_size = table_infos.iter().map(|sst| sst.file_size).sum();
        let uncompressed_file_size = table_infos
            .iter()
            .map(|sst| sst.uncompressed_file_size)
            .sum();
        Level {
            level_idx,
            level_type: LevelType::Nonoverlapping as i32,
            table_infos,
            total_file_size,
            sub_level_id: 0,
            uncompressed_file_size,
            vnode_partition_count: 0,
        }
    }

    /// Returns a `OverlappingLevel`, with each `table_infos`'s element placed in a nonoverlapping
    /// sub-level.
    pub fn generate_l0_nonoverlapping_sublevels(table_infos: Vec<SstableInfo>) -> OverlappingLevel {
        let total_file_size = table_infos.iter().map(|table| table.file_size).sum::<u64>();
        let uncompressed_file_size = table_infos
            .iter()
            .map(|table| table.uncompressed_file_size)
            .sum::<u64>();
        OverlappingLevel {
            sub_levels: table_infos
                .into_iter()
                .enumerate()
                .map(|(idx, table)| Level {
                    level_idx: 0,
                    level_type: LevelType::Nonoverlapping as i32,
                    total_file_size: table.file_size,
                    uncompressed_file_size: table.uncompressed_file_size,
                    sub_level_id: idx as u64,
                    table_infos: vec![table],
                    vnode_partition_count: 0,
                })
                .collect_vec(),
            total_file_size,
            uncompressed_file_size,
        }
    }

    pub fn generate_l0_nonoverlapping_multi_sublevels(
        table_infos: Vec<Vec<SstableInfo>>,
    ) -> OverlappingLevel {
        let mut l0 = OverlappingLevel {
            sub_levels: table_infos
                .into_iter()
                .enumerate()
                .map(|(idx, table)| Level {
                    level_idx: 0,
                    level_type: LevelType::Nonoverlapping as i32,
                    total_file_size: table.iter().map(|table| table.file_size).sum::<u64>(),
                    uncompressed_file_size: table
                        .iter()
                        .map(|sst| sst.uncompressed_file_size)
                        .sum::<u64>(),
                    sub_level_id: idx as u64,
                    table_infos: table,
                    vnode_partition_count: 0,
                })
                .collect_vec(),
            total_file_size: 0,
            uncompressed_file_size: 0,
        };

        l0.total_file_size = l0.sub_levels.iter().map(|l| l.total_file_size).sum::<u64>();
        l0.uncompressed_file_size = l0
            .sub_levels
            .iter()
            .map(|l| l.uncompressed_file_size)
            .sum::<u64>();
        l0
    }

    /// Returns a `OverlappingLevel`, with each `table_infos`'s element placed in a overlapping
    /// sub-level.
    pub fn generate_l0_overlapping_sublevels(
        table_infos: Vec<Vec<SstableInfo>>,
    ) -> OverlappingLevel {
        let mut l0 = OverlappingLevel {
            sub_levels: table_infos
                .into_iter()
                .enumerate()
                .map(|(idx, table)| Level {
                    level_idx: 0,
                    level_type: LevelType::Overlapping as i32,
                    total_file_size: table.iter().map(|table| table.file_size).sum::<u64>(),
                    sub_level_id: idx as u64,
                    table_infos: table.clone(),
                    uncompressed_file_size: table
                        .iter()
                        .map(|sst| sst.uncompressed_file_size)
                        .sum::<u64>(),
                    vnode_partition_count: 0,
                })
                .collect_vec(),
            total_file_size: 0,
            uncompressed_file_size: 0,
        };
        l0.total_file_size = l0.sub_levels.iter().map(|l| l.total_file_size).sum::<u64>();
        l0.uncompressed_file_size = l0
            .sub_levels
            .iter()
            .map(|l| l.uncompressed_file_size)
            .sum::<u64>();
        l0
    }

    pub(crate) fn assert_compaction_task(
        compact_task: &CompactionTask,
        level_handlers: &[LevelHandler],
    ) {
        for i in &compact_task.input.input_levels {
            for t in &i.table_infos {
                assert!(level_handlers[i.level_idx as usize].is_pending_compact(&t.sst_id));
            }
        }
    }

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
        let selector = DynamicLevelSelectorCore::new(Arc::new(config));
        let levels = vec![
            generate_level(1, vec![]),
            generate_level(2, generate_tables(0..5, 0..1000, 3, 10)),
            generate_level(3, generate_tables(5..10, 0..1000, 2, 50)),
            generate_level(4, generate_tables(10..15, 0..1000, 1, 200)),
        ];
        let mut levels = Levels {
            levels,
            l0: Some(generate_l0_nonoverlapping_sublevels(vec![])),
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
            .map(|sst| sst.file_size)
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

        levels.l0.as_mut().unwrap().sub_levels.clear();
        levels.l0.as_mut().unwrap().total_file_size = 0;
        levels.levels[0].table_infos = generate_tables(26..32, 0..1000, 1, 100);
        levels.levels[0].total_file_size = levels.levels[0]
            .table_infos
            .iter()
            .map(|sst| sst.file_size)
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
            .level0_sub_level_compact_level_count(1)
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
            l0: Some(generate_l0_nonoverlapping_sublevels(generate_tables(
                15..25,
                0..600,
                3,
                10,
            ))),
            member_table_ids: vec![1],
            ..Default::default()
        };

        let mut selector = DynamicLevelSelector::default();
        let mut levels_handlers = (0..5).map(LevelHandler::new).collect_vec();
        let mut local_stats = LocalSelectorStatistic::default();
        let compaction = selector
            .pick_compaction(
                1,
                &group_config,
                &levels,
                &mut levels_handlers,
                &mut local_stats,
                HashMap::default(),
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

        levels.l0.as_mut().unwrap().sub_levels.clear();
        levels.l0.as_mut().unwrap().total_file_size = 0;
        push_tables_level0_nonoverlapping(&mut levels, generate_tables(15..25, 0..600, 3, 20));
        let mut levels_handlers = (0..5).map(LevelHandler::new).collect_vec();
        let compaction = selector
            .pick_compaction(
                1,
                &group_config,
                &levels,
                &mut levels_handlers,
                &mut local_stats,
                HashMap::default(),
            )
            .unwrap();
        assert_compaction_task(&compaction, &levels_handlers);
        assert_eq!(compaction.input.input_levels[0].level_idx, 0);
        assert_eq!(compaction.input.target_level, 2);

        levels_handlers[0].remove_task(1);
        levels_handlers[2].remove_task(1);
        levels.l0.as_mut().unwrap().sub_levels.clear();
        levels.levels[1].table_infos = generate_tables(20..30, 0..1000, 3, 10);
        let compaction = selector
            .pick_compaction(
                2,
                &group_config,
                &levels,
                &mut levels_handlers,
                &mut local_stats,
                HashMap::default(),
            )
            .unwrap();
        assert_compaction_task(&compaction, &levels_handlers);
        assert_eq!(compaction.input.input_levels[0].level_idx, 3);
        assert_eq!(compaction.input.target_level, 4);
        assert_eq!(
            compaction.input.input_levels[0]
                .table_infos
                .iter()
                .map(|sst| sst.get_sst_id())
                .collect_vec(),
            vec![5]
        );
        assert_eq!(
            compaction.input.input_levels[1]
                .table_infos
                .iter()
                .map(|sst| sst.get_sst_id())
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
            &group_config,
            &levels,
            &mut levels_handlers,
            &mut local_stats,
            HashMap::default(),
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
            l0: Some(generate_l0_nonoverlapping_sublevels(generate_tables(
                15..25,
                0..600,
                3,
                100,
            ))),
            ..Default::default()
        };

        let dynamic_level_core = DynamicLevelSelectorCore::new(Arc::new(config));
        let ctx = dynamic_level_core.calculate_level_base_size(&levels);
        assert_eq!(1, ctx.base_level);
        assert_eq!(1000, levels.l0.as_ref().unwrap().total_file_size); // l0
        assert_eq!(0, levels.levels.get(0).unwrap().total_file_size); // l1
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

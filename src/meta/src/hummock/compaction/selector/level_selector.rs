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

// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

use std::sync::Arc;

use risingwave_hummock_sdk::HummockCompactionTaskId;
use risingwave_hummock_sdk::level::{Levels, OverlappingLevel};
use risingwave_pb::hummock::compact_task::PbTaskType;
use risingwave_pb::hummock::{CompactionConfig, LevelType};

use super::single_table_compaction::{SingleTableCompactionGroup, SingleTableL0PickerType};
use super::{
    CompactionSelector, LevelCompactionPicker, PartitionL0CandidateInfo,
    PartitionL0CompactionObservation, TierCompactionPicker, create_compaction_task,
};
use crate::hummock::compaction::overlap_strategy::OverlapStrategy;
use crate::hummock::compaction::picker::{
    CompactionPicker, CompactionTaskValidator, IntraCompactionPicker, L0PickerMode,
    LocalPickerStatistic, MinOverlappingPicker,
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
    TrivialMove,
    #[default]
    BottomLevel,
}

impl std::fmt::Display for PickerType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            PickerType::Tier => "Tier",
            PickerType::Intra => "Intra",
            PickerType::ToBase => "ToBase",
            PickerType::TrivialMove => "TrivialMove",
            PickerType::BottomLevel => "BottomLevel",
        })
    }
}

impl PickerType {
    fn label(&self) -> &'static str {
        match self {
            PickerType::Tier => "tier",
            PickerType::Intra => "intra",
            PickerType::ToBase => "to-base",
            PickerType::TrivialMove => "trivial-move",
            PickerType::BottomLevel => "bottom-level",
        }
    }

    fn sort_rank(&self) -> u8 {
        match self {
            PickerType::Tier => 0,
            PickerType::ToBase => 1,
            PickerType::TrivialMove => 2,
            PickerType::Intra => 3,
            PickerType::BottomLevel => 4,
        }
    }
}

#[derive(Default, Debug)]
pub struct PickerInfo {
    pub score: u64,
    pub select_level: usize,
    pub target_level: usize,
    pub picker_type: PickerType,
    eligible: bool,
    input: PickerInput,
}

#[derive(Default, Debug)]
enum PickerInput {
    #[default]
    Global,
    SingleTablePartition {
        candidate: PartitionL0CandidateInfo,
        l0: Arc<OverlappingLevel>,
        min_l0_level_count: usize,
    },
}

impl PickerInput {
    fn partition_score(&self) -> u64 {
        match self {
            Self::Global => 0,
            Self::SingleTablePartition { candidate, .. } => candidate.partition_score,
        }
    }

    fn picker_mode(&self, picker_type: &PickerType) -> L0PickerMode {
        match self {
            Self::Global => L0PickerMode::Legacy,
            Self::SingleTablePartition { .. } if matches!(picker_type, PickerType::TrivialMove) => {
                L0PickerMode::SingleTablePartitionTrivialMove
            }
            Self::SingleTablePartition {
                min_l0_level_count, ..
            } => L0PickerMode::SingleTablePartition {
                min_l0_level_count: *min_l0_level_count,
            },
        }
    }

    fn partition_observation(
        &self,
        picker_type: &PickerType,
        outcome: &'static str,
        input: Option<&crate::hummock::compaction::picker::CompactionInput>,
        stats: &LocalPickerStatistic,
    ) -> Option<PartitionL0CompactionObservation> {
        let Self::SingleTablePartition {
            candidate,
            min_l0_level_count,
            ..
        } = self
        else {
            return None;
        };

        let selected_l0_level_count = input
            .map(|input| {
                input
                    .input_levels
                    .iter()
                    .filter(|level| level.level_idx == 0)
                    .count() as u64
            })
            .unwrap_or_default();
        let selected_l0_sst_ref_count = input
            .map(|input| {
                input
                    .input_levels
                    .iter()
                    .filter(|level| level.level_idx == 0)
                    .map(|level| level.table_infos.len() as u64)
                    .sum()
            })
            .unwrap_or_default();
        let target_sst_ref_count = input
            .map(|input| {
                input
                    .input_levels
                    .iter()
                    .filter(|level| level.level_idx != 0)
                    .map(|level| level.table_infos.len() as u64)
                    .sum()
            })
            .unwrap_or_default();

        let picker = if stats.is_trivial_move {
            "trivial-move"
        } else {
            picker_type.label()
        };
        let selected_l0_referenced_object_size = input
            .map(|input| {
                input
                    .input_levels
                    .iter()
                    .filter(|level| level.level_idx == 0)
                    .flat_map(|level| &level.table_infos)
                    .map(|sst| sst.file_size)
                    .sum()
            })
            .unwrap_or_default();
        let target_referenced_object_size = input
            .map(|input| {
                input
                    .input_levels
                    .iter()
                    .filter(|level| level.level_idx != 0)
                    .flat_map(|level| &level.table_infos)
                    .map(|sst| sst.file_size)
                    .sum()
            })
            .unwrap_or_default();

        Some(PartitionL0CompactionObservation {
            candidate: *candidate,
            picker,
            outcome,
            min_seed_depth: *min_l0_level_count as u64,
            selected_l0_level_count,
            selected_l0_sst_ref_count,
            selected_l0_size: input
                .map(|input| input.select_input_size)
                .unwrap_or_default(),
            selected_l0_referenced_object_size,
            target_sst_ref_count,
            target_size: input
                .map(|input| input.target_input_size)
                .unwrap_or_default(),
            target_referenced_object_size,
            growth: stats.partition_l0_growth,
        })
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct EffectiveLevelSize {
    pub(super) current: u64,
    pub(super) incoming: u64,
    pub(super) outgoing: u64,
    pub(super) effective: u64,
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

fn effective_level_size_info(
    levels: &Levels,
    handlers: &[LevelHandler],
    level: usize,
) -> EffectiveLevelSize {
    let target_level = level as u32;
    let incoming_size = handlers
        .iter()
        .enumerate()
        .filter(|(source_level, _)| *source_level != level)
        .fold(0u64, |size, (_, handler)| {
            size.saturating_add(handler.pending_output_file_size(target_level))
        });
    let outgoing_size = handlers[level]
        .pending_file_size()
        .saturating_sub(handlers[level].pending_output_file_size(target_level));

    let current = levels.get_level(level).total_file_size;
    EffectiveLevelSize {
        current,
        incoming: incoming_size,
        outgoing: outgoing_size,
        effective: current
            .saturating_sub(outgoing_size)
            .saturating_add(incoming_size),
    }
}

fn effective_level_size(levels: &Levels, handlers: &[LevelHandler], level: usize) -> u64 {
    effective_level_size_info(levels, handlers, level).effective
}

fn fill_score(level_size: u64, level_target_size: u64) -> u64 {
    let score =
        (level_size as u128) * (SCORE_BASE as u128) / (std::cmp::max(1, level_target_size) as u128);
    std::cmp::min(score, u64::MAX as u128) as u64
}

/// Scale a source-level fill score by the fill of its output level.
///
/// The 1% floor matches Pebble's protection against an unbounded priority when the output level is
/// empty. Admission remains separate: L0 must pass this adjusted score, while positive levels only
/// need their own fill score to exceed the configured target.
pub(super) fn adjust_score_for_output_level(
    source_score: u64,
    output_level_size: u64,
    output_level_target_size: u64,
) -> u64 {
    let output_target = std::cmp::max(1, output_level_target_size);
    let min_output_size = std::cmp::max(1, output_target / SCORE_BASE);
    let denominator = std::cmp::max(min_output_size, output_level_size);
    let score = (source_score as u128) * (output_target as u128) / (denominator as u128);
    std::cmp::min(score, u64::MAX as u128) as u64
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
        let picker_mode = picker_info.input.picker_mode(&picker_info.picker_type);
        let compaction_task_validator = if picker_mode.is_single_table_partition() {
            Arc::new(CompactionTaskValidator::unused())
        } else {
            compaction_task_validator
        };
        match picker_info.picker_type {
            PickerType::Tier => Box::new(TierCompactionPicker::new_with_validator(
                self.config.clone(),
                compaction_task_validator,
            )),
            PickerType::ToBase | PickerType::TrivialMove => {
                Box::new(LevelCompactionPicker::new_with_mode(
                    picker_info.target_level,
                    self.config.clone(),
                    compaction_task_validator,
                    self.developer_config.clone(),
                    picker_mode,
                ))
            }
            PickerType::Intra if picker_mode.is_single_table_partition() => {
                Box::new(IntraCompactionPicker::new_with_mode(
                    self.config.clone(),
                    compaction_task_validator,
                    self.developer_config.clone(),
                    picker_mode,
                ))
            }
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
    /// reach. This algorithm refers to the implementation in  `https://github.com/facebook/rocksdb/blob/v7.2.2/db/version_set.cc#L3706`
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
        self.get_priority_levels_with_single_table_strategy(levels, handlers, None)
    }

    fn get_priority_levels_with_single_table_strategy(
        &self,
        levels: &Levels,
        handlers: &[LevelHandler],
        single_table_compaction_group: Option<SingleTableCompactionGroup>,
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
                    eligible: true,
                    input: PickerInput::Global,
                })
            }

            let single_table_candidates = single_table_compaction_group.and_then(|group| {
                group.build_l0_candidates(
                    &self.config,
                    &levels.l0,
                    &handlers[0],
                    effective_level_size_info(levels, handlers, ctx.base_level),
                    ctx.level_max_bytes[ctx.base_level],
                )
            });
            if let Some(candidates) = single_table_candidates {
                ctx.score_levels
                    .extend(candidates.into_iter().map(|candidate| PickerInfo {
                        score: candidate.score,
                        select_level: 0,
                        target_level: match candidate.picker_type {
                            SingleTableL0PickerType::ToBase
                            | SingleTableL0PickerType::TrivialMove => ctx.base_level,
                            SingleTableL0PickerType::Intra => 0,
                        },
                        picker_type: match candidate.picker_type {
                            SingleTableL0PickerType::ToBase => PickerType::ToBase,
                            SingleTableL0PickerType::TrivialMove => PickerType::TrivialMove,
                            SingleTableL0PickerType::Intra => PickerType::Intra,
                        },
                        eligible: true,
                        input: PickerInput::SingleTablePartition {
                            candidate: candidate.info,
                            l0: candidate.l0,
                            min_l0_level_count: candidate.min_l0_level_count,
                        },
                    }));
            } else {
                self.add_legacy_l0_candidates(levels, handlers, &mut ctx);
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
            let legacy_level_size = level.total_file_size.saturating_sub(output_file_size);
            let use_inter_level_score = single_table_compaction_group.is_some();
            let level_size = if use_inter_level_score {
                effective_level_size(levels, handlers, level_idx)
            } else {
                legacy_level_size
            };
            if level_size == 0 {
                continue;
            }

            let raw_score = fill_score(level_size, ctx.level_max_bytes[level_idx]);
            let eligible = raw_score > SCORE_BASE;
            let score = if use_inter_level_score && eligible {
                let output_level = level_idx + 1;
                adjust_score_for_output_level(
                    raw_score,
                    effective_level_size(levels, handlers, output_level),
                    ctx.level_max_bytes[output_level],
                )
            } else {
                raw_score
            };
            ctx.score_levels.push({
                PickerInfo {
                    score,
                    select_level: level_idx,
                    target_level: level_idx + 1,
                    picker_type: PickerType::BottomLevel,
                    eligible,
                    input: PickerInput::Global,
                }
            });
        }

        // sort reverse to pick the largest one.
        ctx.score_levels.sort_by(|a, b| {
            b.score
                .cmp(&a.score)
                .then_with(|| a.target_level.cmp(&b.target_level))
                .then_with(|| a.picker_type.sort_rank().cmp(&b.picker_type.sort_rank()))
                .then_with(|| b.input.partition_score().cmp(&a.input.partition_score()))
        });
        ctx
    }

    fn add_legacy_l0_candidates(
        &self,
        levels: &Levels,
        handlers: &[LevelHandler],
        ctx: &mut SelectContext,
    ) {
        // Keep this path equivalent to the original dynamic-level selector. It is used by every
        // non-single-table compaction group and while a single-table group still contains SSTs
        // that cannot be represented by the fixed vnode partition layout.
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
        let size_score = total_size * SCORE_BASE
            / std::cmp::max(self.config.max_bytes_for_level_base, base_level_size);
        let non_overlapping_level_count = levels
            .l0
            .sub_levels
            .iter()
            .filter(|level| level.level_type == LevelType::Nonoverlapping)
            .count() as u64;
        let level_score = non_overlapping_level_count * SCORE_BASE
            / std::cmp::max(
                base_level_sst_count / 16,
                self.config.level0_sub_level_compact_level_count as u64,
            );
        let score = std::cmp::max(size_score, level_score);

        if size_score > SCORE_BASE {
            ctx.score_levels.push(PickerInfo {
                score: score + 1,
                select_level: 0,
                target_level: ctx.base_level,
                picker_type: PickerType::ToBase,
                eligible: true,
                input: PickerInput::Global,
            });
        }
        if level_score > SCORE_BASE {
            ctx.score_levels.push(PickerInfo {
                score,
                select_level: 0,
                target_level: 0,
                picker_type: PickerType::Intra,
                eligible: true,
                input: PickerInput::Global,
            });
        }
    }

    /// `compact_pending_bytes_needed` calculates the number of compact bytes needed to balance the
    /// LSM Tree from the current state of each level in the LSM Tree in combination with
    /// `compaction_config`
    /// This algorithm refers to the implementation in  `https://github.com/facebook/rocksdb/blob/main/db/version_set.cc#L3141`
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
            in_progress_compactions,
            single_table_compaction_group,
            ..
        } = context;
        let dynamic_level_core = DynamicLevelSelectorCore::new(
            compaction_group.compaction_config.clone(),
            developer_config,
        );
        let overlap_strategy =
            create_overlap_strategy(compaction_group.compaction_config.compaction_mode());
        let ctx = dynamic_level_core.get_priority_levels_with_single_table_strategy(
            levels,
            level_handlers,
            single_table_compaction_group,
        );
        // TODO: Determine which rule to enable by write limit
        let compaction_task_validator = Arc::new(CompactionTaskValidator::new(
            compaction_group.compaction_config.clone(),
        ));
        for picker_info in &ctx.score_levels {
            if !matches!(
                picker_info.picker_type,
                PickerType::ToBase | PickerType::TrivialMove
            ) {
                continue;
            }
            if let Some(observation) = picker_info.input.partition_observation(
                &picker_info.picker_type,
                "candidate",
                None,
                &LocalPickerStatistic::default(),
            ) {
                selector_stats.record_partition_l0(observation);
            }
        }
        for picker_info in &ctx.score_levels {
            if !picker_info.eligible {
                continue;
            }
            let single_table_levels;
            let picker_levels = match &picker_info.input {
                PickerInput::Global => levels,
                PickerInput::SingleTablePartition { l0, .. } => {
                    single_table_levels = Levels {
                        l0: l0.as_ref().clone(),
                        ..levels.clone()
                    };
                    &single_table_levels
                }
            };

            let mut stats = LocalPickerStatistic::default();
            let ret = if matches!(picker_info.input, PickerInput::SingleTablePartition { .. })
                && matches!(picker_info.picker_type, PickerType::ToBase)
            {
                // Only partition ToBase growth needs to test a wider output before replacing
                // its accepted seed. Legacy/Intra retain the common picker interface and route.
                LevelCompactionPicker::new_with_mode(
                    picker_info.target_level,
                    dynamic_level_core.config.clone(),
                    Arc::new(CompactionTaskValidator::unused()),
                    dynamic_level_core.developer_config.clone(),
                    picker_info.input.picker_mode(&picker_info.picker_type),
                )
                .pick_compaction_with_output_conflict_check(
                    picker_levels,
                    level_handlers,
                    &mut stats,
                    |input| in_progress_compactions.has_conflict_with_input(input),
                )
            } else {
                dynamic_level_core
                    .create_compaction_picker(
                        picker_info,
                        overlap_strategy.clone(),
                        compaction_task_validator.clone(),
                    )
                    .pick_compaction(picker_levels, level_handlers, &mut stats)
            };
            let Some(ret) = ret else {
                if let Some(observation) = picker_info.input.partition_observation(
                    &picker_info.picker_type,
                    "picker-empty",
                    None,
                    &stats,
                ) {
                    selector_stats.record_partition_l0(observation);
                }
                selector_stats.skip_picker.push((
                    picker_info.select_level,
                    picker_info.target_level,
                    stats,
                ));
                continue;
            };
            if !ret.skip_target_range_conflict_check
                && in_progress_compactions.has_conflict_with_input(&ret)
            {
                stats.skip_by_overlapping += 1;
                if let Some(observation) = picker_info.input.partition_observation(
                    &picker_info.picker_type,
                    "range-conflict",
                    Some(&ret),
                    &stats,
                ) {
                    selector_stats.record_partition_l0(observation);
                }
                selector_stats.skip_picker.push((
                    picker_info.select_level,
                    picker_info.target_level,
                    stats,
                ));
                continue;
            }

            if let Some(observation) = picker_info.input.partition_observation(
                &picker_info.picker_type,
                "selected",
                Some(&ret),
                &stats,
            ) {
                selector_stats.record_partition_l0(observation);
            }
            ret.add_pending_task(task_id, level_handlers);
            return Some(create_compaction_task(
                dynamic_level_core.get_config(),
                ret,
                ctx.base_level,
                self.task_type(),
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
    use risingwave_common::catalog::TableId;
    use risingwave_common::constants::hummock::CompactionFilterFlag;
    use risingwave_hummock_sdk::HummockCompactionTaskId;
    use risingwave_hummock_sdk::compact_task::{CompactTask, CompactTaskAssignment};
    use risingwave_hummock_sdk::level::{InputLevel, Levels};
    use risingwave_hummock_sdk::version::HummockVersionStateTableInfo;
    use risingwave_pb::hummock::compaction_config::CompactionMode;
    use risingwave_pb::hummock::{CompactionConfig, LevelType};

    use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
    use crate::hummock::compaction::in_progress_compaction::InProgressCompactionView;
    use crate::hummock::compaction::selector::tests::{
        assert_compaction_task, generate_l0_nonoverlapping_sublevels, generate_level,
        generate_table, generate_tables, push_tables_level0_nonoverlapping,
    };
    use crate::hummock::compaction::selector::{
        CompactionSelector, CompactionSelectorContext, DynamicLevelSelector,
        DynamicLevelSelectorCore, LocalSelectorStatistic, SingleTableCompactionGroup,
    };
    use crate::hummock::compaction::{CompactionDeveloperConfig, CompactionTask};
    use crate::hummock::level_handler::LevelHandler;
    use crate::hummock::model::CompactionGroup;
    use crate::hummock::test_utils::compaction_selector_context;

    fn pick_compaction_with_in_progress(
        selector: &mut DynamicLevelSelector,
        task_id: HummockCompactionTaskId,
        group: &CompactionGroup,
        levels: &Levels,
        level_handlers: &mut [LevelHandler],
        selector_stats: &mut LocalSelectorStatistic,
        in_progress_compactions: &InProgressCompactionView,
        single_table_compaction_group: Option<SingleTableCompactionGroup>,
    ) -> Option<CompactionTask> {
        selector.pick_compaction(
            task_id,
            CompactionSelectorContext {
                group,
                levels,
                member_table_ids: &BTreeSet::new(),
                single_table_compaction_group,
                level_handlers,
                selector_stats,
                table_id_to_options: &HashMap::default(),
                developer_config: Arc::new(CompactionDeveloperConfig::default()),
                table_watermarks: &HashMap::default(),
                state_table_info: &HummockVersionStateTableInfo::empty(),
                in_progress_compactions,
            },
        )
    }

    #[test]
    fn effective_level_size_accounts_for_pending_flow() {
        let base_sst = generate_table(1, 1, 0, 99, 1);
        let incoming_l0_sst = generate_table(2, 1, 0, 19, 2);
        let levels = Levels {
            levels: vec![generate_level(1, vec![base_sst.clone()])],
            ..Default::default()
        };
        let mut handlers = vec![LevelHandler::new(0), LevelHandler::new(1)];

        assert_eq!(super::effective_level_size(&levels, &handlers, 1), 100);

        handlers[1].add_pending_task(1, 2, [&base_sst]);
        handlers[0].add_pending_task(2, 1, [&incoming_l0_sst]);
        assert_eq!(super::effective_level_size(&levels, &handlers, 1), 20);
    }

    #[test]
    fn inter_level_score_uses_output_fill() {
        assert_eq!(super::fill_score(200, 100), 200);
        assert_eq!(super::adjust_score_for_output_level(200, 400, 100), 50);
        assert_eq!(super::adjust_score_for_output_level(200, 50, 100), 400);
        assert_eq!(super::adjust_score_for_output_level(200, 0, 100), 20_000);
    }

    #[test]
    fn single_table_strategy_adjusts_positive_level_priority_only() {
        let config = CompactionConfig {
            split_weight_by_vnode: 8,
            ..CompactionConfigBuilder::new()
                .max_bytes_for_level_base(100)
                .max_bytes_for_level_multiplier(10)
                .max_level(3)
                .build()
        };
        let core = DynamicLevelSelectorCore::new(
            Arc::new(config),
            Arc::new(CompactionDeveloperConfig::default()),
        );
        let levels = Levels {
            levels: vec![
                generate_level(1, generate_tables(1..2, 0..100, 1, 200)),
                generate_level(2, generate_tables(2..3, 0..100, 1, 4_000)),
                generate_level(3, generate_tables(3..4, 0..100, 1, 10_000)),
            ],
            ..Default::default()
        };
        let handlers = (0..=3).map(LevelHandler::new).collect_vec();

        let legacy = core.get_priority_levels(&levels, &handlers);
        let legacy_l1 = legacy
            .score_levels
            .iter()
            .find(|candidate| candidate.select_level == 1)
            .unwrap();
        assert_eq!(legacy_l1.score, 200);
        assert!(legacy_l1.eligible);

        let partition = core.get_priority_levels_with_single_table_strategy(
            &levels,
            &handlers,
            Some(SingleTableCompactionGroup::new(TableId::new(1), 256)),
        );
        let partition_l1 = partition
            .score_levels
            .iter()
            .find(|candidate| candidate.select_level == 1)
            .unwrap();
        assert_eq!(partition_l1.score, 50);
        // Positive levels stay eligible based on their own fill even when a fuller output level
        // lowers their scheduling priority below 1.0.
        assert!(partition_l1.eligible);
    }

    #[test]
    fn adjusted_positive_level_below_one_remains_runnable() {
        let config = CompactionConfig {
            split_weight_by_vnode: 8,
            ..CompactionConfigBuilder::new()
                .max_bytes_for_level_base(100)
                .max_bytes_for_level_multiplier(10)
                .max_level(2)
                .build()
        };
        let group = CompactionGroup::new(1, config);
        let levels = Levels {
            levels: vec![
                generate_level(1, generate_tables(1..2, 0..100, 1, 200)),
                generate_level(2, generate_tables(2..3, 0..100, 1, 4_000)),
            ],
            ..Default::default()
        };
        let mut handlers = (0..=2).map(LevelHandler::new).collect_vec();
        let mut selector = DynamicLevelSelector::default();
        let task = pick_compaction_with_in_progress(
            &mut selector,
            1,
            &group,
            &levels,
            &mut handlers,
            &mut LocalSelectorStatistic::default(),
            &InProgressCompactionView::default(),
            Some(SingleTableCompactionGroup::new(TableId::new(1), 256)),
        )
        .unwrap();

        assert_eq!(task.input.input_levels[0].level_idx, 1);
        assert_eq!(task.input.target_level, 2);
    }

    #[test]
    fn test_legacy_depth_pressure_uses_intra_without_size_pressure() {
        let config = CompactionConfig {
            split_weight_by_vnode: 8,
            ..CompactionConfigBuilder::new()
                .max_level(1)
                .max_bytes_for_level_base(1024)
                .level0_sub_level_compact_level_count(3)
                .sst_allowed_trivial_move_min_size(Some(u64::MAX))
                .build()
        };
        let group = CompactionGroup::new(1, config.clone());
        let levels = Levels {
            levels: vec![generate_level(1, vec![])],
            l0: generate_l0_nonoverlapping_sublevels(
                (1..=4).map(|id| generate_table(id, 1, 0, 10, id)).collect(),
            ),
            ..Default::default()
        };
        assert!(levels.l0.total_file_size < config.max_bytes_for_level_base);
        let mut handlers = vec![LevelHandler::new(0), LevelHandler::new(1)];
        let core = DynamicLevelSelectorCore::new(
            Arc::new(config),
            Arc::new(CompactionDeveloperConfig::default()),
        );
        let priorities = core.get_priority_levels(&levels, &handlers);
        assert!(
            !priorities
                .score_levels
                .iter()
                .any(|p| matches!(p.picker_type, super::PickerType::ToBase))
        );
        assert!(
            priorities
                .score_levels
                .iter()
                .any(|p| matches!(p.picker_type, super::PickerType::Intra))
        );
        let mut selector = DynamicLevelSelector::default();
        let task = pick_compaction_with_in_progress(
            &mut selector,
            1,
            &group,
            &levels,
            &mut handlers,
            &mut LocalSelectorStatistic::default(),
            &InProgressCompactionView::default(),
            None,
        )
        .unwrap();
        assert_eq!(task.input.target_level, 0);
        assert_compaction_task(&task, &handlers);
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
    fn test_trivial_move_skips_in_progress_target_overlap() {
        let config = CompactionConfigBuilder::new()
            .max_bytes_for_level_base(1000)
            .max_bytes_for_level_multiplier(10)
            .max_level(4)
            .max_compaction_bytes(10000)
            .level0_sub_level_compact_level_count(20)
            .sst_allowed_trivial_move_min_size(Some(0))
            .sst_allowed_trivial_move_max_count(Some(10))
            .compaction_mode(CompactionMode::Range as i32)
            .build();
        let group_config = CompactionGroup::new(9, config);
        let levels = Levels {
            levels: vec![
                generate_level(1, vec![]),
                generate_level(2, vec![]),
                generate_level(3, vec![]),
                generate_level(4, vec![generate_table(878784, 1, 565, 633, 1)]),
            ],
            l0: generate_l0_nonoverlapping_sublevels(vec![
                generate_table(877832, 1, 706, 718, 1),
                generate_table(877833, 1, 800, 2000, 1),
            ]),
            ..Default::default()
        };
        let in_progress = InProgressCompactionView::for_group(
            &[CompactTaskAssignment {
                compact_task: CompactTask {
                    task_id: 15040,
                    compaction_group_id: 9.into(),
                    target_level: 4,
                    input_ssts: vec![
                        InputLevel {
                            level_idx: 0,
                            level_type: LevelType::Nonoverlapping,
                            table_infos: vec![generate_table(881160, 1, 592, 722, 1)],
                        },
                        InputLevel {
                            level_idx: 4,
                            level_type: LevelType::Nonoverlapping,
                            table_infos: vec![generate_table(878784, 1, 565, 633, 1)],
                        },
                    ],
                    ..Default::default()
                },
                context_id: 1.into(),
            }],
            9.into(),
        );

        let mut selector = DynamicLevelSelector::default();
        let mut levels_handlers = (0..5).map(LevelHandler::new).collect_vec();
        let mut local_stats = LocalSelectorStatistic::default();
        let empty_in_progress = InProgressCompactionView::default();
        let compaction = pick_compaction_with_in_progress(
            &mut selector,
            1,
            &group_config,
            &levels,
            &mut levels_handlers,
            &mut local_stats,
            &empty_in_progress,
            None,
        )
        .unwrap();
        assert_eq!(compaction.input.target_level, 4);
        assert!(compaction.input.input_levels[1].table_infos.is_empty());
        assert!(
            compaction.input.input_levels[0]
                .table_infos
                .iter()
                .any(|sst| sst.sst_id.as_raw_id() == 877832)
        );

        let mut selector = DynamicLevelSelector::default();
        let mut levels_handlers = (0..5).map(LevelHandler::new).collect_vec();
        let mut local_stats = LocalSelectorStatistic::default();
        assert!(
            pick_compaction_with_in_progress(
                &mut selector,
                2,
                &group_config,
                &levels,
                &mut levels_handlers,
                &mut local_stats,
                &in_progress,
                None,
            )
            .is_none()
        );
        assert_eq!(local_stats.skip_picker.len(), 1);
        assert_eq!(local_stats.skip_picker[0].2.skip_by_overlapping, 1);
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

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
pub const SCORE_THRESHOLD: f64 = 1.0;

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
    pub score: f64,
    pub select_level: usize,
    pub target_level: usize,
    pub picker_type: PickerType,
    pub raw_score: f64,
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
    /// `calculate_level_base_size_v1` uses `RocksDB` v7.2.2 algorithm for level size calculation.
    /// This algorithm refers to the implementation in `https://github.com/facebook/rocksdb/blob/v7.2.2/db/version_set.cc#L3706`
    fn calculate_level_base_size_v1(&self, levels: &Levels) -> SelectContext {
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

    /// `calculate_level_base_size_v2` uses Pebble's algorithm for level size calculation.
    /// Key differences from RocksDB/v1:
    /// 1. Uses total DB size instead of max level size for more balanced distribution
    /// 2. Calculates smoothed multiplier to ensure geometric progression
    /// 3. No minimum constraint (`base_bytes_max`) to allow true geometric growth
    /// 4. Considers in-progress L0 compactions when determining firstNonEmptyLevel
    ///
    /// Reference: `https://github.com/cockroachdb/pebble/blob/master/compaction.go#L1825`
    fn calculate_level_base_size_v2(
        &self,
        levels: &Levels,
        handlers: &[LevelHandler],
    ) -> SelectContext {
        let mut first_non_empty_level = usize::MAX;
        let mut ctx = SelectContext::default();
        let mut db_size = 0u64;

        // Check L1+ and calculate total DB size (excluding L0 initially)
        for level in &levels.levels {
            if level.total_file_size > 0 {
                if first_non_empty_level == usize::MAX {
                    first_non_empty_level = level.level_idx as usize;
                }
                db_size += level.total_file_size;
            }
        }

        // Account for in-progress L0 compactions
        // If there are ongoing L0 compactions targeting a level below firstNonEmptyLevel,
        // we should consider that level as the first non-empty level
        if !handlers.is_empty() {
            for task in handlers[0].pending_tasks() {
                let target_level = task.target_level as usize;
                if target_level == 0 {
                    continue;
                }
                if first_non_empty_level == usize::MAX || target_level < first_non_empty_level {
                    first_non_empty_level = target_level;
                }
            }
        }

        ctx.level_max_bytes
            .resize(self.config.max_level as usize + 1, u64::MAX);

        let db_size_below_l0 = db_size;
        db_size += levels.l0.total_file_size;

        if db_size_below_l0 == 0 {
            // No levels for L1 and up contain any data. Target L0 compactions for the
            // last level or to the level to which there is an ongoing L0 compaction.
            ctx.base_level = self.config.max_level as usize;
            if first_non_empty_level != usize::MAX {
                ctx.base_level = first_non_empty_level;
            }
            return ctx;
        }

        let base_bytes_max = self.config.max_bytes_for_level_base;
        let multiplier = self.config.max_bytes_for_level_multiplier;

        // Calculate bottom level target size (e.g., 90% of total DB for multiplier=10)
        let bottom_level_size = db_size.saturating_sub(db_size / multiplier);

        // Determine base_level: find the highest level where base_bytes_max fits
        let mut cur_level_size = bottom_level_size;
        for _ in first_non_empty_level..self.config.max_level as usize {
            cur_level_size /= multiplier;
        }

        // Compute base level (where L0 data is compacted to).
        ctx.base_level = first_non_empty_level;
        while ctx.base_level > 1 && cur_level_size > base_bytes_max {
            ctx.base_level -= 1;
            cur_level_size /= multiplier;
        }

        let max_level = self.config.max_level as usize;
        let mut smoothed_level_multiplier = 1.0;
        if ctx.base_level < max_level {
            smoothed_level_multiplier = (bottom_level_size as f64 / base_bytes_max as f64)
                .powf(1.0 / (max_level - ctx.base_level) as f64);
        }

        let mut level_size = base_bytes_max as f64;
        for level in ctx.base_level..=max_level {
            if level > ctx.base_level && level_size > 0.0 {
                level_size *= smoothed_level_multiplier;
            }
            // Round the result since test cases use small target level sizes, which
            // can be impacted by floating-point imprecision + integer truncation.
            let rounded_level_size = level_size.round();
            if rounded_level_size > u64::MAX as f64 {
                ctx.level_max_bytes[level] = u64::MAX;
            } else {
                ctx.level_max_bytes[level] = rounded_level_size as u64;
            }
        }

        ctx
    }

    /// `calculate_level_base_size` calculate base level and the base size of LSM tree build for
    /// current dataset. In other words, `level_max_bytes` is our compaction goal which shall reach.
    pub fn calculate_level_base_size(
        &self,
        levels: &Levels,
        handlers: &[LevelHandler],
    ) -> SelectContext {
        // TODO: Add configuration flag to switch between algorithms
        let use_v2 = false;

        if use_v2 {
            self.calculate_level_base_size_v2(levels, handlers)
        } else {
            self.calculate_level_base_size_v1(levels)
        }
    }

    fn calculate_l0_overlap_score(&self, levels: &Levels, handlers: &[LevelHandler]) -> f64 {
        let idle_overlapping_file_count = levels
            .l0
            .sub_levels
            .iter()
            .filter(|level| level.level_type == LevelType::Overlapping)
            .flat_map(|level| level.table_infos.iter())
            .filter(|sst| !handlers[0].is_pending_compact(&sst.sst_id))
            .count();

        if idle_overlapping_file_count > 0 {
            idle_overlapping_file_count as f64 / self.config.level0_tier_compact_file_number as f64
        } else {
            0.0
        }
    }

    fn calculate_l0_non_overlap_score(
        &self,
        levels: &Levels,
        handlers: &[LevelHandler],
    ) -> (f64, f64) {
        let total_size = levels
            .l0
            .sub_levels
            .iter()
            .filter(|level| level.level_type == LevelType::Nonoverlapping)
            .filter(|level| !handlers[0].is_level_pending_compact(level))
            .map(|level| level.total_file_size)
            .sum::<u64>();

        let non_overlapping_size_score =
            total_size as f64 / self.config.max_bytes_for_level_base as f64;

        let non_overlapping_level_count = levels
            .l0
            .sub_levels
            .iter()
            .filter(|level| level.level_type == LevelType::Nonoverlapping)
            .filter(|level| !handlers[0].is_level_pending_compact(level))
            .count() as u64;

        let non_overlapping_level_score = non_overlapping_level_count as f64
            / self.config.level0_sub_level_compact_level_count as f64;

        (non_overlapping_size_score, non_overlapping_level_score)
    }

    fn calculate_level_score_v2(
        &self,
        level_idx: usize,
        levels: &Levels,
        handlers: &[LevelHandler],
        target_size: u64,
    ) -> f64 {
        let level = &levels.levels[level_idx - 1];
        if level.level_idx as usize != level_idx {
            return 0.0;
        }
        let output_file_size = handlers[level_idx].pending_output_file_size(level.level_idx + 1);
        let total_size = level.total_file_size.saturating_sub(output_file_size);
        if total_size == 0 {
            return 0.0;
        }
        total_size as f64 / target_size as f64
    }

    pub(crate) fn get_priority_levels_v2(
        &self,
        levels: &Levels,
        handlers: &[LevelHandler],
    ) -> SelectContext {
        let mut ctx = self.calculate_level_base_size_v2(levels, handlers);

        // Calculate scores for all levels first
        let mut level_scores = vec![0.0; self.config.max_level as usize + 1];
        for level_idx in ctx.base_level..=self.config.max_level as usize {
            level_scores[level_idx] = self.calculate_level_score_v2(
                level_idx,
                levels,
                handlers,
                ctx.level_max_bytes[level_idx],
            );
        }

        // L0 scores
        {
            let base_level_score = level_scores[ctx.base_level];
            let base_level_denominator = f64::max(0.01, base_level_score);

            let (mut non_overlapping_size_score, mut non_overlapping_level_score) =
                self.calculate_l0_non_overlap_score(levels, handlers);

            let non_overlapping_raw_score =
                f64::max(non_overlapping_size_score, non_overlapping_level_score);

            // Apply boost to non-overlapping scores as well
            non_overlapping_size_score /= base_level_denominator;
            non_overlapping_level_score /= base_level_denominator;

            let non_overlapping_score =
                f64::max(non_overlapping_size_score, non_overlapping_level_score);

            if non_overlapping_score > 1.0 {
                // Try ToBase first, if failed, degrade to Intra.
                ctx.score_levels.push(PickerInfo {
                    score: non_overlapping_score + 0.01,
                    select_level: 0,
                    target_level: ctx.base_level,
                    picker_type: PickerType::ToBase,
                    raw_score: non_overlapping_raw_score,
                });

                ctx.score_levels.push(PickerInfo {
                    score: non_overlapping_score,
                    select_level: 0,
                    target_level: 0,
                    picker_type: PickerType::Intra,
                    raw_score: non_overlapping_raw_score,
                });
            }

            // Tier compaction score is backpressured by non-overlapping score
            let mut l0_overlap_score = self.calculate_l0_overlap_score(levels, handlers);
            let l0_overlap_raw_score = l0_overlap_score;
            let l0_overlap_denominator = f64::max(0.01, non_overlapping_raw_score);
            l0_overlap_score /= l0_overlap_denominator;

            if l0_overlap_score > 1.0 {
                ctx.score_levels.push(PickerInfo {
                    score: l0_overlap_score,
                    select_level: 0,
                    target_level: 0,
                    picker_type: PickerType::Tier,
                    raw_score: l0_overlap_raw_score,
                });
            }
        }

        for level_idx in ctx.base_level..self.config.max_level as usize {
            let original_score = level_scores[level_idx];
            if original_score <= 1.0 {
                continue;
            }

            let next_level_score = level_scores[level_idx + 1];
            // Adjust each level's fill factor by the fill factor of the next level to get a score.
            // If the next level has a high fill factor, and is thus a priority for compaction,
            // this reduces the priority for compacting the current level.
            // If the next level has a low fill factor (i.e. it is below its target size),
            // this increases the priority for compacting the current level.
            let denominator = f64::max(0.01, next_level_score);
            let score = original_score / denominator;

            ctx.score_levels.push(PickerInfo {
                score,
                select_level: level_idx,
                target_level: level_idx + 1,
                picker_type: PickerType::BottomLevel,
                raw_score: original_score,
            });
        }

        // sort reverse to pick the largest one.
        ctx.score_levels.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.target_level.cmp(&b.target_level))
        });
        ctx
    }

    pub(crate) fn get_priority_levels(
        &self,
        levels: &Levels,
        handlers: &[LevelHandler],
    ) -> SelectContext {
        let mut ctx = self.calculate_level_base_size(levels, handlers);

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
                let l0_overlapping_score = self.calculate_l0_overlap_score(levels, handlers);
                // Reduce the level num of l0 overlapping sub_level
                ctx.score_levels.push(PickerInfo {
                    score: if l0_overlapping_score > 1.0 {
                        l0_overlapping_score
                    } else {
                        1.0 + 1.0 / SCORE_BASE as f64
                    },
                    select_level: 0,
                    target_level: 0,
                    picker_type: PickerType::Tier,
                    raw_score: l0_overlapping_score,
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
            let non_overlapping_size_score = (total_size * SCORE_BASE
                / std::cmp::max(self.config.max_bytes_for_level_base, base_level_size))
                as f64
                / SCORE_BASE as f64;
            // level count limit
            let non_overlapping_level_count = levels
                .l0
                .sub_levels
                .iter()
                .filter(|level| level.level_type == LevelType::Nonoverlapping)
                .count() as u64;
            let non_overlapping_level_score = (non_overlapping_level_count * SCORE_BASE
                / std::cmp::max(
                    base_level_sst_count / 16,
                    self.config.level0_sub_level_compact_level_count as u64,
                )) as f64
                / SCORE_BASE as f64;

            let non_overlapping_score =
                f64::max(non_overlapping_size_score, non_overlapping_level_score);

            // Reduce the level num of l0 non-overlapping sub_level
            if non_overlapping_size_score > 1.0 {
                ctx.score_levels.push(PickerInfo {
                    score: non_overlapping_score + 0.01,
                    select_level: 0,
                    target_level: ctx.base_level,
                    picker_type: PickerType::ToBase,
                    raw_score: non_overlapping_score,
                });
            }

            if non_overlapping_level_score > 1.0 {
                // FIXME: more accurate score calculation algorithm will be introduced (#11903)
                ctx.score_levels.push(PickerInfo {
                    score: non_overlapping_score,
                    select_level: 0,
                    target_level: 0,
                    picker_type: PickerType::Intra,
                    raw_score: non_overlapping_score,
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
                    score: (total_size * SCORE_BASE / ctx.level_max_bytes[level_idx]) as f64
                        / SCORE_BASE as f64,
                    select_level: level_idx,
                    target_level: level_idx + 1,
                    picker_type: PickerType::BottomLevel,
                    raw_score: (total_size * SCORE_BASE / ctx.level_max_bytes[level_idx]) as f64
                        / SCORE_BASE as f64,
                }
            });
        }

        // sort reverse to pick the largest one.
        ctx.score_levels.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.target_level.cmp(&b.target_level))
        });
        ctx
    }

    /// `compact_pending_bytes_needed` calculates the number of compact bytes needed to balance the
    /// LSM Tree from the current state of each level in the LSM Tree in combination with
    /// `compaction_config`
    /// This algorithm refers to the implementation in  `https://github.com/facebook/rocksdb/blob/main/db/version_set.cc#L3141`
    pub fn compact_pending_bytes_needed(&self, levels: &Levels, handlers: &[LevelHandler]) -> u64 {
        let ctx = self.calculate_level_base_size(levels, handlers);
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
            if picker_info.score <= SCORE_THRESHOLD {
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
        let handlers = (0..5).map(LevelHandler::new).collect::<Vec<_>>();
        let ctx = selector.calculate_level_base_size(&levels, &handlers);
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

        let ctx = selector.calculate_level_base_size(&levels, &handlers);
        // data size increase, so we need increase one level to place more data.
        assert_eq!(ctx.base_level, 1);
        assert_eq!(ctx.level_max_bytes[1], 100);
        assert_eq!(ctx.level_max_bytes[2], 120);
        assert_eq!(ctx.level_max_bytes[3], 600);
        assert_eq!(ctx.level_max_bytes[4], 3000);

        // append a large data to L0 but it does not change the base size of LSM tree.
        push_tables_level0_nonoverlapping(&mut levels, generate_tables(20..26, 0..1000, 1, 100));

        let ctx = selector.calculate_level_base_size(&levels, &handlers);
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

        let ctx = selector.calculate_level_base_size(&levels, &handlers);
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
        let handlers = (0..5).map(LevelHandler::new).collect::<Vec<_>>();
        let ctx = dynamic_level_core.calculate_level_base_size(&levels, &handlers);
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

        let compact_pending_bytes =
            dynamic_level_core.compact_pending_bytes_needed(&levels, &handlers);
        assert_eq!(24400 + 40110 + 47281, compact_pending_bytes);
    }

    #[test]
    fn test_calculate_level_base_size_v1_vs_v2() {
        // Test case: L6=600GB with multiplier=10
        // This simulates the scenario we discussed where v1 creates hotspots
        let config = CompactionConfigBuilder::new()
            .max_bytes_for_level_base(512 * 1024 * 1024) // 512MB
            .max_level(6)
            .max_bytes_for_level_multiplier(10)
            .compaction_mode(CompactionMode::Range as i32)
            .build();

        let selector = DynamicLevelSelectorCore::new(
            Arc::new(config),
            Arc::new(CompactionDeveloperConfig::default()),
        );

        // Create levels with L6=600GB, L5=60GB, L4=6GB, L3=600MB, L2=60MB
        // This simulates a realistic distribution after compaction
        let mb = 1024 * 1024u64;
        let levels = vec![
            generate_level(1, vec![]),
            generate_level(2, generate_tables(0..6, 0..1000000, 1, 10 * mb)), // ~60MB
            generate_level(3, generate_tables(6..66, 0..1000000, 1, 10 * mb)), // ~600MB
            generate_level(4, generate_tables(66..666, 0..1000000, 1, 10 * mb)), // ~6GB
            generate_level(5, generate_tables(666..6666, 0..1000000, 1, 10 * mb)), // ~60GB
            generate_level(6, generate_tables(6666..66666, 0..1000000, 1, 10 * mb)), // ~600GB
        ];
        let levels = Levels {
            levels,
            l0: generate_l0_nonoverlapping_sublevels(vec![]),
            ..Default::default()
        };

        let handlers = (0..7).map(LevelHandler::new).collect::<Vec<_>>();

        let ctx_v1 = selector.calculate_level_base_size_v1(&levels);
        println!("\n=== V1 Results ===");
        println!("base_level: {}", ctx_v1.base_level);
        for i in 1..=6 {
            println!(
                "L{}: target={:.2}GB, actual={:.2}GB",
                i,
                ctx_v1.level_max_bytes[i] as f64 / (1024.0 * 1024.0 * 1024.0),
                levels.levels[i - 1].total_file_size as f64 / (1024.0 * 1024.0 * 1024.0)
            );
        }

        let ctx_v2 = selector.calculate_level_base_size_v2(&levels, &handlers);
        println!("\n=== V2 Results ===");
        println!("base_level: {}", ctx_v2.base_level);
        for i in 1..=6 {
            println!(
                "L{}: target={:.2}GB, actual={:.2}GB",
                i,
                ctx_v2.level_max_bytes[i] as f64 / (1024.0 * 1024.0 * 1024.0),
                levels.levels[i - 1].total_file_size as f64 / (1024.0 * 1024.0 * 1024.0)
            );
        }

        // Assertions for v1 (expected to have bottleneck)
        assert_eq!(ctx_v1.base_level, 2);
        let base_mb = 512 * 1024 * 1024;
        // V1 enforces minimum base_bytes_max (512MB) on all levels
        assert_eq!(ctx_v1.level_max_bytes[2], base_mb);
        // L3 should be close to base_mb due to max() constraint - this is the bottleneck!
        assert!(ctx_v1.level_max_bytes[3] < base_mb * 2); // Bottleneck: only ~1.2x growth

        let v1_ratio_3_2 = ctx_v1.level_max_bytes[3] as f64 / ctx_v1.level_max_bytes[2] as f64;
        let v1_ratio_4_3 = ctx_v1.level_max_bytes[4] as f64 / ctx_v1.level_max_bytes[3] as f64;
        println!("\n=== V1 Growth Ratios (with bottleneck) ===");
        println!("V1 L3/L2: {:.2}x (bottleneck!)", v1_ratio_3_2);
        println!("V1 L4/L3: {:.2}x (jump!)", v1_ratio_4_3);

        // Assertions for v2 (expected to have smooth progression)
        assert_eq!(ctx_v2.base_level, 2);
        // V2 sets base level target to base_bytes_max
        assert_eq!(
            ctx_v2.level_max_bytes[2], base_mb,
            "V2 should set base level to base_bytes_max"
        );

        // V2 should have smooth geometric progression
        let ratio_3_2 = ctx_v2.level_max_bytes[3] as f64 / ctx_v2.level_max_bytes[2] as f64;
        let ratio_4_3 = ctx_v2.level_max_bytes[4] as f64 / ctx_v2.level_max_bytes[3] as f64;
        let ratio_5_4 = ctx_v2.level_max_bytes[5] as f64 / ctx_v2.level_max_bytes[4] as f64;

        println!("\n=== V2 Growth Ratios (smooth) ===");
        println!("V2 L3/L2: {:.2}x", ratio_3_2);
        println!("V2 L4/L3: {:.2}x", ratio_4_3);
        println!("V2 L5/L4: {:.2}x", ratio_5_4);

        // V2 should have consistent growth ratios
        // With 0.5GB start and ~600GB end over 4 intervals, ratio should be (1200)^0.25 ~= 5.88
        assert!(
            ratio_3_2 > 5.0 && ratio_3_2 < 7.0,
            "Expected smooth ratio ~5.88x"
        );
        assert!(
            ratio_4_3 > 5.0 && ratio_4_3 < 7.0,
            "Expected smooth ratio ~5.88x"
        );
        assert!(
            ratio_5_4 > 5.0 && ratio_5_4 < 7.0,
            "Expected smooth ratio ~5.88x"
        );

        // Verify v1 has bottleneck while v2 doesn't
        assert!(v1_ratio_3_2 < 2.0, "V1 should have bottleneck at L2/L3");
        assert!(v1_ratio_4_3 > 8.0, "V1 should have large jump at L3/L4");
        assert!(
            (ratio_3_2 - ratio_4_3).abs() < 2.0,
            "V2 ratios should be consistent"
        );
    }

    #[test]
    fn test_calculate_level_base_size_v2_smoothed_multiplier() {
        // Test that v2 correctly calculates smoothed multiplier
        let config = CompactionConfigBuilder::new()
            .max_bytes_for_level_base(512 * 1024 * 1024) // 512MB
            .max_level(6)
            .max_bytes_for_level_multiplier(10)
            .compaction_mode(CompactionMode::Range as i32)
            .build();

        let selector = DynamicLevelSelectorCore::new(
            Arc::new(config),
            Arc::new(CompactionDeveloperConfig::default()),
        );

        // Simulate total DB size = 666GB (L2..L6)
        let db_total = 666 * 1024 * 1024 * 1024u64;
        let levels = vec![
            generate_level(1, vec![]),
            generate_level(2, generate_tables(0..1, 0..1000000, 1, 10 * 1024 * 1024)),
            generate_level(3, generate_tables(1..2, 0..1000000, 1, 10 * 1024 * 1024)),
            generate_level(4, generate_tables(2..3, 0..1000000, 1, 10 * 1024 * 1024)),
            generate_level(5, generate_tables(3..4, 0..1000000, 1, 10 * 1024 * 1024)),
            generate_level(6, generate_tables(4..5, 0..1000000, 1, db_total)),
        ];
        let levels = Levels {
            levels,
            l0: generate_l0_nonoverlapping_sublevels(vec![]),
            ..Default::default()
        };

        let handlers = (0..7).map(LevelHandler::new).collect::<Vec<_>>();
        let ctx = selector.calculate_level_base_size_v2(&levels, &handlers);

        println!("\n=== Smoothed Multiplier Test ===");
        println!(
            "DB total: {:.2}GB",
            db_total as f64 / (1024.0 * 1024.0 * 1024.0)
        );
        println!("Base level: {}", ctx.base_level);

        for i in 2..=6 {
            let ratio = if i > 2 {
                ctx.level_max_bytes[i] as f64 / ctx.level_max_bytes[i - 1] as f64
            } else {
                0.0
            };
            println!(
                "L{}: {:.2}GB (ratio: {:.4})",
                i,
                ctx.level_max_bytes[i] as f64 / (1024.0 * 1024.0 * 1024.0),
                ratio
            );
        }

        // Verify the multiplier is approximately consistent
        assert_eq!(ctx.base_level, 2);
        let actual_ratios: Vec<f64> = (3..=6)
            .map(|i| ctx.level_max_bytes[i] as f64 / ctx.level_max_bytes[i - 1] as f64)
            .collect();

        println!("\n=== Consistency Check ===");
        println!(
            "All ratios: {:?}",
            actual_ratios
                .iter()
                .map(|r| format!("{:.2}x", r))
                .collect::<Vec<_>>()
        );

        // All ratios should be approximately equal (consistent geometric progression)
        let avg_ratio = actual_ratios.iter().sum::<f64>() / actual_ratios.len() as f64;
        for ratio in &actual_ratios {
            assert!(
                (ratio - avg_ratio).abs() < 1.0,
                "Ratio {:.4} should be consistent with average {:.4}",
                ratio,
                avg_ratio
            );
        }

        // The average ratio should be close to configured multiplier when DB is large
        // With 0.5GB start and ~600GB end over 4 intervals, ratio should be (1200)^0.25 ~= 5.88
        assert!(
            avg_ratio > 5.0 && avg_ratio < 7.0,
            "Average ratio should be close to 5.88x"
        );
    }

    #[test]
    fn test_get_priority_levels_v2_logic() {
        let config = CompactionConfigBuilder::new()
            .max_bytes_for_level_base(100)
            .max_level(4)
            .max_bytes_for_level_multiplier(5)
            .compaction_mode(CompactionMode::Range as i32)
            .build();
        let selector = DynamicLevelSelectorCore::new(
            Arc::new(config),
            Arc::new(CompactionDeveloperConfig::default()),
        );

        // Construct levels:
        // L2: 150 (Target 100) -> Over limit, needs compaction.
        // L3: 20 (Target ~500) -> Very empty.
        // L4: 1000 (Target ~1000) -> Full.
        let levels = vec![
            generate_level(1, vec![]),
            generate_level(2, generate_tables(0..15, 0..1000, 1, 10)), // 150
            generate_level(3, generate_tables(15..17, 0..1000, 1, 10)), // 20
            generate_level(4, generate_tables(17..117, 0..1000, 1, 10)), // 1000
        ];
        let levels = Levels {
            levels,
            l0: generate_l0_nonoverlapping_sublevels(vec![]),
            ..Default::default()
        };
        let handlers = (0..5).map(LevelHandler::new).collect::<Vec<_>>();

        // V1 Logic
        let ctx_v1 = selector.get_priority_levels(&levels, &handlers);
        // Find L2 score
        let l2_score_v1 = ctx_v1
            .score_levels
            .iter()
            .find(|p| p.select_level == 2)
            .map(|p| p.score)
            .unwrap_or(0.0);

        // V2 Logic
        let ctx_v2 = selector.get_priority_levels_v2(&levels, &handlers);
        let l2_score_v2 = ctx_v2
            .score_levels
            .iter()
            .find(|p| p.select_level == 2)
            .map(|p| p.score)
            .unwrap_or(0.0);

        // V1 Score: size(150) / target(100) * 100 = 150.
        // V2 Score:
        //   L2 Score Raw = 150 / 100 = 1.5.
        //   L3 Score Raw = 20 / 500 (approx) = 0.04.
        //   Boosted = 1.5 / 0.04 = 37.5.
        //   Final = 3750.
        // Note: V2 targets might differ slightly due to smoothed multiplier, but the boost effect should be massive.

        assert!(
            l2_score_v2 > l2_score_v1 * 2.0,
            "V2 score should be significantly boosted by empty next level"
        );
    }

    #[test]
    fn test_v2_scoring_scenarios() {
        use risingwave_hummock_sdk::level::Level;
        use risingwave_pb::hummock::LevelType;

        use crate::hummock::compaction::selector::level_selector::PickerType;

        let config = CompactionConfigBuilder::new()
            .max_bytes_for_level_base(100)
            .max_level(4)
            .max_bytes_for_level_multiplier(5)
            .level0_tier_compact_file_number(2)
            .level0_sub_level_compact_level_count(2)
            .compaction_mode(CompactionMode::Range as i32)
            .build();

        let selector = DynamicLevelSelectorCore::new(
            Arc::new(config),
            Arc::new(CompactionDeveloperConfig::default()),
        );
        let handlers = (0..5).map(LevelHandler::new).collect::<Vec<_>>();

        // Helper to check float equality
        let assert_score = |actual: f64, expected: f64, msg: &str| {
            assert!(
                (actual - expected).abs() < 0.001,
                "{}: got {}, expected {}",
                msg,
                actual,
                expected
            );
        };

        // Scenario 1: Balanced Tree
        // Base=L2 (100), L3 (500), L4 (2500)
        {
            println!("--- Scenario 1: Balanced ---");
            let levels = Levels {
                levels: vec![
                    generate_level(1, vec![]),
                    generate_level(2, generate_tables(0..1, 0..1000, 1, 100)), // 100/100 = 1.0
                    generate_level(3, generate_tables(1..2, 0..1000, 1, 500)), // 500/500 = 1.0
                    generate_level(4, generate_tables(2..3, 0..1000, 1, 2500)), // 2500/2500 = 1.0
                ],
                l0: generate_l0_nonoverlapping_sublevels(vec![]),
                ..Default::default()
            };

            let ctx = selector.get_priority_levels_v2(&levels, &handlers);
            assert_eq!(ctx.base_level, 2);

            // L2 Score
            // Note: L2 might not be selected if score <= 1.0, but let's check if it is selected or we can inspect internal state if we could.
            // But get_priority_levels_v2 returns ctx.score_levels.
            // If score <= 1.0, it might be skipped in the loop `if original_score <= 1.0 { continue; }`.
            // Wait, if score is exactly 1.0, it is skipped.
            // Let's make it slightly larger to ensure it's picked, or check that it is NOT picked.
            // If balanced, no compaction is needed.
            let l2 = ctx.score_levels.iter().find(|p| p.select_level == 2);
            assert!(
                l2.is_none(),
                "Balanced tree should not trigger compaction (Score <= 1.0)"
            );
        }

        // Scenario 2: Top Heavy (Boosting)
        // L2 (200, Target 100) -> Raw 2.0
        // L3 (50, Target ~500) -> Raw ~0.1
        {
            println!("--- Scenario 2: Top Heavy ---");
            let levels = Levels {
                levels: vec![
                    generate_level(1, vec![]),
                    generate_level(2, generate_tables(0..1, 0..1000, 1, 200)), // 200/100 = 2.0
                    generate_level(3, generate_tables(1..2, 0..1000, 1, 50)),  // 50/500 = 0.1
                    generate_level(4, generate_tables(2..3, 0..1000, 1, 2500)),
                ],
                l0: generate_l0_nonoverlapping_sublevels(vec![]),
                ..Default::default()
            };

            let ctx = selector.get_priority_levels_v2(&levels, &handlers);

            let l2 = ctx
                .score_levels
                .iter()
                .find(|p| p.select_level == 2)
                .expect("L2 should be selected");

            // Calculate expected dynamically
            let l2_target = ctx.level_max_bytes[2];
            let l3_target = ctx.level_max_bytes[3];
            let l2_size = levels.levels[1].total_file_size;
            let l3_size = levels.levels[2].total_file_size;

            let l2_raw = l2_size as f64 / l2_target as f64;
            let l3_score = l3_size as f64 / l3_target as f64;
            let expected_score = l2_raw / f64::max(0.01, l3_score);

            assert_score(l2.raw_score, l2_raw, "L2 Raw");
            assert_score(l2.score, expected_score, "L2 Score");
        }

        // Scenario 3: Bottom Heavy (Suppression)
        // L2 (200, Target 100) -> Raw 2.0
        // L3 (1000, Target ~500) -> Raw ~2.0
        {
            println!("--- Scenario 3: Bottom Heavy ---");
            let levels = Levels {
                levels: vec![
                    generate_level(1, vec![]),
                    generate_level(2, generate_tables(0..1, 0..1000, 1, 200)), // 200/100 = 2.0
                    generate_level(3, generate_tables(1..2, 0..1000, 1, 1000)), // 1000/500 = 2.0
                    generate_level(4, generate_tables(2..3, 0..1000, 1, 2500)),
                ],
                l0: generate_l0_nonoverlapping_sublevels(vec![]),
                ..Default::default()
            };

            let ctx = selector.get_priority_levels_v2(&levels, &handlers);

            // Calculate expected dynamically
            let l2_target = ctx.level_max_bytes[2];
            let l3_target = ctx.level_max_bytes[3];
            let l2_size = levels.levels[1].total_file_size;
            let l3_size = levels.levels[2].total_file_size;

            let l2_raw = l2_size as f64 / l2_target as f64;
            let l3_score = l3_size as f64 / l3_target as f64;
            let expected_score = l2_raw / f64::max(0.01, l3_score);

            if let Some(l2) = ctx.score_levels.iter().find(|p| p.select_level == 2) {
                assert_score(l2.raw_score, l2_raw, "L2 Raw");
                assert_score(l2.score, expected_score, "L2 Score");
                // Ensure it is suppressed (Score < Raw)
                assert!(l2.score < l2.raw_score, "L2 should be suppressed");
            } else {
                // If not selected, it means score <= 1.0.
                assert!(
                    expected_score <= 1.001,
                    "L2 not selected but expected score > 1.0"
                );
            }
        }

        // Scenario 4: L0 Backpressure
        // L0 Tier: 5 files (Threshold 2) -> Raw 2.5
        // L0 NonOverlap: 4 levels (Threshold 2) -> Raw 2.0
        // Base Level (L2): 50 (Target 100) -> Raw 0.5
        {
            println!("--- Scenario 4: L0 Backpressure ---");
            let mut l0 = generate_l0_nonoverlapping_sublevels(vec![]);
            for i in 0..4 {
                l0.sub_levels.push(Level {
                    level_idx: 0,
                    level_type: LevelType::Nonoverlapping,
                    total_file_size: 10,
                    sub_level_id: i as u64,
                    table_infos: vec![],
                    ..Default::default()
                });
            }
            l0.sub_levels.push(Level {
                level_idx: 0,
                level_type: LevelType::Overlapping,
                total_file_size: 100,
                sub_level_id: 100,
                table_infos: generate_tables(100..105, 0..1000, 1, 100), // 5 files
                ..Default::default()
            });

            let levels = Levels {
                levels: vec![
                    generate_level(1, vec![]),
                    generate_level(2, generate_tables(0..1, 0..1000, 1, 50)), // 50/100 = 0.5
                    generate_level(3, generate_tables(1..2, 0..1000, 1, 500)),
                    generate_level(4, generate_tables(2..3, 0..1000, 1, 2500)),
                ],
                l0,
                ..Default::default()
            };

            let ctx = selector.get_priority_levels_v2(&levels, &handlers);

            // NonOverlap
            let non_overlap = ctx
                .score_levels
                .iter()
                .find(|p| {
                    matches!(p.picker_type, PickerType::Intra)
                        || matches!(p.picker_type, PickerType::ToBase)
                })
                .expect("NonOverlap should be selected");
            assert_score(non_overlap.raw_score, 2.0, "NonOverlap Raw");

            // NonOverlap Score = Raw(2.0) / BaseLevelScore(0.5) = 4.0
            let expected_non_overlap_score = 2.0 / 0.5;
            if matches!(non_overlap.picker_type, PickerType::ToBase) {
                assert_score(
                    non_overlap.score,
                    expected_non_overlap_score + 0.01,
                    "NonOverlap Score (ToBase)",
                );
            } else {
                assert_score(
                    non_overlap.score,
                    expected_non_overlap_score,
                    "NonOverlap Score (Intra)",
                );
            }

            // Tier
            let tier = ctx
                .score_levels
                .iter()
                .find(|p| matches!(p.picker_type, PickerType::Tier))
                .expect("Tier should be selected");
            assert_score(tier.raw_score, 2.5, "Tier Raw");

            // Tier Score = Raw(2.5) / NonOverlapRaw(2.0) = 1.25
            // If it used Boosted NonOverlap (4.0), Score would be 0.625 and not selected.
            assert_score(tier.score, 1.25, "Tier Score");
        }

        // Scenario 5: Pending Compaction (Score Reduction)
        // L2 (400, Target ~300) -> Raw ~1.33
        // Handler has 200 bytes pending output to L3.
        // Effective Size = 400 - 200 = 200.
        // Effective Score = 200 / 300 = 0.66.
        {
            println!("--- Scenario 5: Pending Compaction ---");
            let levels = Levels {
                levels: vec![
                    generate_level(1, vec![]),
                    generate_level(2, generate_tables(0..4, 0..1000, 1, 100)), /* 4 files, 100 each = 400 total */
                    generate_level(3, generate_tables(4..5, 0..1000, 1, 500)),
                    generate_level(4, generate_tables(5..6, 0..1000, 1, 2500)),
                ],
                l0: generate_l0_nonoverlapping_sublevels(vec![]),
                ..Default::default()
            };

            // Case A: With Pending Compaction
            {
                let mut handlers_with_pending = (0..5).map(LevelHandler::new).collect::<Vec<_>>();
                // Add pending task for L2 -> L3
                // Pick 2 files from L2 (size 200)
                let pending_ssts = levels.levels[1]
                    .table_infos
                    .iter()
                    .take(2)
                    .collect::<Vec<_>>();
                handlers_with_pending[2].add_pending_task(1, 3, pending_ssts);

                let ctx = selector.get_priority_levels_v2(&levels, &handlers_with_pending);
                let l2 = ctx.score_levels.iter().find(|p| p.select_level == 2);
                assert!(
                    l2.is_none(),
                    "L2 should NOT be selected due to pending compaction reducing score < 1.0"
                );
            }

            // Case B: Without Pending Compaction
            {
                let handlers_no_pending = (0..5).map(LevelHandler::new).collect::<Vec<_>>();
                let ctx = selector.get_priority_levels_v2(&levels, &handlers_no_pending);
                let l2 = ctx.score_levels.iter().find(|p| p.select_level == 2);
                assert!(
                    l2.is_some(),
                    "L2 SHOULD be selected without pending compaction"
                );
                let l2 = l2.unwrap();
                assert!(l2.score > 1.0, "L2 score should be > 1.0");
            }
        }

        // Scenario 6: L0 Intra vs ToBase
        // L0 has 5 levels (Intra Threshold 2) -> Intra Raw 2.5
        // L0 size 500 (Base Target 100) -> ToBase Raw 5.0
        // Both should be evaluated.
        {
            println!("--- Scenario 6: L0 Intra vs ToBase ---");
            let l0 = generate_l0_nonoverlapping_sublevels(generate_tables(1..6, 0..500, 1, 100));
            // 5 levels. Intra score = 5 / 2 = 2.5.
            // Total size = 500. Base target = 100. ToBase score = 5.0.

            let levels = Levels {
                levels: vec![
                    generate_level(1, vec![]),
                    generate_level(2, generate_tables(0..1, 0..1000, 1, 100)),
                    generate_level(3, generate_tables(1..2, 0..1000, 1, 500)),
                    generate_level(4, generate_tables(2..3, 0..1000, 1, 2500)),
                ],
                l0,
                ..Default::default()
            };

            let ctx = selector.get_priority_levels_v2(&levels, &handlers);

            let intra = ctx
                .score_levels
                .iter()
                .find(|p| matches!(p.picker_type, PickerType::Intra));
            let to_base = ctx
                .score_levels
                .iter()
                .find(|p| matches!(p.picker_type, PickerType::ToBase));

            assert!(intra.is_some(), "Intra should be evaluated");
            assert!(to_base.is_some(), "ToBase should be evaluated");

            let intra = intra.unwrap();
            let to_base = to_base.unwrap();

            // Intra: Max(SizeScore 5.0, LevelScore 2.5) = 5.0
            assert_score(intra.raw_score, 5.0, "Intra Raw");

            // ToBase: Same raw score
            assert_score(to_base.raw_score, 5.0, "ToBase Raw");

            // Ensure ToBase is higher priority
            assert!(
                to_base.score > intra.score,
                "ToBase should have higher score"
            );
        }
    }
}

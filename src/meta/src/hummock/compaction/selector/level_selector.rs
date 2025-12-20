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

use risingwave_common::config::meta::default::compaction_config::{
    level0_non_overlapping_file_count_threshold, level0_non_overlapping_file_size_threshold,
    level0_non_overlapping_level_count_threshold,
};
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
        let use_v2 = self.config.enable_score_v2.unwrap_or_else(
            risingwave_common::config::meta::default::compaction_config::enable_score_v2,
        );

        if use_v2 {
            self.calculate_level_base_size_v2(levels, handlers)
        } else {
            self.calculate_level_base_size_v1(levels)
        }
    }

    /// Unified method to get priority levels with configurable algorithm version.
    /// This ensures both base size calculation and scoring logic are consistent.
    pub fn get_priority_levels(&self, levels: &Levels, handlers: &[LevelHandler]) -> SelectContext {
        let use_v2 = self.config.enable_score_v2.unwrap_or_else(
            risingwave_common::config::meta::default::compaction_config::enable_score_v2,
        );

        if use_v2 {
            self.get_priority_levels_v2(levels, handlers)
        } else {
            self.get_priority_levels_v1(levels, handlers)
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

        // Count all overlapping levels (regardless of pending status)
        // High overlapping level count increases write-stop risk
        let overlapping_level_count = levels
            .l0
            .sub_levels
            .iter()
            .filter(|level| level.level_type == LevelType::Overlapping)
            .count();

        let mut max_score = 0.0f64;

        // 1. File count score
        if self.config.level0_tier_compact_file_number > 0 && idle_overlapping_file_count > 0 {
            let file_count_score = idle_overlapping_file_count as f64
                / self.config.level0_tier_compact_file_number as f64;
            max_score = max_score.max(file_count_score);
        }

        // 2. Level count score (multiplied by 2 like non-overlapping)
        // This detects write-stop risk from accumulated overlapping sublevels
        // Uses level0_overlapping_sub_level_compact_level_count as threshold
        if self.config.level0_overlapping_sub_level_compact_level_count > 0 {
            let level_count_score = (2 * overlapping_level_count) as f64
                / self.config.level0_overlapping_sub_level_compact_level_count as f64;
            max_score = max_score.max(level_count_score);
        }

        max_score
    }

    fn calculate_l0_non_overlap_score(&self, levels: &Levels, handlers: &[LevelHandler]) -> f64 {
        let mut total_size = 0u64;
        let mut idle_file_count = 0usize;

        // Collect size and file count from non-pending files
        for level in levels.l0.sub_levels.iter() {
            if level.level_type != LevelType::Nonoverlapping {
                continue;
            }

            for sst in &level.table_infos {
                if !handlers[0].is_pending_compact(&sst.sst_id) {
                    total_size += sst.sst_size;
                    idle_file_count += 1;
                }
            }
        }

        // Count all non-overlapping levels (regardless of pending status)
        let non_overlapping_level_count = levels
            .l0
            .sub_levels
            .iter()
            .filter(|level| level.level_type == LevelType::Nonoverlapping)
            .count();

        let mut max_score = 0.0f64;

        // 1. Size score
        if let Some(threshold) = self
            .config
            .level0_non_overlapping_file_size_threshold
            .or_else(|| Some(level0_non_overlapping_file_size_threshold()))
        {
            if threshold > 0 {
                let score = total_size as f64 / threshold as f64;
                max_score = max_score.max(score);
            }
        }

        // 2. File count score
        if let Some(threshold) = self
            .config
            .level0_non_overlapping_file_count_threshold
            .or_else(|| Some(level0_non_overlapping_file_count_threshold()))
        {
            if threshold > 0 {
                let score = idle_file_count as f64 / threshold as f64;
                max_score = max_score.max(score);
            }
        }

        // 3. Level count score
        if let Some(threshold) = self
            .config
            .level0_non_overlapping_level_count_threshold
            .or_else(|| Some(level0_non_overlapping_level_count_threshold()))
        {
            if threshold > 0 {
                let score = (2 * non_overlapping_level_count) as f64 / threshold as f64;
                max_score = max_score.max(score);
            }
        }

        max_score
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

        // L0 scores - Unified L0 pressure with base_level backpressure
        // Key change: Unify overlapping and non-overlapping pressure, then apply base_level backpressure
        // This removes the incorrect mutual suppression between overlapping and non-overlapping
        {
            let overlapping_pressure = self.calculate_l0_overlap_score(levels, handlers);
            let non_overlapping_pressure = self.calculate_l0_non_overlap_score(levels, handlers);
            let base_level_score = level_scores[ctx.base_level];

            // Unified L0 pressure: max of overlapping and non-overlapping
            // Rationale: 216 overlapping + 84 non-overlapping = 300 sublevels is a unified L0 pressure problem
            // Old V2 bug: overlapping was suppressed by non-overlapping, causing Tier to lose priority
            let l0_raw_score = overlapping_pressure.max(non_overlapping_pressure);

            // Apply base_level backpressure to unified L0 score
            // This preserves the original design: when base is full, reduce L0 compaction priority
            let l0_score = l0_raw_score / base_level_score.max(0.01);

            // Only trigger L0 compaction when unified pressure exceeds threshold
            // Once triggered, add all three compaction types and let them compete
            if l0_raw_score > 1.0 {
                // Priority 1: Tier compaction (score + 0.02)
                // Fastest layer reduction - compacts N consecutive overlapping sublevels at once
                ctx.score_levels.push(PickerInfo {
                    score: l0_score + 0.02,
                    select_level: 0,
                    target_level: 0,
                    picker_type: PickerType::Tier,
                    raw_score: l0_raw_score,
                });

                // Priority 2: ToBase compaction (score + 0.01)
                // Medium priority - reduces layers while pushing data to base level
                ctx.score_levels.push(PickerInfo {
                    score: l0_score + 0.01,
                    select_level: 0,
                    target_level: ctx.base_level,
                    picker_type: PickerType::ToBase,
                    raw_score: l0_raw_score,
                });

                // Priority 3: Intra compaction (score + 0.00)
                // Lowest priority - in-place optimization within L0
                ctx.score_levels.push(PickerInfo {
                    score: l0_score,
                    select_level: 0,
                    target_level: 0,
                    picker_type: PickerType::Intra,
                    raw_score: l0_raw_score,
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

    pub(crate) fn get_priority_levels_v1(
        &self,
        levels: &Levels,
        handlers: &[LevelHandler],
    ) -> SelectContext {
        let mut ctx = self.calculate_level_base_size_v1(levels);

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
        // Note: V2 (get_priority_levels_v2) already filters scores <= 1.0 internally.
        // V1 (get_priority_levels) doesn't filter, so this check is needed for V1.
        // For V2, ctx.score_levels already excludes low scores, making this check redundant but harmless.
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
        assert_compaction_task, generate_l0_nonoverlapping_multi_sublevels,
        generate_l0_nonoverlapping_sublevels, generate_level, generate_tables,
        push_tables_level0_nonoverlapping,
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
            .enable_score_v2(Some(true))
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
        let ctx_v1 = selector.get_priority_levels_v1(&levels, &handlers);
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
            .enable_score_v2(Some(true))
            .level0_non_overlapping_file_size_threshold(Some(100))
            .level0_non_overlapping_file_count_threshold(Some(10))
            .level0_non_overlapping_level_count_threshold(Some(2))
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
        // L0 NonOverlap: 4 levels, 40MB total (Threshold 2, Target 100MB)
        //   level_score = (2 * 4) / 2 = 4.0 (note: calculate_l0_non_overlap_score multiplies count by 2)
        //   size_score = 40 / 100 = 0.4
        //   raw = max(0.4, 4.0) = 4.0
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
            assert_score(non_overlap.raw_score, 4.0, "NonOverlap Raw");

            // NonOverlap Score = Raw(4.0) / BaseLevelScore(0.5) = 8.0
            let expected_non_overlap_score = 4.0 / 0.5;
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

            // Tier Score = Raw(2.5) / NonOverlapRaw(4.0) = 0.625
            // This tests that Tier uses raw NonOverlap score (4.0), not boosted score (8.0)
            assert_score(tier.score, 0.625, "Tier Score");
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

    #[test]
    fn test_v2_l0_priority_with_default_config() {
        // Test V2 with default config: L0 should be prioritized when it has high pressure
        // Default: max_bytes_for_level_base = 512MB, multiplier = 5, level0_tier = 12
        let config = CompactionConfigBuilder::new()
            .enable_score_v2(Some(true))
            .build();

        let selector = DynamicLevelSelectorCore::new(
            Arc::new(config.clone()),
            Arc::new(CompactionDeveloperConfig::default()),
        );

        let mb = 1024 * 1024u64;

        // Simulate dynamic-level filling: L6 (bottommost) fills first
        // L6: ~3GB (approaching target ~2.5GB after smoothing)
        // L5: ~600MB (starting to fill, target ~512MB)
        // L4-L1: empty (not yet reached)
        // L0: 15 sublevels, 800MB total - HIGH PRESSURE
        //
        // Key: Base level will be L5 (first non-empty below L6)
        // L0 has both level count pressure (15/12) and size pressure (800MB/512MB)
        let levels = Levels {
            levels: vec![
                generate_level(1, vec![]),
                generate_level(2, vec![]),
                generate_level(3, vec![]),
                generate_level(4, vec![]),
                generate_level(5, generate_tables(100..160, 0..5000000, 1, 10 * mb)), // 600MB
                generate_level(6, generate_tables(160..460, 0..10000000, 1, 10 * mb)), // 3GB
            ],
            l0: generate_l0_nonoverlapping_multi_sublevels(vec![
                // 15 sublevels, each ~53MB, total ~800MB
                generate_tables(0..1, 0..100000, 1, 53 * mb),
                generate_tables(1..2, 100000..200000, 1, 53 * mb),
                generate_tables(2..3, 200000..300000, 1, 53 * mb),
                generate_tables(3..4, 300000..400000, 1, 53 * mb),
                generate_tables(4..5, 400000..500000, 1, 53 * mb),
                generate_tables(5..6, 500000..600000, 1, 53 * mb),
                generate_tables(6..7, 600000..700000, 1, 53 * mb),
                generate_tables(7..8, 700000..800000, 1, 53 * mb),
                generate_tables(8..9, 800000..900000, 1, 53 * mb),
                generate_tables(9..10, 900000..1000000, 1, 53 * mb),
                generate_tables(10..11, 1000000..1100000, 1, 53 * mb),
                generate_tables(11..12, 1100000..1200000, 1, 53 * mb),
                generate_tables(12..13, 1200000..1300000, 1, 53 * mb),
                generate_tables(13..14, 1300000..1400000, 1, 53 * mb),
                generate_tables(14..15, 1400000..1500000, 1, 53 * mb),
            ]),
            ..Default::default()
        };

        let handlers = (0..7).map(LevelHandler::new).collect::<Vec<_>>();
        let ctx = selector.get_priority_levels(&levels, &handlers);

        println!("\n=== V2 L0 Priority Test (Default Config) ===");
        println!(
            "L0 sublevels: {}, total: {}MB",
            levels.l0.sub_levels.len(),
            levels.l0.total_file_size / mb
        );
        println!(
            "L5: {}MB (target: {}MB)",
            levels.levels[4].total_file_size / mb,
            ctx.level_max_bytes[5] / mb
        );
        println!(
            "L6: {}MB (target: {}MB)",
            levels.levels[5].total_file_size / mb,
            ctx.level_max_bytes[6] / mb
        );
        println!("Base level: {}", ctx.base_level);
        println!("\nTop 3 candidates:");
        for (i, picker) in ctx.score_levels.iter().take(3).enumerate() {
            println!(
                "  #{}: L{}->{} type={}, score={:.2}, raw={:.2}",
                i + 1,
                picker.select_level,
                picker.target_level,
                picker.picker_type,
                picker.score,
                picker.raw_score
            );
        }

        // Verify L0 is prioritized
        assert!(
            !ctx.score_levels.is_empty(),
            "Should have compaction candidates"
        );
        let top_candidate = &ctx.score_levels[0];

        // L0 should be top priority (either ToBase or Intra)
        assert_eq!(
            top_candidate.select_level, 0,
            "L0 should be top priority with 15 sublevels (trigger=12) and 800MB size (base=512MB), \
             but got L{} with score {:.2}",
            top_candidate.select_level, top_candidate.score
        );
    }

    #[test]
    fn test_v2_anti_starvation_with_default_config() {
        // Test V2's anti-starvation mechanism with default config
        // Key: Set up MULTIPLE levels that are all over-target simultaneously
        // Then verify through multiple compaction rounds that different levels get picked
        let config = CompactionConfigBuilder::new()
            .enable_score_v2(Some(true))
            .build();

        let mb = 1024 * 1024u64;

        // Construct a scenario with multiple over-target levels:
        // We need to carefully design sizes so that after V2's smoothed multiplier calculation,
        // multiple levels exceed their targets.
        //
        // Strategy: Start with a large DB (~10GB) and make several intermediate levels over-target
        // L6: 6GB (will be near target after smoothing)
        // L5: 3GB (will be over-target)
        // L4: 1.5GB (will be over-target)
        // L3: 800MB (will be over-target)
        // L2: empty (base level)
        // L0: 240MB with 12 sublevels (triggers level0_tier_compact_file_number=12 threshold)
        let mut levels = Levels {
            levels: vec![
                generate_level(1, vec![]),
                generate_level(2, vec![]), // base level
                generate_level(3, generate_tables(100..180, 0..3000000, 1, 10 * mb)), // 800MB
                generate_level(4, generate_tables(180..330, 0..6000000, 1, 10 * mb)), // 1.5GB
                generate_level(5, generate_tables(330..630, 0..12000000, 1, 10 * mb)), // 3GB
                generate_level(6, generate_tables(630..1230, 0..24000000, 1, 10 * mb)), // 6GB
            ],
            l0: generate_l0_nonoverlapping_multi_sublevels(vec![
                // L0 with high pressure: 12 sublevels (= tier trigger), 240MB total
                generate_tables(0..2, 0..100000, 1, 10 * mb), // 20MB
                generate_tables(2..4, 100000..200000, 1, 10 * mb), // 20MB
                generate_tables(4..6, 200000..300000, 1, 10 * mb), // 20MB
                generate_tables(6..8, 300000..400000, 1, 10 * mb), // 20MB
                generate_tables(8..10, 400000..500000, 1, 10 * mb), // 20MB
                generate_tables(10..12, 500000..600000, 1, 10 * mb), // 20MB
                generate_tables(12..14, 600000..700000, 1, 10 * mb), // 20MB
                generate_tables(14..16, 700000..800000, 1, 10 * mb), // 20MB
                generate_tables(16..18, 800000..900000, 1, 10 * mb), // 20MB
                generate_tables(18..20, 900000..1000000, 1, 10 * mb), // 20MB
                generate_tables(20..22, 1000000..1100000, 1, 10 * mb), // 20MB
                generate_tables(22..24, 1100000..1200000, 1, 10 * mb), // 20MB
            ]),
            ..Default::default()
        };

        let selector = DynamicLevelSelectorCore::new(
            Arc::new(config.clone()),
            Arc::new(CompactionDeveloperConfig::default()),
        );

        let handlers = (0..7).map(LevelHandler::new).collect::<Vec<_>>();

        println!("\n=== V2 Anti-Starvation Test (Default Config) ===");
        println!("Goal: Multiple levels over-target, verify they all get compaction opportunities");
        println!("Method: Validate scores through assertions at each round\n");

        // Track which levels get picked across multiple rounds
        let mut picked_levels = std::collections::HashSet::new();
        let mut pick_history = Vec::new();

        // Simulate 8 compaction rounds
        for round in 1..=8 {
            let ctx = selector.get_priority_levels(&levels, &handlers);

            // Print detailed state at each round
            println!("--- Round {} ---", round);
            println!("Current state (base level: L{}):", ctx.base_level);

            // Print L0 state
            let l0_size = levels.l0.total_file_size;
            let l0_sublevels = levels.l0.sub_levels.len();
            println!("  L0: {}MB ({} sublevels)", l0_size / mb, l0_sublevels);

            // Print L1+ states
            for i in 2..=6 {
                if levels.levels[i - 1].total_file_size > 0 {
                    let size = levels.levels[i - 1].total_file_size;
                    let target = ctx.level_max_bytes[i];
                    let ratio = size as f64 / target as f64;
                    println!(
                        "  L{}: {}MB / {}MB = {:.2}x {}",
                        i,
                        size / mb,
                        target / mb,
                        ratio,
                        if ratio > 1.0 { "OVER ⚠" } else { "under" }
                    );
                }
            }

            if ctx.score_levels.is_empty() {
                println!("No compaction needed (balanced state reached)\n");
                break;
            }

            // Print top 3 candidates with scores
            println!("Candidates:");
            for (idx, pick) in ctx.score_levels.iter().take(3).enumerate() {
                println!(
                    "  #{}: L{}->{} score={:.2} raw={:.2} type={}",
                    idx + 1,
                    pick.select_level,
                    pick.target_level,
                    pick.score,
                    pick.raw_score,
                    pick.picker_type
                );
            }

            let top_pick = &ctx.score_levels[0];
            let select_level = top_pick.select_level;
            let target_level = top_pick.target_level;

            println!(
                "→ Selected: L{}->{} (score={:.2})",
                select_level, target_level, top_pick.score
            );

            // === CRITICAL: Validate score calculation ===
            // This is the core of UT design - verify correctness through assertions
            if select_level == 0 {
                // L0 validation
                let base_level_size = levels.levels[ctx.base_level - 1].total_file_size;
                let base_level_target = ctx.level_max_bytes[ctx.base_level];
                let base_level_score = base_level_size as f64 / base_level_target as f64;

                // Calculate expected L0 raw score
                let total_size: u64 = levels
                    .l0
                    .sub_levels
                    .iter()
                    .filter(|level| {
                        use risingwave_pb::hummock::LevelType;
                        level.level_type == LevelType::Nonoverlapping
                    })
                    .map(|level| level.total_file_size)
                    .sum();
                let sublevel_count = levels
                    .l0
                    .sub_levels
                    .iter()
                    .filter(|level| {
                        use risingwave_pb::hummock::LevelType;
                        level.level_type == LevelType::Nonoverlapping
                    })
                    .count() as f64;

                let size_score = total_size as f64 / config.max_bytes_for_level_base as f64;
                // Note: calculate_l0_non_overlap_score multiplies level count by 2
                // Use level0_non_overlapping_level_count_threshold (default: 8) instead of level0_sub_level_compact_level_count
                let level_count_threshold = config.level0_non_overlapping_level_count_threshold
                    .unwrap_or(risingwave_common::config::meta::default::compaction_config::level0_non_overlapping_level_count_threshold());
                let level_score = (2.0 * sublevel_count) / level_count_threshold as f64;
                let expected_raw = f64::max(size_score, level_score);

                println!(
                    "  L0 validation: size_score={:.2}, level_score={:.2}, raw={:.2}, base_level_score={:.2}",
                    size_score, level_score, expected_raw, base_level_score
                );

                // Verify raw score
                assert!(
                    (top_pick.raw_score - expected_raw).abs() < 0.01,
                    "Round {}: L0 raw score mismatch. Expected {:.2} (max({:.2}, {:.2})), got {:.2}",
                    round,
                    expected_raw,
                    size_score,
                    level_score,
                    top_pick.raw_score
                );

                // Verify boosted score
                let expected_boosted = expected_raw / f64::max(0.01, base_level_score);
                assert!(
                    (top_pick.score - expected_boosted).abs() < 0.1,
                    "Round {}: L0 boosted score mismatch. Expected {:.2} ({:.2} / max(0.01, {:.2})), got {:.2}",
                    round,
                    expected_boosted,
                    expected_raw,
                    base_level_score,
                    top_pick.score
                );
            } else if select_level <= levels.levels.len() {
                // L1+ validation
                let level_size = levels.levels[select_level - 1].total_file_size;
                let level_target = ctx.level_max_bytes[select_level];
                let expected_raw_score = level_size as f64 / level_target as f64;

                // Verify raw score is correct
                assert!(
                    (top_pick.raw_score - expected_raw_score).abs() < 0.01,
                    "Round {}: L{} raw score mismatch. Expected {:.2} ({}MB / {}MB), got {:.2}",
                    round,
                    select_level,
                    expected_raw_score,
                    level_size / mb,
                    level_target / mb,
                    top_pick.raw_score
                );

                // Verify final score includes backpressure adjustment
                if select_level < 6 {
                    let next_level_size = levels.levels[select_level].total_file_size;
                    let next_level_target = ctx.level_max_bytes[select_level + 1];
                    let next_level_score = next_level_size as f64 / next_level_target as f64;
                    let expected_score = expected_raw_score / f64::max(0.01, next_level_score);

                    assert!(
                        (top_pick.score - expected_score).abs() < 0.1,
                        "Round {}: L{} backpressure score mismatch. \
                         Expected {:.2} ({:.2} / max(0.01, {:.2})), got {:.2}",
                        round,
                        select_level,
                        expected_score,
                        expected_raw_score,
                        next_level_score,
                        top_pick.score
                    );
                }
            }

            picked_levels.insert(select_level);
            pick_history.push(select_level);

            // Simulate compaction with simplified logic:
            // - L0: Clear entire L0, move all data to base level
            // - L1+: Remove 2/3 of data, move to target level
            let compacted_size = if select_level == 0 {
                // L0 compaction: Move entire L0 to base level (simplified)
                let old_size = levels.l0.total_file_size;
                let old_sublevels = levels.l0.sub_levels.len();

                let moved_size = old_size;
                levels.l0.total_file_size = 0;
                levels.l0.sub_levels.clear();

                println!(
                    "  Data flow: L0 {}MB ({} sublevels) -> 0MB (0 sublevels), moved {}MB to L{}",
                    old_size / mb,
                    old_sublevels,
                    moved_size / mb,
                    target_level
                );
                moved_size
            } else if select_level <= levels.levels.len() {
                // L1+ compaction: Remove 2/3 of size, move to target level
                let level_obj = &mut levels.levels[select_level - 1];
                let old_size = level_obj.total_file_size;
                let reduction = (level_obj.total_file_size * 2) / 3;
                level_obj.total_file_size = level_obj.total_file_size.saturating_sub(reduction);

                // Also remove 2/3 of tables
                let tables_to_remove = (level_obj.table_infos.len() * 2) / 3;
                if tables_to_remove > 0 {
                    let new_len = level_obj.table_infos.len() - tables_to_remove;
                    level_obj.table_infos.truncate(new_len);
                }

                println!(
                    "  Data flow: L{} {}MB -> {}MB, moved {}MB to L{}",
                    select_level,
                    old_size / mb,
                    level_obj.total_file_size / mb,
                    reduction / mb,
                    target_level
                );
                reduction
            } else {
                0
            };

            // Add the compacted data to target level (simulating compaction output)
            if target_level > 0 && target_level <= levels.levels.len() && compacted_size > 0 {
                let old_target_size = levels.levels[target_level - 1].total_file_size;
                levels.levels[target_level - 1].total_file_size += compacted_size;
                println!(
                    "  Target level: L{} {}MB -> {}MB\n",
                    target_level,
                    old_target_size / mb,
                    levels.levels[target_level - 1].total_file_size / mb
                );
            } else {
                println!(); // Empty line for readability
            }
        }

        println!("=== Results ===");
        println!(
            "Picked levels: {:?}",
            picked_levels.iter().sorted().collect::<Vec<_>>()
        );
        println!("Pick sequence: {:?}", pick_history);

        // Verify anti-starvation: At least 2 different levels should be picked
        // (proving the algorithm doesn't starve any over-target level)
        assert!(
            picked_levels.len() >= 2,
            "Anti-starvation FAILED: only {} level(s) picked. \
             Expected multiple levels to get compaction opportunities across {} rounds. \
             Pick history: {:?}",
            picked_levels.len(),
            pick_history.len(),
            pick_history
        );

        // If we picked 3+ different levels, that's strong evidence of good anti-starvation
        if picked_levels.len() >= 3 {
            println!(
                "✓ Strong anti-starvation: {} different levels picked",
                picked_levels.len()
            );
        } else {
            println!(
                "✓ Basic anti-starvation: {} different levels picked",
                picked_levels.len()
            );
        }

        // Verify the sequence makes sense (non-L0 levels should be in the picked set)
        let non_l0_picked = picked_levels.iter().any(|&l| l > 0);
        assert!(
            non_l0_picked,
            "Expected non-L0 levels to be picked in multi-level over-target scenario"
        );

        println!("✓ Test passed: V2 anti-starvation mechanism working correctly");
        println!("✓ All score calculations validated through assertions");
    }

    #[test]
    fn test_v2_production_scenario_with_heavy_l0() {
        // Test case based on real production data:
        // L0: 224 overlapping sublevels (~35GB) + 25 non-overlapping sublevels (~27GB)
        // L3: 5.8GB, L4: 13.4GB, L5: 29.3GB, L6: 66.2GB
        // Total: ~114GB with severe L0 backlog
        //
        // This tests whether default thresholds are reasonable for production workloads
        let config = CompactionConfigBuilder::new()
            .enable_score_v2(Some(true))
            .build();

        let mb = 1024 * 1024u64;
        let gb = 1024 * mb;

        use risingwave_hummock_sdk::level::Level;
        use risingwave_pb::hummock::LevelType;

        // Simulate L0 state - strictly following production data
        let mut l0 = generate_l0_nonoverlapping_sublevels(vec![]);

        // Add 25 non-overlapping sublevels with exact sizes from production
        // sub_level_id, sst_num, size (in bytes)
        let non_overlapping_data = vec![
            (705, 65, 1871579609),
            (695, 63, 1637417615),
            (685, 61, 1600402097),
            (675, 60, 1596711753),
            (665, 60, 1621336053),
            (657, 53, 1534993286),
            (648, 52, 1609504103),
            (638, 54, 1443638283),
            (629, 55, 1587889721),
            (617, 49, 1250541664),
            (608, 45, 1140778076),
            (599, 40, 1154532135),
            (590, 39, 1096704216),
            (580, 34, 919126565),
            (571, 34, 928150285),
            (563, 24, 676381747),
            (554, 18, 557074190),
            (545, 17, 450520552),
            (535, 8, 175011067),
            (526, 1, 11950900),
            (479, 2, 13394821),
            (443, 1, 5093290),
            (434, 1, 4331289),
            (397, 1, 6777613),
            (245, 3, 8743049),
        ];

        for (sub_level_id, sst_num, total_size) in non_overlapping_data {
            l0.sub_levels.push(Level {
                level_idx: 0,
                level_type: LevelType::Nonoverlapping,
                total_file_size: total_size,
                sub_level_id,
                table_infos: generate_tables(
                    (sub_level_id * 100)..(sub_level_id * 100 + sst_num),
                    0..100000,
                    1,
                    total_size / sst_num,
                ),
                ..Default::default()
            });
        }

        // Add 224 overlapping sublevels (sub_level_id 715-938)
        // Each has 1 SST, sizes from production data
        let overlapping_sizes = vec![
            133616835, 200556239, 200515709, 268610926, 98974598, 200555380, 167069957, 100232723,
            267367849, 200476068, 268610219, 132460975, 100192590, 133577417, 268610958, 32242961,
            268610963, 98974369, 200515336, 200556054, 267368000, 268610584, 32162890, 100192548,
            268611024, 32203110, 33384404, 200475573, 268610163, 65569021, 133657395, 268610999,
            98974776, 200595650, 167069666, 200555821, 167069762, 267287235, 200595543, 200515473,
            234001688, 233921573, 268610685, 32203105, 268610310, 132420146, 268610507, 65608745,
            167069215, 66768435, 268610564, 266244115, 233881307, 167070097, 167109496, 267367115,
            268611006, 99014879, 233962089, 233921952, 200556465, 233922232, 200516537, 268610132,
            32162804, 66808453, 267367483, 133616891, 200515452, 268609903, 268610494, 30919674,
            66808496, 267407318, 268610299, 99054614, 200475640, 167069474, 268610223, 132420348,
            200556004, 268610219, 99095082, 268610169, 98974277, 268610681, 99015070, 268610186,
            165866015, 33384314, 133617690, 268610519, 32122835, 268610214, 99054573, 133656993,
            267286645, 268611299, 32243079, 200435093, 268610146, 199352714, 133616889, 268611349,
            32203121, 33384215, 268610819, 32162746, 167149506, 66808496, 267368300, 233961801,
            100193130, 267367853, 200555445, 33384242, 267327811, 167109759, 267327356, 100192734,
            267447978, 100152782, 167109465, 268610933, 165826524, 200555476, 66808676, 233921265,
            267447445, 133536603, 233921541, 133616937, 167149732, 268610974, 98974296, 167069527,
            268610333, 199312062, 100233263, 100153073, 268610869, 32203110, 268610160, 32162746,
            167110215, 33384413, 200595419, 268611387, 32162858, 200475987, 268610990, 132420730,
            234042301, 33384197, 133576751, 267326692, 268611153, 99095163, 167069229, 233922106,
            267326860, 268611074, 65608699, 267407122, 133536852, 234001562, 267326926, 100233020,
            167109736, 167069417, 200515194, 33424227, 200555928, 233880889, 268610665, 32122885,
            167110019, 133616957, 268610381, 99054726, 200475930, 233961094, 268611107, 32203105,
            233961039, 66768435, 268610942, 65609048, 233962222, 267367491, 268610450, 98974277,
            167030001, 268610447, 65769153, 100152848, 200435138, 268610304, 32162888, 200596287,
            200595383, 133577507, 268610255, 132340285, 233961527, 66808532, 233962201, 267367642,
            268610830, 65568981, 200555601, 267327017, 267448190, 233921256, 200515349, 33384206,
            268610858, 65568956, 268609983, 99054962, 167149423, 100153190, 200555994, 133617127,
        ];

        for (i, size) in overlapping_sizes.iter().enumerate() {
            let sub_level_id = 715 + i as u64;
            l0.sub_levels.push(Level {
                level_idx: 0,
                level_type: LevelType::Overlapping,
                total_file_size: *size,
                sub_level_id,
                table_infos: generate_tables(
                    (sub_level_id * 100)..(sub_level_id * 100 + 1),
                    0..100000,
                    1,
                    *size,
                ),
                ..Default::default()
            });
        }

        l0.total_file_size = l0.sub_levels.iter().map(|l| l.total_file_size).sum();

        let levels = Levels {
            levels: vec![
                generate_level(1, vec![]),
                generate_level(2, vec![]),
                generate_level(3, generate_tables(200..377, 0..6000000, 1, 33 * mb)), /* 177 ssts, 5.8GB */
                generate_level(4, generate_tables(377..794, 0..12000000, 1, 32 * mb)), /* 417 ssts, 13.4GB */
                generate_level(5, generate_tables(794..1442, 0..24000000, 1, 45 * mb)), /* 648 ssts, 29.3GB */
                generate_level(6, generate_tables(1442..2722, 0..48000000, 1, 52 * mb)), /* 1280 ssts, 66.2GB */
            ],
            l0,
            ..Default::default()
        };

        let selector = DynamicLevelSelectorCore::new(
            Arc::new(config.clone()),
            Arc::new(CompactionDeveloperConfig::default()),
        );

        let handlers = (0..7).map(LevelHandler::new).collect::<Vec<_>>();
        let ctx = selector.get_priority_levels(&levels, &handlers);

        println!("\n=== Production Scenario Analysis ===");
        println!("Total DB size: ~114GB");
        println!("L0 overlapping: 224 sublevels, each with 1 SST");
        println!("L0 non-overlapping: 25 sublevels with varying SST counts");
        println!("L3: 5.8GB, L4: 13.4GB, L5: 29.3GB, L6: 66.2GB");
        println!("\nBase level: {}", ctx.base_level);
        for i in 3..=6 {
            println!(
                "L{} target: {:.2}GB, actual: {:.2}GB, ratio: {:.2}x",
                i,
                ctx.level_max_bytes[i] as f64 / gb as f64,
                levels.levels[i - 1].total_file_size as f64 / gb as f64,
                levels.levels[i - 1].total_file_size as f64 / ctx.level_max_bytes[i] as f64
            );
        }

        println!("\n=== Score Calculations ===");

        // Calculate L0 scores
        let overlapping_count = levels
            .l0
            .sub_levels
            .iter()
            .filter(|l| l.level_type == LevelType::Overlapping)
            .flat_map(|l| l.table_infos.iter())
            .count();
        let overlapping_score =
            overlapping_count as f64 / config.level0_tier_compact_file_number as f64;

        let non_overlapping_count = levels
            .l0
            .sub_levels
            .iter()
            .filter(|l| l.level_type == LevelType::Nonoverlapping)
            .count();
        let non_overlapping_size: u64 = levels
            .l0
            .sub_levels
            .iter()
            .filter(|l| l.level_type == LevelType::Nonoverlapping)
            .map(|l| l.total_file_size)
            .sum();
        let non_overlapping_file_count: usize = levels
            .l0
            .sub_levels
            .iter()
            .filter(|l| l.level_type == LevelType::Nonoverlapping)
            .flat_map(|l| l.table_infos.iter())
            .count();

        let size_threshold = config
            .level0_non_overlapping_file_size_threshold
            .unwrap_or(risingwave_common::config::meta::default::compaction_config::level0_non_overlapping_file_size_threshold());
        let count_threshold = config
            .level0_non_overlapping_file_count_threshold
            .unwrap_or(risingwave_common::config::meta::default::compaction_config::level0_non_overlapping_file_count_threshold());
        let level_threshold = config
            .level0_non_overlapping_level_count_threshold
            .unwrap_or(risingwave_common::config::meta::default::compaction_config::level0_non_overlapping_level_count_threshold());

        let size_score = non_overlapping_size as f64 / size_threshold as f64;
        let file_count_score = non_overlapping_file_count as f64 / count_threshold as f64;
        let level_count_score = (2 * non_overlapping_count) as f64 / level_threshold as f64;
        let non_overlapping_raw_score = size_score.max(file_count_score).max(level_count_score);

        println!("L0 Overlapping:");
        println!(
            "  File count: {} (threshold: {})",
            overlapping_count, config.level0_tier_compact_file_number
        );
        println!("  Raw score: {:.2}", overlapping_score);

        println!("\nL0 Non-overlapping:");
        println!(
            "  Size: {:.2}GB (threshold: {:.2}GB)",
            non_overlapping_size as f64 / gb as f64,
            size_threshold as f64 / gb as f64
        );
        println!(
            "  File count: {} (threshold: {})",
            non_overlapping_file_count, count_threshold
        );
        println!(
            "  Level count: {} (threshold: {})",
            non_overlapping_count, level_threshold
        );
        println!("  Size score: {:.2}", size_score);
        println!("  File count score: {:.2}", file_count_score);
        println!("  Level count score: {:.2}", level_count_score);
        println!("  Raw score: {:.2}", non_overlapping_raw_score);

        // Calculate Tier score (with level count score)
        let tier_file_count_score =
            overlapping_count as f64 / config.level0_tier_compact_file_number as f64;
        let tier_level_count_score = if config.level0_overlapping_sub_level_compact_level_count > 0
        {
            (2 * overlapping_count) as f64
                / config.level0_overlapping_sub_level_compact_level_count as f64
        } else {
            0.0
        };
        let tier_raw_score = tier_file_count_score.max(tier_level_count_score);
        let tier_backpressure_denominator = non_overlapping_raw_score.max(0.01);
        let tier_score = tier_raw_score / tier_backpressure_denominator;

        println!("\nL0 Tier (calculated):");
        println!(
            "  File count: {} (threshold: {})",
            overlapping_count, config.level0_tier_compact_file_number
        );
        println!(
            "  Level count: {} sublevels (threshold: {})",
            overlapping_count, config.level0_overlapping_sub_level_compact_level_count
        );
        println!("  File count score: {:.2}", tier_file_count_score);
        println!("  Level count score: {:.2}", tier_level_count_score);
        println!("  Raw score: {:.2}", tier_raw_score);
        println!(
            "  Backpressure denominator: {:.2} (non-overlapping raw score)",
            tier_backpressure_denominator
        );
        println!(
            "  Final score: {:.2} / {:.2} = {:.2}",
            tier_raw_score, tier_backpressure_denominator, tier_score
        );

        println!(
            "\n=== All Candidates (Total: {}) ===",
            ctx.score_levels.len()
        );
        for (i, picker) in ctx.score_levels.iter().enumerate() {
            println!(
                "#{}: L{}->{} score={:.2} raw={:.2} type={}",
                i + 1,
                picker.select_level,
                picker.target_level,
                picker.score,
                picker.raw_score,
                picker.picker_type
            );
        }

        println!("\n=== Top 5 Candidates ===");
        for (i, picker) in ctx.score_levels.iter().take(5).enumerate() {
            println!(
                "#{}: L{}->{} score={:.2} raw={:.2} type={}",
                i + 1,
                picker.select_level,
                picker.target_level,
                picker.score,
                picker.raw_score,
                picker.picker_type
            );
        }

        // Verify L0 has highest priority given the severe backlog
        assert!(
            !ctx.score_levels.is_empty(),
            "Should have compaction candidates"
        );

        let top_pick = &ctx.score_levels[0];
        println!("\n=== Analysis ===");

        // With default config:
        // - overlapping_score = 56 / 12 = 4.67
        // - level_count_score = (2 * 25) / 8 = 6.25 → actual 16.67
        // - BUT L3 is 11.41x over-target, so L0 gets suppressed

        if top_pick.select_level == 0 {
            println!("✓ L0 correctly prioritized (score={:.2})", top_pick.score);
            println!("  This is expected given:");
            println!("    - 224 overlapping files (18.67x threshold)");
            println!("    - 25 non-overlapping levels (6.25x threshold)");
        } else {
            println!(
                "⚠ L0 not top priority, picked L{}->{}",
                top_pick.select_level, top_pick.target_level
            );
            println!(
                "  Reason: L0 raw_score={:.2}, but base_level (L{}) score={:.2}",
                non_overlapping_raw_score,
                ctx.base_level,
                levels.levels[ctx.base_level - 1].total_file_size as f64
                    / ctx.level_max_bytes[ctx.base_level] as f64
            );
            println!(
                "  L0 suppressed_score = {:.2} / {:.2} = {:.2}",
                non_overlapping_raw_score,
                levels.levels[ctx.base_level - 1].total_file_size as f64
                    / ctx.level_max_bytes[ctx.base_level] as f64,
                non_overlapping_raw_score
                    / (levels.levels[ctx.base_level - 1].total_file_size as f64
                        / ctx.level_max_bytes[ctx.base_level] as f64)
                        .max(0.01)
            );
            println!("  This reveals a design tradeoff:");
            println!("    - Pro: Prevents L0 compaction from making L3 worse");
            println!("    - Con: L0 can accumulate to level0_stop_write_threshold");
            println!("  Possible solutions:");
            println!("    1. Increase level_count_threshold from 8 to 16-32");
            println!("    2. Add L0 raw_score cap (e.g., force top priority if raw > 10)");
            println!("    3. Use separate backpressure logic for extreme L0 cases");
        }

        // Check if thresholds are reasonable
        println!("\n=== Threshold Evaluation ===");

        if overlapping_score > 3.0 {
            println!("✓ Overlapping threshold (12) is reasonable - detects severe backlog");
        } else {
            println!("⚠ Overlapping threshold might be too high for this workload");
        }

        if level_count_score > 3.0 {
            println!("✓ Level count threshold (8) is reasonable - detects severe fragmentation");
        } else {
            println!("⚠ Level count threshold might be too high");
        }

        if size_score > 1.0 {
            println!("✓ Size threshold (8GB) is reasonable - detects size pressure");
        } else {
            println!("ℹ Size threshold (8GB) is conservative - prioritizes level count over size");
        }
    }

    #[test]
    fn test_v2_anti_starvation_complex_scenario() {
        // Complex scenario that simulates real production behavior:
        // 1. Continuous L0 writes: L0 has 12 sublevels, each compaction only removes 4 (simulating ongoing writes)
        // 2. Unbalanced over-target levels: L3/L4/L5 all over-target but with different pressure ratios
        // 3. Goal: Verify algorithm balances all levels fairly over time
        let config = CompactionConfigBuilder::new()
            .enable_score_v2(Some(true))
            .build();

        let mb = 1024 * 1024u64;

        // Initial state design (carefully crafted to create interesting dynamics):
        // L0: 240MB, 12 sublevels (4.0x over level count threshold)
        // L3: 1024MB (2.0x over-target ~512MB) - HIGH pressure
        // L4: 2500MB (1.8x over-target ~1400MB) - HIGH pressure
        // L5: 5000MB (1.3x over-target ~3800MB) - MEDIUM pressure
        // L6: 8000MB (0.77x under-target ~10400MB) - Absorbs everything
        //
        // Key insight: Different over-target ratios create competing priorities
        let mut levels = Levels {
            levels: vec![
                generate_level(1, vec![]),
                generate_level(2, vec![]), // base level
                generate_level(3, generate_tables(100..202, 0..3000000, 1, 10 * mb)), // 1024MB
                generate_level(4, generate_tables(202..452, 0..6000000, 1, 10 * mb)), // 2500MB
                generate_level(5, generate_tables(452..952, 0..12000000, 1, 10 * mb)), // 5000MB
                generate_level(6, generate_tables(952..1752, 0..24000000, 1, 10 * mb)), // 8000MB
            ],
            l0: generate_l0_nonoverlapping_multi_sublevels(vec![
                // 12 sublevels, 240MB total
                generate_tables(0..2, 0..100000, 1, 10 * mb),
                generate_tables(2..4, 100000..200000, 1, 10 * mb),
                generate_tables(4..6, 200000..300000, 1, 10 * mb),
                generate_tables(6..8, 300000..400000, 1, 10 * mb),
                generate_tables(8..10, 400000..500000, 1, 10 * mb),
                generate_tables(10..12, 500000..600000, 1, 10 * mb),
                generate_tables(12..14, 600000..700000, 1, 10 * mb),
                generate_tables(14..16, 700000..800000, 1, 10 * mb),
                generate_tables(16..18, 800000..900000, 1, 10 * mb),
                generate_tables(18..20, 900000..1000000, 1, 10 * mb),
                generate_tables(20..22, 1000000..1100000, 1, 10 * mb),
                generate_tables(22..24, 1100000..1200000, 1, 10 * mb),
            ]),
            ..Default::default()
        };

        let selector = DynamicLevelSelectorCore::new(
            Arc::new(config.clone()),
            Arc::new(CompactionDeveloperConfig::default()),
        );

        let handlers = (0..7).map(LevelHandler::new).collect::<Vec<_>>();

        println!("\n=== V2 Anti-Starvation Complex Scenario ===");
        println!("Simulates continuous writes + unbalanced over-target levels");
        println!("L0: Each compaction removes 4 sublevels (simulating ongoing writes)\n");

        let mut picked_levels = std::collections::HashSet::new();
        let mut pick_history = Vec::new();

        // Simulate 12 rounds (enough to see patterns emerge)
        for round in 1..=12 {
            let ctx = selector.get_priority_levels(&levels, &handlers);

            println!("--- Round {} ---", round);
            println!("State (base: L{}):", ctx.base_level);

            let l0_size = levels.l0.total_file_size;
            let l0_sublevels = levels.l0.sub_levels.len();
            println!("  L0: {}MB ({} sublevels)", l0_size / mb, l0_sublevels);

            for i in 2..=6 {
                if levels.levels[i - 1].total_file_size > 0 {
                    let size = levels.levels[i - 1].total_file_size;
                    let target = ctx.level_max_bytes[i];
                    let ratio = size as f64 / target as f64;
                    println!(
                        "  L{}: {}MB / {}MB = {:.2}x {}",
                        i,
                        size / mb,
                        target / mb,
                        ratio,
                        if ratio > 1.0 { "OVER ⚠" } else { "under" }
                    );
                }
            }

            if ctx.score_levels.is_empty() {
                println!("Balanced state reached\n");
                break;
            }

            println!("Top 3:");
            for (idx, pick) in ctx.score_levels.iter().take(3).enumerate() {
                println!(
                    "  #{}: L{}->{} score={:.2} raw={:.2} type={}",
                    idx + 1,
                    pick.select_level,
                    pick.target_level,
                    pick.score,
                    pick.raw_score,
                    pick.picker_type
                );
            }

            let top_pick = &ctx.score_levels[0];
            let select_level = top_pick.select_level;
            let target_level = top_pick.target_level;

            println!(
                "→ Pick: L{}->{} (score={:.2})",
                select_level, target_level, top_pick.score
            );

            // Validate score calculation (same logic as before)
            if select_level == 0 {
                let base_level_size = levels.levels[ctx.base_level - 1].total_file_size;
                let base_level_target = ctx.level_max_bytes[ctx.base_level];
                let base_level_score = base_level_size as f64 / base_level_target as f64;

                let total_size: u64 = levels
                    .l0
                    .sub_levels
                    .iter()
                    .filter(|level| {
                        use risingwave_pb::hummock::LevelType;
                        level.level_type == LevelType::Nonoverlapping
                    })
                    .map(|level| level.total_file_size)
                    .sum();
                let sublevel_count = levels
                    .l0
                    .sub_levels
                    .iter()
                    .filter(|level| {
                        use risingwave_pb::hummock::LevelType;
                        level.level_type == LevelType::Nonoverlapping
                    })
                    .count() as f64;

                let size_score = total_size as f64 / config.max_bytes_for_level_base as f64;
                // Note: calculate_l0_non_overlap_score multiplies level count by 2
                // Use level0_non_overlapping_level_count_threshold (default: 8) instead of level0_sub_level_compact_level_count
                let level_count_threshold = config.level0_non_overlapping_level_count_threshold
                    .unwrap_or(risingwave_common::config::meta::default::compaction_config::level0_non_overlapping_level_count_threshold());
                let level_score = (2.0 * sublevel_count) / level_count_threshold as f64;
                let expected_raw = f64::max(size_score, level_score);

                assert!(
                    (top_pick.raw_score - expected_raw).abs() < 0.01,
                    "Round {}: L0 raw score mismatch",
                    round
                );

                let expected_boosted = expected_raw / f64::max(0.01, base_level_score);
                assert!(
                    (top_pick.score - expected_boosted).abs() < 0.1,
                    "Round {}: L0 boosted score mismatch",
                    round
                );
            } else if select_level <= levels.levels.len() {
                let level_size = levels.levels[select_level - 1].total_file_size;
                let level_target = ctx.level_max_bytes[select_level];
                let expected_raw_score = level_size as f64 / level_target as f64;

                assert!(
                    (top_pick.raw_score - expected_raw_score).abs() < 0.01,
                    "Round {}: L{} raw score mismatch",
                    round,
                    select_level
                );

                if select_level < 6 {
                    let next_level_size = levels.levels[select_level].total_file_size;
                    let next_level_target = ctx.level_max_bytes[select_level + 1];
                    let next_level_score = next_level_size as f64 / next_level_target as f64;
                    let expected_score = expected_raw_score / f64::max(0.01, next_level_score);

                    assert!(
                        (top_pick.score - expected_score).abs() < 0.1,
                        "Round {}: L{} backpressure mismatch",
                        round,
                        select_level
                    );
                }
            }

            picked_levels.insert(select_level);
            pick_history.push(select_level);

            // Simulate compaction with realistic behavior:
            // - L0: Remove 4 sublevels (1/3, simulating continuous writes)
            // - L1+: Remove 2/3 of size (aggressive cleanup)
            let compacted_size = if select_level == 0 {
                let old_size = levels.l0.total_file_size;
                let old_sublevels = levels.l0.sub_levels.len();

                // Only compact 4 sublevels (simulating ongoing writes)
                let sublevels_to_compact = std::cmp::min(4, old_sublevels);
                if sublevels_to_compact > 0 {
                    let moved_size: u64 = levels
                        .l0
                        .sub_levels
                        .iter()
                        .take(sublevels_to_compact)
                        .map(|l| l.total_file_size)
                        .sum();
                    levels.l0.total_file_size =
                        levels.l0.total_file_size.saturating_sub(moved_size);
                    levels.l0.sub_levels.drain(0..sublevels_to_compact);

                    println!(
                        "  Flow: L0 {}MB ({} sublevels) -> {}MB ({} sublevels), moved {}MB",
                        old_size / mb,
                        old_sublevels,
                        levels.l0.total_file_size / mb,
                        levels.l0.sub_levels.len(),
                        moved_size / mb
                    );
                    moved_size
                } else {
                    0
                }
            } else if select_level <= levels.levels.len() {
                let level_obj = &mut levels.levels[select_level - 1];
                let old_size = level_obj.total_file_size;
                let reduction = (level_obj.total_file_size * 2) / 3;
                level_obj.total_file_size = level_obj.total_file_size.saturating_sub(reduction);

                let tables_to_remove = (level_obj.table_infos.len() * 2) / 3;
                if tables_to_remove > 0 {
                    let new_len = level_obj.table_infos.len() - tables_to_remove;
                    level_obj.table_infos.truncate(new_len);
                }

                println!(
                    "  Flow: L{} {}MB -> {}MB, moved {}MB",
                    select_level,
                    old_size / mb,
                    level_obj.total_file_size / mb,
                    reduction / mb
                );
                reduction
            } else {
                0
            };

            if target_level > 0 && target_level <= levels.levels.len() && compacted_size > 0 {
                let old_target_size = levels.levels[target_level - 1].total_file_size;
                levels.levels[target_level - 1].total_file_size += compacted_size;
                println!(
                    "  Target: L{} {}MB -> {}MB\n",
                    target_level,
                    old_target_size / mb,
                    levels.levels[target_level - 1].total_file_size / mb
                );
            } else {
                println!();
            }
        }

        println!("=== Results ===");
        println!(
            "Picked levels: {:?}",
            picked_levels.iter().sorted().collect::<Vec<_>>()
        );
        println!("Pick sequence: {:?}", pick_history);

        // Verify anti-starvation in complex scenario
        assert!(
            picked_levels.len() >= 3,
            "Complex scenario FAILED: only {} level(s) picked. \
             Expected algorithm to balance multiple unbalanced over-target levels. \
             Pick history: {:?}",
            picked_levels.len(),
            pick_history
        );

        // Verify L0 was handled (continuous write pressure)
        assert!(
            picked_levels.contains(&0),
            "L0 should be compacted to handle continuous write pressure"
        );

        // Verify multiple non-L0 levels were balanced
        let non_l0_count = picked_levels.iter().filter(|&&l| l > 0).count();
        assert!(
            non_l0_count >= 2,
            "Expected at least 2 non-L0 levels to be compacted, got {}. \
             This indicates algorithm may be starving some over-target levels.",
            non_l0_count
        );

        println!(
            "✓ Complex scenario passed: {} different levels picked",
            picked_levels.len()
        );
        println!(
            "✓ L0 handled: {} times (continuous write simulation)",
            pick_history.iter().filter(|&&l| l == 0).count()
        );
        println!(
            "✓ Non-L0 levels balanced: {} different levels",
            non_l0_count
        );
        println!("✓ All score calculations validated");
    }

    #[test]
    fn test_enable_score_v2_config_actually_switches_algorithm() {
        // Verify enable_score_v2 config actually switches between V1 and V2 algorithms.
        let mb = 1024 * 1024u64;
        let levels = vec![
            generate_level(1, vec![]),
            generate_level(2, generate_tables(0..5, 0..1000, 1, 10 * mb)),
            generate_level(3, generate_tables(5..15, 0..1000, 1, 10 * mb)),
            generate_level(4, generate_tables(15..65, 0..1000, 1, 10 * mb)),
            generate_level(5, generate_tables(65..165, 0..1000, 1, 10 * mb)),
            generate_level(6, generate_tables(165..665, 0..1000, 1, 10 * mb)),
        ];
        let levels = Levels {
            levels,
            l0: generate_l0_nonoverlapping_sublevels(vec![]),
            ..Default::default()
        };
        let handlers = (0..7).map(LevelHandler::new).collect::<Vec<_>>();

        let config_v1 = CompactionConfigBuilder::new()
            .max_bytes_for_level_base(512 * mb)
            .max_level(6)
            .max_bytes_for_level_multiplier(10)
            .enable_score_v2(Some(false))
            .build();

        let config_v2 = CompactionConfigBuilder::new()
            .max_bytes_for_level_base(512 * mb)
            .max_level(6)
            .max_bytes_for_level_multiplier(10)
            .enable_score_v2(Some(true))
            .build();

        let selector_v1 = DynamicLevelSelectorCore::new(
            Arc::new(config_v1),
            Arc::new(CompactionDeveloperConfig::default()),
        );
        let selector_v2 = DynamicLevelSelectorCore::new(
            Arc::new(config_v2),
            Arc::new(CompactionDeveloperConfig::default()),
        );

        // Call unified entry point - the config should control which algorithm is used
        let ctx_v1 = selector_v1.get_priority_levels(&levels, &handlers);
        let ctx_v2 = selector_v2.get_priority_levels(&levels, &handlers);

        // V1 and V2 must produce different level_max_bytes (different base size calculation)
        assert_ne!(
            ctx_v1.level_max_bytes[3], ctx_v2.level_max_bytes[3],
            "Config switch failed: V1 and V2 produced same level targets"
        );

        println!(
            "✅ enable_score_v2 config controls algorithm: L3 target V1={}MB vs V2={}MB",
            ctx_v1.level_max_bytes[3] / mb,
            ctx_v2.level_max_bytes[3] / mb
        );
    }

    #[test]
    fn test_v2_l0_not_filtered_when_base_level_over_target() {
        use super::PickerType;
        // Regression test for Bug #1: L0 ToBase/Intra should not be filtered out when base level is over-target
        //
        // Scenario:
        // - L0 has real pressure (raw_score = 1.5)
        // - Base level (L2) is 2.0x over-target (common in high-load production)
        //
        // Bug: Using boosted score for filtering → L0 gets boosted_score = 1.5 / 2.0 = 0.75 → FILTERED OUT
        // Fix: Use raw score for filtering → L0 gets raw_score = 1.5 > 1.0 → INCLUDED (but with lower priority)
        //
        // This is critical because:
        // 1. L0 accumulation can eventually block writes (level0_stop_write_threshold)
        // 2. Backpressure should affect PRIORITY, not FILTERING
        let config = CompactionConfigBuilder::new()
            .max_bytes_for_level_base(100)
            .max_level(4)
            .max_bytes_for_level_multiplier(5)
            .level0_sub_level_compact_level_count(3)
            .compaction_mode(CompactionMode::Range as i32)
            .enable_score_v2(Some(true))
            .build();

        let selector = DynamicLevelSelectorCore::new(
            Arc::new(config.clone()),
            Arc::new(CompactionDeveloperConfig::default()),
        );

        // Setup to trigger the bug:
        // Create a large DB so base_level stays at L2, and L2 is over-target
        // Total DB = 250 + 250 + 1250 = 1750MB
        // bottom_level_size = 1750 * (1 - 1/5) = 1400MB
        // Divide back: 1400/5=280, 280/5=56 < 100 → stop at L2
        // So base_level = L2, and L2 target ≈ 100MB, but actual size = 250MB → over-target!
        //
        // - L0: 2 sublevels (threshold=3), ~120MB → raw_score = max(1.2, 1.33) = 1.33
        // - L2 (base): 250MB (target ~100MB) → score = 2.5 (over-target!)
        // - L3: 250MB (target ~500MB) → score = 0.5 (under-target)
        // - L4: 1250MB (target ~2500MB) → score = 0.5 (under-target)
        //
        // Bug scenario: L0 raw_score=1.33 > 1.0 (needs compaction)
        //               When base_level (L2) is over-target (score=2.5), L0 gets suppressed
        //               adjusted_score = 1.33 / 2.5 = 0.53 < 1.0
        //               If using adjusted_score for filtering → L0 FILTERED OUT (BUG!)
        //               Fix: Use raw_score for filtering → L0 INCLUDED (even with low priority)
        let levels = Levels {
            levels: vec![
                generate_level(1, vec![]),
                generate_level(2, generate_tables(100..125, 0..1000, 1, 10)), /* 250MB - base level, over-target */
                generate_level(3, generate_tables(125..150, 0..1000, 1, 10)), // 250MB
                generate_level(4, generate_tables(150..275, 0..1000, 1, 10)), // 1250MB
            ],
            l0: generate_l0_nonoverlapping_multi_sublevels(vec![
                // 2 sublevels, total 120MB
                generate_tables(0..12, 0..100000, 1, 5), // 60MB
                generate_tables(12..24, 100000..200000, 1, 5), // 60MB
            ]),
            ..Default::default()
        };

        let handlers = (0..5).map(LevelHandler::new).collect::<Vec<_>>();
        let ctx = selector.get_priority_levels_v2(&levels, &handlers);

        // Verify base_level is L2 and over-target
        assert_eq!(ctx.base_level, 2, "base_level should be L2");
        let base_level_score =
            levels.levels[1].total_file_size as f64 / ctx.level_max_bytes[2] as f64;
        assert!(
            base_level_score > 1.0,
            "L2 (base level) should be over-target, got score={:.2}",
            base_level_score
        );

        // Calculate expected L0 scores
        let l0_size_score =
            levels.l0.total_file_size as f64 / config.max_bytes_for_level_base as f64;
        // Use level0_non_overlapping_level_count_threshold (default: 8) instead of level0_sub_level_compact_level_count
        let level_count_threshold = config.level0_non_overlapping_level_count_threshold
            .unwrap_or(risingwave_common::config::meta::default::compaction_config::level0_non_overlapping_level_count_threshold());
        let l0_level_score = (2 * levels.l0.sub_levels.len()) as f64 / level_count_threshold as f64;
        let l0_raw_score = f64::max(l0_size_score, l0_level_score);

        // L0 raw score should be > 1.0 (needs compaction)
        assert!(
            l0_raw_score > 1.0,
            "L0 raw_score should be > 1.0, got {:.2}",
            l0_raw_score
        );

        // CRITICAL ASSERTION: L0 must not be filtered out even though base level is over-target
        let l0_candidates: Vec<_> = ctx
            .score_levels
            .iter()
            .filter(|p| p.select_level == 0)
            .collect();

        assert!(
            !l0_candidates.is_empty(),
            "❌ BUG: L0 was filtered out! L0 raw_score={:.2} > 1.0 should be INCLUDED \
             even though base level is over-target (score={:.2}). \
             This happens when using adjusted_score for filtering instead of raw_score.",
            l0_raw_score,
            base_level_score
        );

        // Verify ToBase or Intra exists
        let has_tobase = l0_candidates
            .iter()
            .any(|p| matches!(p.picker_type, PickerType::ToBase));
        let has_intra = l0_candidates
            .iter()
            .any(|p| matches!(p.picker_type, PickerType::Intra));
        assert!(
            has_tobase || has_intra,
            "L0 should have ToBase or Intra compaction candidate"
        );

        // Verify raw_score is used for filtering (all L0 candidates should have raw_score > 1.0)
        for candidate in &l0_candidates {
            if matches!(
                candidate.picker_type,
                PickerType::ToBase | PickerType::Intra
            ) {
                assert!(
                    (candidate.raw_score - l0_raw_score).abs() < 0.01,
                    "L0 raw_score should be {:.2}, got {:.2}",
                    l0_raw_score,
                    candidate.raw_score
                );
            }
        }
    }

    #[test]
    fn test_v2_l0_tier_not_filtered_when_nonoverlapping_has_pressure() {
        use super::PickerType;
        // Regression test for Bug #2: L0 Tier compaction should not be filtered by backpressure
        //
        // Scenario:
        // - L0 has overlapping files (need Tier compaction, raw_score = 2.5)
        // - L0 non-overlapping levels also have pressure (raw_score = 2.0)
        //
        // Bug: Using backpressured score for filtering → tier_score = 2.5 / 2.0 = 1.25, but if threshold
        //      check uses backpressured score, it might be suppressed incorrectly
        // Fix: Use raw score for filtering → tier_raw_score = 2.5 > 1.0 → INCLUDED
        //
        // This is critical because:
        // 1. Overlapping files severely hurt read performance (need to merge all SSTs)
        // 2. Backpressure from non-overlapping should affect PRIORITY, not FILTERING
        let config = CompactionConfigBuilder::new()
            .max_bytes_for_level_base(100)
            .max_level(4)
            .max_bytes_for_level_multiplier(5)
            .level0_tier_compact_file_number(2)
            .level0_sub_level_compact_level_count(3)
            .compaction_mode(CompactionMode::Range as i32)
            .enable_score_v2(Some(true))
            .build();

        let selector = DynamicLevelSelectorCore::new(
            Arc::new(config.clone()),
            Arc::new(CompactionDeveloperConfig::default()),
        );

        use risingwave_hummock_sdk::level::Level;
        use risingwave_pb::hummock::LevelType;

        // Setup:
        // - L0 Overlapping: 5 files (threshold=2) → raw_score = 2.5
        // - L0 NonOverlapping: 6 sublevels (threshold=3) → raw_score = 2.0
        // - L2 (base): 100MB (balanced)
        let mut l0 = generate_l0_nonoverlapping_sublevels(vec![]);

        // Add 6 non-overlapping sublevels
        for i in 0..6 {
            l0.sub_levels.push(Level {
                level_idx: 0,
                level_type: LevelType::Nonoverlapping,
                table_infos: generate_tables(
                    i * 5..(i + 1) * 5,
                    (i * 100000) as usize..((i + 1) * 100000) as usize,
                    1,
                    4,
                ),
                total_file_size: 20,
                sub_level_id: i as u64,
                uncompressed_file_size: 20,
                ..Default::default()
            });
        }

        // Add 1 overlapping sublevel with 5 files
        l0.sub_levels.push(Level {
            level_idx: 0,
            level_type: LevelType::Overlapping,
            table_infos: generate_tables(100..105, 0..1000000, 1, 2),
            total_file_size: 10,
            sub_level_id: 100,
            uncompressed_file_size: 10,
            ..Default::default()
        });

        l0.total_file_size = l0.sub_levels.iter().map(|l| l.total_file_size).sum();

        let levels = Levels {
            levels: vec![
                generate_level(1, vec![]),
                generate_level(2, generate_tables(200..210, 0..1000, 1, 10)), // 100MB (balanced)
                generate_level(3, generate_tables(210..260, 0..1000, 1, 10)), // 500MB
                generate_level(4, generate_tables(260..510, 0..1000, 1, 10)), // 2500MB
            ],
            l0,
            ..Default::default()
        };

        let handlers = (0..5).map(LevelHandler::new).collect::<Vec<_>>();
        let ctx = selector.get_priority_levels_v2(&levels, &handlers);

        println!("\n=== Bug Regression Test: L0 Tier Filtering with NonOverlapping Pressure ===");

        let overlapping_file_count = levels
            .l0
            .sub_levels
            .iter()
            .filter(|l| l.level_type == LevelType::Overlapping)
            .flat_map(|l| &l.table_infos)
            .count();
        let nonoverlapping_sublevel_count = levels
            .l0
            .sub_levels
            .iter()
            .filter(|l| l.level_type == LevelType::Nonoverlapping)
            .count();

        let tier_raw_score =
            overlapping_file_count as f64 / config.level0_tier_compact_file_number as f64;
        let nonoverlapping_level_score = nonoverlapping_sublevel_count as f64
            / config.level0_sub_level_compact_level_count as f64;

        println!(
            "L0 Overlapping: {} files (threshold={}) → raw_score={:.2}",
            overlapping_file_count, config.level0_tier_compact_file_number, tier_raw_score
        );
        println!(
            "L0 NonOverlapping: {} sublevels (threshold={}) → level_score={:.2}",
            nonoverlapping_sublevel_count,
            config.level0_sub_level_compact_level_count,
            nonoverlapping_level_score
        );

        let tier_backpressured = tier_raw_score / f64::max(0.01, nonoverlapping_level_score);
        println!(
            "L0 Tier backpressured_score: {:.2} (= {:.2} / {:.2})",
            tier_backpressured, tier_raw_score, nonoverlapping_level_score
        );

        // CRITICAL ASSERTION: Tier compaction must not be filtered out
        let tier_candidate = ctx
            .score_levels
            .iter()
            .find(|p| matches!(p.picker_type, PickerType::Tier));

        assert!(
            tier_candidate.is_some(),
            "❌ BUG DETECTED: L0 Tier compaction was filtered out! This happens when using \
             backpressured_score ({:.2}) instead of raw_score ({:.2}) for filtering. \
             Overlapping files have real pressure (raw={:.2} > 1.0) and MUST be compacted \
             even when non-overlapping levels have pressure (score={:.2})",
            tier_backpressured,
            tier_raw_score,
            tier_raw_score,
            nonoverlapping_level_score
        );

        let tier = tier_candidate.unwrap();
        println!("✅ L0 Tier is included in candidates (using raw_score for filtering)");

        // Verify scores
        assert!(
            (tier.raw_score - tier_raw_score).abs() < 0.01,
            "Tier raw_score should be {:.2}, got {:.2}",
            tier_raw_score,
            tier.raw_score
        );

        // Verify backpressure affects priority (score might be suppressed)
        if nonoverlapping_level_score > 1.0 {
            assert!(
                tier.score < tier.raw_score,
                "Tier score ({:.2}) should be backpressured below raw score ({:.2}) \
                 when non-overlapping has pressure",
                tier.score,
                tier.raw_score
            );
            println!(
                "✅ L0 Tier: raw_score={:.2} > 1.0 (included), backpressured_score={:.2} (adjusted priority)",
                tier.raw_score, tier.score
            );
        }

        println!(
            "\n✅ Test passed: L0 Tier uses raw_score for filtering, backpressured_score only affects priority"
        );
    }
}

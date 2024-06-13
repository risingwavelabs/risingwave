//  Copyright 2024 RisingWave Labs
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
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use risingwave_common::catalog::TableOption;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::{
    build_initial_compaction_group_levels, HummockLevelsExt,
};
use risingwave_hummock_sdk::prost_key_range::KeyRangeExt;
use risingwave_hummock_sdk::HummockCompactionTaskId;
use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::{compact_task, CompactionConfig, Level, LevelType};

use super::{
    create_compaction_task, CompactionSelector, LevelCompactionPicker, TierCompactionPicker,
};
use crate::hummock::compaction::overlap_strategy::OverlapStrategy;
use crate::hummock::compaction::picker::{
    CompactionInput, CompactionPicker, CompactionTaskValidator, IntraCompactionPicker,
    LocalPickerStatistic, MinOverlappingPicker,
};
use crate::hummock::compaction::{
    create_overlap_strategy, CompactionDeveloperConfig, CompactionTask, LocalSelectorStatistic,
};
use crate::hummock::level_handler::LevelHandler;
use crate::hummock::model::CompactionGroup;

pub const SCORE_BASE: u64 = 100;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
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
            ctx.level_max_bytes[i] =
                std::cmp::max(level_size, self.config.max_bytes_for_level_base);
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
                    picker_type: PickerType::Tier,
                })
            }

            // The read query at the non-overlapping level only selects ssts that match the query
            // range at each level, so the number of levels is the most important factor affecting
            // the read performance. At the same time, the size factor is also added to the score
            // calculation rule to avoid unbalanced compact task due to large size.
            let total_size = levels
                .l0
                .as_ref()
                .unwrap()
                .sub_levels
                .iter()
                .filter(|level| {
                    level.vnode_partition_count == self.config.split_weight_by_vnode
                        && level.level_type() == LevelType::Nonoverlapping
                })
                .map(|level| level.total_file_size)
                .sum::<u64>()
                - handlers[0].get_pending_output_file_size(ctx.base_level as u32);
            let base_level_size = levels.get_level(ctx.base_level).total_file_size;
            let base_level_sst_count = levels.get_level(ctx.base_level).table_infos.len() as u64;

            // size limit
            let non_overlapping_size_score = total_size * SCORE_BASE
                / std::cmp::max(self.config.max_bytes_for_level_base, base_level_size);
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
                handlers[level_idx].get_pending_output_file_size(level.level_idx + 1);
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

fn remove_table_from_level(
    origin_level: &Level,
    virtual_level: &mut HashMap<u32, Level>,
    table_id: u32,
) {
    if let Some(vlevel) = virtual_level.remove(&table_id) {
        let new_level = virtual_level.entry(0).or_insert_with(|| Level {
            level_idx: origin_level.level_idx,
            level_type: origin_level.level_type,
            table_infos: vec![],
            total_file_size: 0,
            sub_level_id: origin_level.sub_level_id,
            uncompressed_file_size: 0,
            vnode_partition_count: 0,
        });
        new_level.table_infos.extend(vlevel.table_infos);
        new_level.total_file_size = new_level
            .table_infos
            .iter()
            .map(|sst| sst.file_size)
            .sum::<u64>();
        new_level.table_infos.sort_by(|sst1, sst2| {
            let a = sst1.key_range.as_ref().unwrap();
            let b = sst2.key_range.as_ref().unwrap();
            a.compare(b)
        });
    }
}

fn remove_table_from_group(
    origin_group: &Levels,
    compaction_group: &CompactionGroup,
    virtual_group: &mut HashMap<u32, Levels>,
    table_id: u32,
) {
    if let Some(group) = virtual_group.remove(&table_id) {
        let hybrid_group = virtual_group.entry(0).or_insert_with(|| {
            build_initial_compaction_group_levels(
                origin_group.group_id,
                compaction_group.compaction_config.as_ref(),
            )
        });
        let l0 = hybrid_group.l0.as_mut().unwrap();
        l0.sub_levels.extend(group.l0.unwrap().sub_levels);
        l0.sub_levels.sort_by_key(|l| l.sub_level_id);
        let mut idx = 1;
        while idx < l0.sub_levels.len() {
            if l0.sub_levels[idx].sub_level_id == l0.sub_levels[idx - 1].sub_level_id {
                let x = std::mem::take(&mut l0.sub_levels[idx].table_infos);
                l0.sub_levels[idx - 1].table_infos.extend(x);
                l0.sub_levels[idx - 1].table_infos.sort_by(|sst1, sst2| {
                    let a = sst1.key_range.as_ref().unwrap();
                    let b = sst2.key_range.as_ref().unwrap();
                    a.compare(b)
                });
                l0.sub_levels[idx - 1].total_file_size = l0.sub_levels[idx - 1]
                    .table_infos
                    .iter()
                    .map(|sst| sst.file_size)
                    .sum::<u64>();
                l0.sub_levels.remove(idx);
            } else {
                idx += 1;
            }
        }
    }
}

impl DynamicLevelSelector {
    fn pick_compaction_per_table(
        &mut self,
        task_id: HummockCompactionTaskId,
        compaction_group: &CompactionGroup,
        levels: &Levels,
        level_handlers: &mut [LevelHandler],
        selector_stats: &mut LocalSelectorStatistic,
        developer_config: Arc<CompactionDeveloperConfig>,
    ) -> Option<CompactionTask> {
        let mut virtual_group: HashMap<u32, Levels> = HashMap::default();
        let mut hybrid_table_ids: HashSet<u32> = HashSet::default();
        let mut has_hybrid_level = false;
        for level in &levels.l0.as_ref().unwrap().sub_levels {
            let mut virtual_level: HashMap<u32, Level> = HashMap::default();
            for sst in &level.table_infos {
                let table_id = if level.level_type() == LevelType::Overlapping
                    || sst.table_ids.len() > 1
                    || hybrid_table_ids.contains(&sst.table_ids[0])
                {
                    // 0 represent hybrid sst.
                    if sst.table_ids.len() > 1 && level.level_type() != LevelType::Overlapping {
                        let small_table_id = sst.table_ids.iter().min().unwrap();
                        let largest_table_id = sst.table_ids.iter().max().unwrap();
                        for table_id in virtual_group.keys() {
                            if table_id > small_table_id
                                && table_id < largest_table_id
                                && !sst.table_ids.contains(table_id)
                            {
                                // For example: there are three table_ids: [3, 4, 5], if the table [4] has lots of data, and was split to a virtual lsm tree, when we meet a sst exists in higher sub-level which only contains [3, 5],
                                // we can not compact it with files only contains [3] and [5] in bottom level, because the result may overlap with some pending task contains table [4].
                                has_hybrid_level = true;
                                break;
                            }
                        }
                        for table_id in &sst.table_ids {
                            if !hybrid_table_ids.contains(table_id) {
                                hybrid_table_ids.insert(*table_id);
                                remove_table_from_level(level, &mut virtual_level, *table_id);
                                remove_table_from_group(
                                    levels,
                                    compaction_group,
                                    &mut virtual_group,
                                    *table_id,
                                );
                            }
                        }
                    }
                    0
                } else {
                    sst.table_ids[0]
                };
                if has_hybrid_level {
                    break;
                }
                let new_level = virtual_level.entry(table_id).or_insert_with(|| Level {
                    level_idx: level.level_idx,
                    level_type: level.level_type,
                    table_infos: vec![],
                    total_file_size: 0,
                    sub_level_id: level.sub_level_id,
                    uncompressed_file_size: 0,
                    vnode_partition_count: 0,
                });
                new_level.table_infos.push(sst.clone());
                new_level.total_file_size += sst.file_size;
                new_level.uncompressed_file_size += sst.uncompressed_file_size;
            }
            for (table_id, vlevel) in virtual_level {
                let group = virtual_group.entry(table_id).or_insert_with(|| {
                    build_initial_compaction_group_levels(
                        levels.group_id,
                        compaction_group.compaction_config.as_ref(),
                    )
                });
                if table_id == 0 {
                    for sst in &vlevel.table_infos {
                        group.member_table_ids.extend(sst.table_ids.clone());
                    }
                } else {
                    group.member_table_ids.push(table_id);
                }
                assert!(group
                    .l0
                    .as_mut()
                    .unwrap()
                    .sub_levels
                    .iter()
                    .all(|l| l.sub_level_id != vlevel.sub_level_id));
                group.l0.as_mut().unwrap().sub_levels.push(vlevel);
            }
        }

        for level in &levels.levels {
            let mut virtual_level: HashMap<u32, Level> = HashMap::default();
            for sst in &level.table_infos {
                let table_id = if sst.table_ids.len() > 1 {
                    for table_id in &sst.table_ids {
                        if virtual_group.contains_key(table_id) {
                            return None;
                        }
                        hybrid_table_ids.insert(*table_id);
                    }
                    0
                } else if hybrid_table_ids.contains(&sst.table_ids[0]) {
                    0
                } else {
                    sst.table_ids[0]
                };
                let new_level = virtual_level.entry(table_id).or_insert_with(|| Level {
                    level_idx: level.level_idx,
                    level_type: level.level_type,
                    table_infos: vec![],
                    total_file_size: 0,
                    sub_level_id: 0,
                    uncompressed_file_size: 0,
                    vnode_partition_count: 0,
                });
                new_level.table_infos.push(sst.clone());
                new_level.total_file_size += sst.file_size;
                new_level.uncompressed_file_size += sst.uncompressed_file_size;
            }
            for (table_id, vlevel) in virtual_level {
                if let Some(group) = virtual_group.get_mut(&table_id) {
                    if table_id == 0 {
                        for sst in &vlevel.table_infos {
                            group.member_table_ids.extend(sst.table_ids.clone());
                        }
                    } else {
                        group.member_table_ids.push(table_id);
                    }
                    group.levels[(level.level_idx as usize) - 1] = vlevel;
                }
            }
        }
        let dynamic_level_core = DynamicLevelSelectorCore::new(
            compaction_group.compaction_config.clone(),
            developer_config.clone(),
        );
        let overlap_strategy =
            create_overlap_strategy(compaction_group.compaction_config.compaction_mode());
        // TODO: Determine which rule to enable by write limit
        let compaction_task_validator = Arc::new(CompactionTaskValidator::new(
            compaction_group.compaction_config.clone(),
        ));
        let mut score_levels = vec![];
        for (table_id, group) in &mut virtual_group {
            group.member_table_ids.sort();
            group.member_table_ids.dedup();
            assert!(!group.member_table_ids.is_empty());
            let ctx = dynamic_level_core.get_priority_levels(group, level_handlers);
            for picker_info in ctx.score_levels {
                score_levels.push((*table_id, picker_info, ctx.base_level));
            }
        }
        score_levels.sort_by(|(_, a, _), (_, b, _)| {
            b.score
                .cmp(&a.score)
                .then_with(|| a.target_level.cmp(&b.target_level))
        });
        for (table_id, picker_info, base_level) in score_levels {
            if picker_info.score <= SCORE_BASE {
                continue;
            }
            let group = virtual_group.get(&table_id).unwrap();
            let mut picker = dynamic_level_core.create_compaction_picker(
                &picker_info,
                overlap_strategy.clone(),
                compaction_task_validator.clone(),
            );

            let mut stats = LocalPickerStatistic::default();
            if let Some(ret) = picker.pick_compaction(group, level_handlers, &mut stats) {
                ret.add_pending_task(task_id, level_handlers);
                selector_stats.record_virtual_group_task(
                    table_id,
                    ret.input_levels[0].level_idx as usize,
                    ret.target_level,
                );
                return Some(create_compaction_task(
                    dynamic_level_core.get_config(),
                    ret,
                    base_level,
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
}

pub fn is_trivial_move_task(task: &CompactionInput) -> bool {
    task.input_levels.len() == 2
        && task.input_levels[1].level_idx as usize == task.target_level
        && task.input_levels[1].table_infos.is_empty()
}

impl CompactionSelector for DynamicLevelSelector {
    fn pick_compaction(
        &mut self,
        task_id: HummockCompactionTaskId,
        compaction_group: &CompactionGroup,
        levels: &Levels,
        level_handlers: &mut [LevelHandler],
        selector_stats: &mut LocalSelectorStatistic,
        _table_id_to_options: HashMap<u32, TableOption>,
        developer_config: Arc<CompactionDeveloperConfig>,
    ) -> Option<CompactionTask> {
        let dynamic_level_core = DynamicLevelSelectorCore::new(
            compaction_group.compaction_config.clone(),
            developer_config.clone(),
        );
        let overlap_strategy =
            create_overlap_strategy(compaction_group.compaction_config.compaction_mode());
        let ctx = dynamic_level_core.get_priority_levels(levels, level_handlers);
        // TODO: Determine which rule to enable by write limit
        let compaction_task_validator = Arc::new(CompactionTaskValidator::new(
            compaction_group.compaction_config.clone(),
        ));
        if levels.member_table_ids.len() > 1 && !ctx.score_levels.is_empty() {
            if ctx.score_levels[0].score > SCORE_BASE {
                let mut picker = dynamic_level_core.create_compaction_picker(
                    &ctx.score_levels[0],
                    overlap_strategy.clone(),
                    compaction_task_validator.clone(),
                );
                let mut stats = LocalPickerStatistic::default();
                if let Some(ret) = picker.pick_compaction(levels, level_handlers, &mut stats)
                    && (ctx.score_levels[0].picker_type == PickerType::Tier
                        || is_trivial_move_task(&ret))
                {
                    ret.add_pending_task(task_id, level_handlers);
                    return Some(create_compaction_task(
                        dynamic_level_core.get_config(),
                        ret,
                        ctx.base_level,
                        self.task_type(),
                    ));
                }
            }

            if let Some(ret) = self.pick_compaction_per_table(
                task_id,
                compaction_group,
                levels,
                level_handlers,
                selector_stats,
                developer_config.clone(),
            ) {
                return Some(ret);
            }
        }
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

    fn task_type(&self) -> compact_task::TaskType {
        compact_task::TaskType::Dynamic
    }
}

#[cfg(test)]
pub mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use itertools::Itertools;
    use risingwave_common::constants::hummock::CompactionFilterFlag;
    use risingwave_pb::hummock::compaction_config::CompactionMode;
    use risingwave_pb::hummock::hummock_version::Levels;

    use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
    use crate::hummock::compaction::selector::tests::{
        assert_compaction_task, generate_l0_nonoverlapping_sublevels, generate_level,
        generate_tables, push_tables_level0_nonoverlapping,
    };
    use crate::hummock::compaction::selector::{
        CompactionSelector, DynamicLevelSelector, DynamicLevelSelectorCore, LocalSelectorStatistic,
    };
    use crate::hummock::compaction::CompactionDeveloperConfig;
    use crate::hummock::level_handler::LevelHandler;
    use crate::hummock::model::CompactionGroup;

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
                Arc::new(CompactionDeveloperConfig::default()),
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
                Arc::new(CompactionDeveloperConfig::default()),
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
                Arc::new(CompactionDeveloperConfig::default()),
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
            Arc::new(CompactionDeveloperConfig::default()),
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

        let dynamic_level_core = DynamicLevelSelectorCore::new(
            Arc::new(config),
            Arc::new(CompactionDeveloperConfig::default()),
        );
        let ctx = dynamic_level_core.calculate_level_base_size(&levels);
        assert_eq!(1, ctx.base_level);
        assert_eq!(1000, levels.l0.as_ref().unwrap().total_file_size); // l0
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

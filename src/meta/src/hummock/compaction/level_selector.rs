//  Copyright 2022 Singularity Data
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
use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::CompactionConfig;

use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
use crate::hummock::compaction::manual_compaction_picker::ManualCompactionPicker;
use crate::hummock::compaction::min_overlap_compaction_picker::MinOverlappingPicker;
use crate::hummock::compaction::overlap_strategy::OverlapStrategy;
use crate::hummock::compaction::{
    create_overlap_strategy, CompactionInput, CompactionPicker, CompactionTask,
    LevelCompactionPicker, ManualCompactionOption, TierCompactionPicker,
};
use crate::hummock::level_handler::LevelHandler;

const SCORE_BASE: u64 = 100;

pub trait LevelSelector: Sync + Send {
    fn need_compaction(&self, levels: &Levels, level_handlers: &[LevelHandler]) -> bool;

    fn pick_compaction(
        &self,
        task_id: HummockCompactionTaskId,
        levels: &Levels,
        level_handlers: &mut [LevelHandler],
    ) -> Option<CompactionTask>;

    fn manual_pick_compaction(
        &self,
        task_id: HummockCompactionTaskId,
        levels: &Levels,
        level_handlers: &mut [LevelHandler],
        option: ManualCompactionOption,
    ) -> Option<CompactionTask>;

    fn name(&self) -> &'static str;
}

#[derive(Default)]
pub struct SelectContext {
    level_max_bytes: Vec<u64>,

    // All data will be placed in the last level. When the cluster is empty, the files in L0 will
    // be compact to `max_level`, and the `max_level` would be `base_level`. When the total
    // size of the files in  `base_level` reaches its capacity, we will place data in a higher
    // level, which equals to `base_level -= 1;`.
    base_level: usize,
    score_levels: Vec<(u64, usize, usize)>,
}

// TODO: Set these configurations by meta rpc
pub struct DynamicLevelSelector {
    config: Arc<CompactionConfig>,
    overlap_strategy: Arc<dyn OverlapStrategy>,
}

impl Default for DynamicLevelSelector {
    fn default() -> Self {
        let config = Arc::new(CompactionConfigBuilder::new().build());
        let overlap_strategy = create_overlap_strategy(config.compaction_mode());
        DynamicLevelSelector::new(config, overlap_strategy)
    }
}

impl DynamicLevelSelector {
    pub fn new(config: Arc<CompactionConfig>, overlap_strategy: Arc<dyn OverlapStrategy>) -> Self {
        DynamicLevelSelector {
            config,
            overlap_strategy,
        }
    }

    fn create_compaction_picker(
        &self,
        select_level: usize,
        target_level: usize,
    ) -> Box<dyn CompactionPicker> {
        if select_level == 0 {
            if target_level == 0 {
                Box::new(TierCompactionPicker::new(
                    self.config.clone(),
                    self.overlap_strategy.clone(),
                ))
            } else {
                Box::new(LevelCompactionPicker::new(
                    target_level,
                    self.config.clone(),
                    self.overlap_strategy.clone(),
                ))
            }
        } else {
            assert_eq!(select_level + 1, target_level);
            Box::new(MinOverlappingPicker::new(
                select_level,
                target_level,
                self.config.max_bytes_for_level_base,
                self.overlap_strategy.clone(),
            ))
        }
    }

    // TODO: calculate this scores in apply compact result.
    /// `calculate_level_base_size` calculate base level and the base size of LSM tree build for
    /// current dataset. In other words,  `level_max_bytes` is our compaction goal which shall
    /// reach. This algorithm refers to the implementation in  `</>https://github.com/facebook/rocksdb/blob/v7.2.2/db/version_set.cc#L3706</>`
    fn calculate_level_base_size(&self, levels: &Levels) -> SelectContext {
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
        let max_l0_score = std::cmp::max(
            SCORE_BASE * 2,
            levels.l0.as_ref().unwrap().sub_levels.len() as u64 * SCORE_BASE
                / self.config.level0_tier_compact_file_number,
        );

        let total_size =
            levels.l0.as_ref().unwrap().total_file_size - handlers[0].get_pending_file_size();
        if idle_file_count > 0 {
            // trigger intra-l0 compaction at first when the number of files is too large.
            let l0_score =
                idle_file_count as u64 * SCORE_BASE / self.config.level0_tier_compact_file_number;
            ctx.score_levels
                .push((std::cmp::min(l0_score, max_l0_score), 0, 0));
            let score = total_size * SCORE_BASE / self.config.max_bytes_for_level_base;
            ctx.score_levels.push((score, 0, ctx.base_level));
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
            ctx.score_levels.push((
                total_size * SCORE_BASE / ctx.level_max_bytes[level_idx],
                level_idx,
                level_idx + 1,
            ));
        }

        // sort reverse to pick the largest one.
        ctx.score_levels.sort_by(|a, b| b.0.cmp(&a.0));
        ctx
    }

    fn create_compaction_task(&self, input: CompactionInput, base_level: usize) -> CompactionTask {
        let target_file_size = if input.target_level == 0 {
            self.config.target_file_size_base
        } else {
            assert!(input.target_level >= base_level);
            let step = (input.target_level - base_level) / 2;
            self.config.target_file_size_base << step
        };
        let compression_algorithm = if input.target_level == 0 {
            self.config.compression_algorithm[0].clone()
        } else {
            let idx = input.target_level - base_level + 1;
            self.config.compression_algorithm[idx].clone()
        };
        CompactionTask {
            input,
            compression_algorithm,
            target_file_size,
        }
    }
}

impl LevelSelector for DynamicLevelSelector {
    fn need_compaction(&self, levels: &Levels, level_handlers: &[LevelHandler]) -> bool {
        let ctx = self.get_priority_levels(levels, level_handlers);
        ctx.score_levels
            .first()
            .map(|(score, _, _)| *score > SCORE_BASE)
            .unwrap_or(false)
    }

    fn pick_compaction(
        &self,
        task_id: HummockCompactionTaskId,
        levels: &Levels,
        level_handlers: &mut [LevelHandler],
    ) -> Option<CompactionTask> {
        let ctx = self.get_priority_levels(levels, level_handlers);
        for (score, select_level, target_level) in ctx.score_levels {
            if score <= SCORE_BASE {
                return None;
            }
            let picker = self.create_compaction_picker(select_level, target_level);
            if let Some(ret) = picker.pick_compaction(levels, level_handlers) {
                ret.add_pending_task(task_id, level_handlers);
                return Some(self.create_compaction_task(ret, ctx.base_level));
            }
        }
        None
    }

    fn manual_pick_compaction(
        &self,
        task_id: HummockCompactionTaskId,
        levels: &Levels,
        level_handlers: &mut [LevelHandler],
        option: ManualCompactionOption,
    ) -> Option<CompactionTask> {
        let ctx = self.get_priority_levels(levels, level_handlers);
        let target_level = if option.level == 0 {
            ctx.base_level
        } else if option.level == self.config.max_level as usize {
            option.level
        } else {
            option.level + 1
        };
        if option.level > 0 && option.level < ctx.base_level {
            return None;
        }

        let picker =
            ManualCompactionPicker::new(self.overlap_strategy.clone(), option, target_level);

        let ret = picker.pick_compaction(levels, level_handlers)?;
        ret.add_pending_task(task_id, level_handlers);
        Some(self.create_compaction_task(ret, ctx.base_level))
    }

    fn name(&self) -> &'static str {
        "DynamicLevelSelector"
    }
}

#[cfg(test)]
pub mod tests {
    use std::collections::HashSet;
    use std::ops::Range;

    use itertools::Itertools;
    use risingwave_common::config::constant::hummock::CompactionFilterFlag;
    use risingwave_pb::hummock::compaction_config::CompactionMode;
    use risingwave_pb::hummock::{KeyRange, Level, LevelType, OverlappingLevel, SstableInfo};

    use super::*;
    use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
    use crate::hummock::compaction::overlap_strategy::RangeOverlapStrategy;
    use crate::hummock::test_utils::iterator_test_key_of_epoch;

    pub fn push_table_level0_overlapping(levels: &mut Levels, sst: SstableInfo) {
        levels.l0.as_mut().unwrap().total_file_size += sst.file_size;
        levels.l0.as_mut().unwrap().sub_levels.push(Level {
            level_idx: 0,
            level_type: LevelType::Overlapping as i32,
            total_file_size: sst.file_size,
            sub_level_id: sst.id,
            table_infos: vec![sst],
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
        let sub_level_id = table_infos[0].id;
        levels.l0.as_mut().unwrap().total_file_size += total_file_size;
        levels.l0.as_mut().unwrap().sub_levels.push(Level {
            level_idx: 0,
            level_type: LevelType::Nonoverlapping as i32,
            total_file_size,
            sub_level_id,
            table_infos,
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
            id,
            key_range: Some(KeyRange {
                left: iterator_test_key_of_epoch(table_prefix, left, epoch),
                right: iterator_test_key_of_epoch(table_prefix, right, epoch),
                inf: false,
            }),
            file_size: (right - left + 1) as u64,
            table_ids: vec![],
            meta_offset: 0,
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
        Level {
            level_idx,
            level_type: LevelType::Nonoverlapping as i32,
            table_infos,
            total_file_size,
            sub_level_id: 0,
        }
    }

    /// Returns a `OverlappingLevel`, with each `table_infos`'s element placed in a nonoverlapping
    /// sub-level.
    pub fn generate_l0_nonoverlapping_sublevels(table_infos: Vec<SstableInfo>) -> OverlappingLevel {
        let total_file_size = table_infos.iter().map(|table| table.file_size).sum::<u64>();
        OverlappingLevel {
            sub_levels: table_infos
                .into_iter()
                .enumerate()
                .map(|(idx, table)| Level {
                    level_idx: 0,
                    level_type: LevelType::Nonoverlapping as i32,
                    total_file_size: table.file_size,
                    sub_level_id: idx as u64,
                    table_infos: vec![table],
                })
                .collect_vec(),
            total_file_size,
        }
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
                })
                .collect_vec(),
            total_file_size: 0,
        };
        l0.total_file_size = l0.sub_levels.iter().map(|l| l.total_file_size).sum::<u64>();
        l0
    }

    fn assert_compaction_task(compact_task: &CompactionTask, level_handlers: &[LevelHandler]) {
        for i in &compact_task.input.input_levels {
            for t in &i.table_infos {
                assert!(level_handlers[i.level_idx as usize].is_pending_compact(&t.id));
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
            .level0_trigger_file_number(1)
            .level0_tier_compact_file_number(2)
            .compaction_mode(CompactionMode::Range as i32)
            .build();
        let selector =
            DynamicLevelSelector::new(Arc::new(config), Arc::new(RangeOverlapStrategy::default()));
        let levels = vec![
            generate_level(1, vec![]),
            generate_level(2, generate_tables(0..5, 0..1000, 3, 10)),
            generate_level(3, generate_tables(5..10, 0..1000, 2, 50)),
            generate_level(4, generate_tables(10..15, 0..1000, 1, 200)),
        ];
        let mut levels = Levels {
            levels,
            l0: Some(generate_l0_nonoverlapping_sublevels(vec![])),
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
            .max_compaction_bytes(10000)
            .level0_trigger_file_number(8)
            .level0_tier_compact_file_number(4)
            .compaction_mode(CompactionMode::Range as i32)
            .build();
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
        };

        let selector = DynamicLevelSelector::new(
            Arc::new(config.clone()),
            Arc::new(RangeOverlapStrategy::default()),
        );
        let mut levels_handlers = (0..5).into_iter().map(LevelHandler::new).collect_vec();
        let compaction = selector
            .pick_compaction(1, &levels, &mut levels_handlers)
            .unwrap();
        // trivial move.
        assert_compaction_task(&compaction, &levels_handlers);
        assert_eq!(compaction.input.input_levels[0].level_idx, 0);
        assert!(compaction.input.input_levels[1].table_infos.is_empty());
        assert_eq!(compaction.input.target_level, 0);

        let compaction_filter_flag = CompactionFilterFlag::STATE_CLEAN | CompactionFilterFlag::TTL;
        let config = CompactionConfigBuilder::new_with(config)
            .max_bytes_for_level_base(100)
            .level0_trigger_file_number(8)
            .compaction_filter_mask(compaction_filter_flag.into())
            .build();
        let selector = DynamicLevelSelector::new(
            Arc::new(config.clone()),
            Arc::new(RangeOverlapStrategy::default()),
        );

        levels.l0.as_mut().unwrap().sub_levels.clear();
        levels.l0.as_mut().unwrap().total_file_size = 0;
        push_tables_level0_nonoverlapping(&mut levels, generate_tables(15..25, 0..600, 3, 20));
        let mut levels_handlers = (0..5).into_iter().map(LevelHandler::new).collect_vec();
        let compaction = selector
            .pick_compaction(1, &levels, &mut levels_handlers)
            .unwrap();
        assert_compaction_task(&compaction, &levels_handlers);
        assert_eq!(compaction.input.input_levels[0].level_idx, 0);
        assert_eq!(compaction.input.target_level, 2);
        assert_eq!(compaction.target_file_size, config.target_file_size_base);

        levels_handlers[0].remove_task(1);
        levels_handlers[2].remove_task(1);
        levels.l0.as_mut().unwrap().sub_levels.clear();
        levels.levels[1].table_infos = generate_tables(20..30, 0..1000, 3, 10);
        let compaction = selector
            .pick_compaction(2, &levels, &mut levels_handlers)
            .unwrap();
        assert_compaction_task(&compaction, &levels_handlers);
        assert_eq!(compaction.input.input_levels[0].level_idx, 3);
        assert_eq!(compaction.input.target_level, 4);
        assert_eq!(compaction.input.input_levels[0].table_infos.len(), 1);
        assert_eq!(compaction.input.input_levels[1].table_infos.len(), 1);
        assert_eq!(
            compaction.target_file_size,
            config.target_file_size_base * 2
        );
        assert_eq!(compaction.compression_algorithm.as_str(), "Lz4",);
        // no compaction need to be scheduled because we do not calculate the size of pending files
        // to score.
        let compaction = selector.pick_compaction(2, &levels, &mut levels_handlers);
        assert!(compaction.is_none());
    }

    #[test]
    fn test_manual_compaction_picker_l0() {
        let config = Arc::new(CompactionConfigBuilder::new().max_level(4).build());
        let selector = DynamicLevelSelector::new(config, Arc::new(RangeOverlapStrategy::default()));
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
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(2, 1, 0, 100, 1),
                    generate_table(3, 1, 101, 200, 1),
                    generate_table(4, 1, 222, 300, 1),
                ],
                total_file_size: 0,
                sub_level_id: 0,
            },
        ];
        assert_eq!(levels.len(), 4);
        let levels = Levels {
            levels,
            l0: Some(l0),
        };
        let mut levels_handler = (0..5).into_iter().map(LevelHandler::new).collect_vec();

        // pick_l0_to_sub_level
        {
            let option = ManualCompactionOption {
                sst_ids: vec![0, 1],
                key_range: KeyRange {
                    left: vec![],
                    right: vec![],
                    inf: true,
                },
                internal_table_id: HashSet::default(),
                level: 0,
            };
            let task = selector
                .manual_pick_compaction(1, &levels, &mut levels_handler, option)
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
                    left: vec![],
                    right: vec![],
                    inf: true,
                },
                internal_table_id: HashSet::default(),
                level: 0,
            };
            let task = selector
                .manual_pick_compaction(2, &levels, &mut levels_handler, option)
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
    fn test_manual_compaction_picker() {
        let config = Arc::new(CompactionConfigBuilder::new().max_level(4).build());
        let selector = DynamicLevelSelector::new(config, Arc::new(RangeOverlapStrategy::default()));
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
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(2, 1, 0, 100, 1),
                    generate_table(3, 1, 101, 200, 1),
                    generate_table(4, 1, 222, 300, 1),
                ],
                total_file_size: 0,
                sub_level_id: 0,
            },
        ];
        assert_eq!(levels.len(), 4);
        let levels = Levels {
            levels,
            l0: Some(l0),
        };
        let mut levels_handler = (0..5).into_iter().map(LevelHandler::new).collect_vec();

        // pick l3 -> l4
        {
            let option = ManualCompactionOption {
                sst_ids: vec![0, 1],
                key_range: KeyRange {
                    left: vec![],
                    right: vec![],
                    inf: true,
                },
                internal_table_id: HashSet::default(),
                level: 3,
            };
            let task = selector
                .manual_pick_compaction(1, &levels, &mut levels_handler, option)
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
                    left: vec![],
                    right: vec![],
                    inf: true,
                },
                internal_table_id: HashSet::default(),
                level: 4,
            };
            let task = selector
                .manual_pick_compaction(1, &levels, &mut levels_handler, option)
                .unwrap();
            assert_compaction_task(&task, &levels_handler);
            assert_eq!(task.input.input_levels.len(), 2);
            assert_eq!(task.input.input_levels[0].level_idx, 4);
            assert_eq!(task.input.input_levels[0].table_infos.len(), 3);
            assert_eq!(task.input.input_levels[1].level_idx, 4);
            assert_eq!(task.input.input_levels[1].table_infos.len(), 0);
            assert_eq!(task.input.target_level, 4);
        }
    }
}

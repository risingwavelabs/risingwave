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

mod emergency_selector;
mod level_selector;
mod manual_selector;
mod space_reclaim_selector;
mod tombstone_compaction_selector;
mod ttl_selector;

use std::collections::HashMap;

pub use emergency_selector::EmergencySelector;
pub use level_selector::{DynamicLevelSelector, DynamicLevelSelectorCore};
pub use manual_selector::{ManualCompactionOption, ManualCompactionSelector};
use risingwave_common::catalog::TableOption;
use risingwave_hummock_sdk::HummockCompactionTaskId;
use risingwave_pb::hummock::compact_task;
use risingwave_pb::hummock::hummock_version::Levels;
pub use space_reclaim_selector::SpaceReclaimCompactionSelector;
pub use tombstone_compaction_selector::TombstoneCompactionSelector;
pub use ttl_selector::TtlCompactionSelector;

use super::picker::LocalPickerStatistic;
use super::{create_compaction_task, LevelCompactionPicker, TierCompactionPicker};
use crate::hummock::compaction::CompactionTask;
use crate::hummock::level_handler::LevelHandler;
use crate::hummock::model::CompactionGroup;
use crate::rpc::metrics::MetaMetrics;

pub trait CompactionSelector: Sync + Send {
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

pub fn default_compaction_selector() -> Box<dyn CompactionSelector> {
    Box::<DynamicLevelSelector>::default()
}

#[derive(Default)]
pub struct LocalSelectorStatistic {
    skip_picker: Vec<(usize, usize, LocalPickerStatistic)>,
}

impl LocalSelectorStatistic {
    pub fn report_to_metrics(&self, group_id: u64, metrics: &MetaMetrics) {
        for (start_level, target_level, stats) in &self.skip_picker {
            let level_label = format!("cg{}-{}-to-{}", group_id, start_level, target_level);
            if stats.skip_by_write_amp_limit > 0 {
                metrics
                    .compact_skip_frequency
                    .with_label_values(&[level_label.as_str(), "write-amp"])
                    .inc();
            }
            if stats.skip_by_count_limit > 0 {
                metrics
                    .compact_skip_frequency
                    .with_label_values(&[level_label.as_str(), "count"])
                    .inc();
            }
            if stats.skip_by_pending_files > 0 {
                metrics
                    .compact_skip_frequency
                    .with_label_values(&[level_label.as_str(), "pending-files"])
                    .inc();
            }
            if stats.skip_by_overlapping > 0 {
                metrics
                    .compact_skip_frequency
                    .with_label_values(&[level_label.as_str(), "overlapping"])
                    .inc();
            }
            metrics
                .compact_skip_frequency
                .with_label_values(&[level_label.as_str(), "picker"])
                .inc();
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::ops::Range;

    use itertools::Itertools;
    use risingwave_pb::hummock::{KeyRange, Level, LevelType, OverlappingLevel, SstableInfo};

    use super::*;
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
}

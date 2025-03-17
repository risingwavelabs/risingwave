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

mod emergency_selector;
pub(crate) mod level_selector;
mod manual_selector;
mod space_reclaim_selector;
mod tombstone_compaction_selector;
mod ttl_selector;
mod vnode_watermark_selector;

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

pub use emergency_selector::EmergencySelector;
pub use level_selector::{DynamicLevelSelector, DynamicLevelSelectorCore};
pub use manual_selector::{ManualCompactionOption, ManualCompactionSelector};
use risingwave_common::catalog::{TableId, TableOption};
use risingwave_hummock_sdk::HummockCompactionTaskId;
use risingwave_hummock_sdk::level::Levels;
use risingwave_hummock_sdk::table_watermark::TableWatermarks;
use risingwave_hummock_sdk::version::HummockVersionStateTableInfo;
use risingwave_pb::hummock::compact_task;
pub use space_reclaim_selector::SpaceReclaimCompactionSelector;
pub use tombstone_compaction_selector::TombstoneCompactionSelector;
pub use ttl_selector::TtlCompactionSelector;
pub use vnode_watermark_selector::VnodeWatermarkCompactionSelector;

use super::picker::LocalPickerStatistic;
use super::{
    CompactionDeveloperConfig, LevelCompactionPicker, TierCompactionPicker, create_compaction_task,
};
use crate::hummock::compaction::CompactionTask;
use crate::hummock::level_handler::LevelHandler;
use crate::hummock::model::CompactionGroup;
use crate::rpc::metrics::MetaMetrics;

pub struct CompactionSelectorContext<'a> {
    pub group: &'a CompactionGroup,
    pub levels: &'a Levels,
    pub member_table_ids: &'a BTreeSet<TableId>,
    pub level_handlers: &'a mut [LevelHandler],
    pub selector_stats: &'a mut LocalSelectorStatistic,
    pub table_id_to_options: &'a HashMap<u32, TableOption>,
    pub developer_config: Arc<CompactionDeveloperConfig>,
    pub table_watermarks: &'a HashMap<TableId, Arc<TableWatermarks>>,
    pub state_table_info: &'a HummockVersionStateTableInfo,
}

pub trait CompactionSelector: Sync + Send {
    fn pick_compaction(
        &mut self,
        task_id: HummockCompactionTaskId,
        context: CompactionSelectorContext<'_>,
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
    use risingwave_hummock_sdk::key_range::KeyRange;
    use risingwave_hummock_sdk::level::{Level, OverlappingLevel};
    use risingwave_hummock_sdk::sstable_info::{SstableInfo, SstableInfoInner};
    use risingwave_pb::hummock::LevelType;

    use super::*;
    use crate::hummock::test_utils::iterator_test_key_of_epoch;

    pub fn push_table_level0_overlapping(levels: &mut Levels, sst: SstableInfo) {
        levels.l0.total_file_size += sst.sst_size;
        levels.l0.sub_levels.push(Level {
            level_idx: 0,
            level_type: LevelType::Overlapping,
            total_file_size: sst.sst_size,
            uncompressed_file_size: sst.uncompressed_file_size,
            sub_level_id: sst.sst_id,
            table_infos: vec![sst],
            ..Default::default()
        });
    }

    pub fn push_table_level0_nonoverlapping(levels: &mut Levels, sst: SstableInfo) {
        push_table_level0_overlapping(levels, sst);
        levels.l0.sub_levels.last_mut().unwrap().level_type = LevelType::Nonoverlapping;
    }

    pub fn push_tables_level0_nonoverlapping(levels: &mut Levels, table_infos: Vec<SstableInfo>) {
        let total_file_size = table_infos.iter().map(|table| table.sst_size).sum::<u64>();
        let uncompressed_file_size = table_infos
            .iter()
            .map(|table| table.uncompressed_file_size)
            .sum();
        let sub_level_id = table_infos[0].sst_id;
        levels.l0.total_file_size += total_file_size;
        levels.l0.sub_levels.push(Level {
            level_idx: 0,
            level_type: LevelType::Nonoverlapping,
            total_file_size,
            sub_level_id,
            table_infos,
            uncompressed_file_size,
            ..Default::default()
        });
    }

    pub fn generate_table(
        id: u64,
        table_prefix: u64,
        left: usize,
        right: usize,
        epoch: u64,
    ) -> SstableInfo {
        generate_table_impl(id, table_prefix, left, right, epoch).into()
    }

    pub fn generate_table_impl(
        id: u64,
        table_prefix: u64,
        left: usize,
        right: usize,
        epoch: u64,
    ) -> SstableInfoInner {
        let object_size = (right - left + 1) as u64;
        SstableInfoInner {
            object_id: id,
            sst_id: id,
            key_range: KeyRange {
                left: iterator_test_key_of_epoch(table_prefix, left, epoch).into(),
                right: iterator_test_key_of_epoch(table_prefix, right, epoch).into(),
                right_exclusive: false,
            },
            file_size: object_size,
            table_ids: vec![table_prefix as u32],
            uncompressed_file_size: (right - left + 1) as u64,
            total_key_count: (right - left + 1) as u64,
            sst_size: object_size,
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
        let object_size = (right - left + 1) as u64;
        SstableInfoInner {
            object_id: id,
            sst_id: id,
            key_range: KeyRange {
                left: iterator_test_key_of_epoch(table_prefix, left, epoch).into(),
                right: iterator_test_key_of_epoch(table_prefix, right, epoch).into(),
                right_exclusive: false,
            },
            file_size: object_size,
            table_ids,
            uncompressed_file_size: object_size,
            min_epoch,
            max_epoch,
            sst_size: object_size,
            ..Default::default()
        }
        .into()
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
            let mut table = generate_table_impl(id, 1, start, start + step - 1, epoch);
            table.file_size = file_size;
            table.sst_size = file_size;
            tables.push(table.into());
            start += step;
        }
        tables
    }

    pub fn generate_level(level_idx: u32, table_infos: Vec<SstableInfo>) -> Level {
        let total_file_size = table_infos.iter().map(|sst| sst.sst_size).sum();
        let uncompressed_file_size = table_infos
            .iter()
            .map(|sst| sst.uncompressed_file_size)
            .sum();
        Level {
            level_idx,
            level_type: LevelType::Nonoverlapping,
            table_infos,
            total_file_size,
            sub_level_id: 0,
            uncompressed_file_size,
            ..Default::default()
        }
    }

    /// Returns a `OverlappingLevel`, with each `table_infos`'s element placed in a nonoverlapping
    /// sub-level.
    pub fn generate_l0_nonoverlapping_sublevels(table_infos: Vec<SstableInfo>) -> OverlappingLevel {
        let total_file_size = table_infos.iter().map(|table| table.sst_size).sum::<u64>();
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
                    level_type: LevelType::Nonoverlapping,
                    total_file_size: table.sst_size,
                    uncompressed_file_size: table.uncompressed_file_size,
                    sub_level_id: idx as u64,
                    table_infos: vec![table],
                    ..Default::default()
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
                    level_type: LevelType::Nonoverlapping,
                    total_file_size: table.iter().map(|table| table.sst_size).sum::<u64>(),
                    uncompressed_file_size: table
                        .iter()
                        .map(|sst| sst.uncompressed_file_size)
                        .sum::<u64>(),
                    sub_level_id: idx as u64,
                    table_infos: table,
                    ..Default::default()
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
                    level_type: LevelType::Overlapping,
                    total_file_size: table.iter().map(|table| table.sst_size).sum::<u64>(),
                    sub_level_id: idx as u64,
                    table_infos: table.clone(),
                    uncompressed_file_size: table
                        .iter()
                        .map(|sst| sst.uncompressed_file_size)
                        .sum::<u64>(),
                    ..Default::default()
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

    pub fn assert_compaction_task(compact_task: &CompactionTask, level_handlers: &[LevelHandler]) {
        for i in &compact_task.input.input_levels {
            for t in &i.table_infos {
                assert!(level_handlers[i.level_idx as usize].is_pending_compact(&t.sst_id));
            }
        }
    }
}

// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;
use risingwave_hummock_sdk::can_concat;
use risingwave_hummock_sdk::filter_key_extractor::FilterKeyExtractorImpl;
use risingwave_hummock_sdk::key_range::{KeyRange, KeyRangeCommon};
use risingwave_pb::hummock::{CompactTask, LevelType};

use super::task_progress::TaskProgress;
use crate::hummock::compactor::delete_range_aggregator::DeleteRangeAggregator;
use crate::hummock::compactor::iterator::ConcatSstableIterator;
use crate::hummock::compactor::{
    CompactOutput, CompactionFilter, Compactor, CompactorContext, CompactorSstableStoreRef,
};
use crate::hummock::iterator::{Forward, HummockIterator, UnorderedMergeIteratorInner};
use crate::hummock::{CachePolicy, CompressionAlgorithm, HummockResult, SstableBuilderOptions};
use crate::monitor::StoreLocalStatistic;

#[derive(Clone)]
pub struct CompactorRunner {
    compact_task: CompactTask,
    compactor: Compactor,
    sstable_store: CompactorSstableStoreRef,
    key_range: KeyRange,
    split_index: usize,
}

impl CompactorRunner {
    pub fn new(split_index: usize, context: &CompactorContext, task: CompactTask) -> Self {
        let max_target_file_size = context.context.options.sstable_size_mb as usize * (1 << 20);
        let total_file_size = task
            .input_ssts
            .iter()
            .flat_map(|level| level.table_infos.iter())
            .map(|table| table.file_size)
            .sum::<u64>();
        let mut options: SstableBuilderOptions = context.context.options.as_ref().into();
        options.capacity = std::cmp::min(task.target_file_size as usize, max_target_file_size);
        options.compression_algorithm = match task.compression_algorithm {
            0 => CompressionAlgorithm::None,
            1 => CompressionAlgorithm::Lz4,
            _ => CompressionAlgorithm::Zstd,
        };
        let total_file_size = (total_file_size as f64 * 1.2).round() as usize;
        if options.compression_algorithm == CompressionAlgorithm::None {
            options.capacity = std::cmp::min(options.capacity, total_file_size);
        }
        let key_range = KeyRange {
            left: Bytes::copy_from_slice(task.splits[split_index].get_left()),
            right: Bytes::copy_from_slice(task.splits[split_index].get_right()),
            inf: task.splits[split_index].get_inf(),
        };
        let compactor = Compactor::new(
            context.context.clone(),
            options,
            key_range.clone(),
            CachePolicy::NotFill,
            task.gc_delete_keys,
            task.watermark,
        );

        Self {
            compactor,
            compact_task: task,
            sstable_store: context.sstable_store.clone(),
            key_range,
            split_index,
        }
    }

    pub async fn run(
        &self,
        compaction_filter: impl CompactionFilter,
        filter_key_extractor: Arc<FilterKeyExtractorImpl>,
        task_progress: Arc<TaskProgress>,
    ) -> HummockResult<CompactOutput> {
        let iter = self.build_sst_iter()?;
        let del_agg = self.build_delete_range_iter().await?;
        let ssts = self
            .compactor
            .compact_key_range(
                iter,
                compaction_filter,
                del_agg,
                filter_key_extractor,
                Some(task_progress),
            )
            .await?;
        Ok((self.split_index, ssts))
    }

    async fn build_delete_range_iter(&self) -> HummockResult<DeleteRangeAggregator> {
        let mut aggregator = DeleteRangeAggregator::new(
            self.key_range.clone(),
            self.compact_task.watermark,
            self.compact_task.gc_delete_keys,
        );
        let mut local_stats = StoreLocalStatistic::default();
        for level in &self.compact_task.input_ssts {
            if level.table_infos.is_empty() {
                continue;
            }

            for table_info in &level.table_infos {
                let key_range = KeyRange::from(table_info.key_range.as_ref().unwrap());
                if !self.key_range.full_key_overlap(&key_range) {
                    continue;
                }
                let table = self
                    .compactor
                    .context
                    .sstable_store
                    .sstable(table_info, &mut local_stats)
                    .await?;
                aggregator.add_tombstone(table.value().meta.range_tombstone_list.clone());
            }
        }
        Ok(aggregator)
    }

    /// Build the merge iterator based on the given input ssts.
    fn build_sst_iter(&self) -> HummockResult<impl HummockIterator<Direction = Forward>> {
        let mut table_iters = Vec::new();

        for level in &self.compact_task.input_ssts {
            if level.table_infos.is_empty() {
                continue;
            }

            // Do not need to filter the table because manager has done it.
            if level.level_type == LevelType::Nonoverlapping as i32 {
                debug_assert!(can_concat(&level.table_infos.iter().collect_vec()));
                let tables = level
                    .table_infos
                    .iter()
                    .filter(|info| {
                        let key_range = KeyRange::from(info.key_range.as_ref().unwrap());
                        self.key_range.full_key_overlap(&key_range)
                    })
                    .cloned()
                    .collect_vec();
                table_iters.push(ConcatSstableIterator::new(
                    tables,
                    self.compactor.task_config.key_range.clone(),
                    self.sstable_store.clone(),
                ));
            } else {
                for table_info in &level.table_infos {
                    let key_range = KeyRange::from(table_info.key_range.as_ref().unwrap());
                    if !self.key_range.full_key_overlap(&key_range) {
                        continue;
                    }
                    table_iters.push(ConcatSstableIterator::new(
                        vec![table_info.clone()],
                        self.compactor.task_config.key_range.clone(),
                        self.sstable_store.clone(),
                    ));
                }
            }
        }
        Ok(UnorderedMergeIteratorInner::for_compactor(table_iters))
    }
}

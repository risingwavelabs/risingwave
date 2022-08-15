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
use risingwave_hummock_sdk::filter_key_extractor::FilterKeyExtractorImpl;
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_pb::hummock::{CompactTask, LevelType};

use crate::hummock::compactor::{CompactOutput, CompactionFilter, Compactor, CompactorContext};
use crate::hummock::iterator::{
    ConcatSstableIterator, Forward, HummockIterator, UnorderedMergeIteratorInner,
};
use crate::hummock::sstable::SstableIteratorReadOptions;
use crate::hummock::utils::can_concat;
use crate::hummock::{CachePolicy, CompressionAlgorithm, HummockResult, SstableBuilderOptions};

#[derive(Clone)]
pub struct CompactorRunner {
    compact_task: CompactTask,
    compactor: Compactor,
}

impl CompactorRunner {
    pub fn new(context: Arc<CompactorContext>, task: CompactTask) -> Self {
        let max_target_file_size = context.options.sstable_size_mb as usize * (1 << 20);
        let cache_policy = if task.target_level == 0 {
            CachePolicy::Fill
        } else {
            CachePolicy::NotFill
        };
        let mut options: SstableBuilderOptions = context.options.as_ref().into();
        options.capacity = std::cmp::min(task.target_file_size as usize, max_target_file_size);
        options.compression_algorithm = match task.compression_algorithm {
            0 => CompressionAlgorithm::None,
            1 => CompressionAlgorithm::Lz4,
            _ => CompressionAlgorithm::Zstd,
        };
        let compactor = Compactor::new(
            context.clone(),
            options,
            context.memory_limiter.clone(),
            task.splits
                .iter()
                .map(|split| KeyRange {
                    left: Bytes::copy_from_slice(split.get_left()),
                    right: Bytes::copy_from_slice(split.get_right()),
                    inf: split.get_inf(),
                })
                .collect_vec(),
            cache_policy,
            task.gc_delete_keys,
            task.watermark,
        );
        Self {
            compactor,
            compact_task: task,
        }
    }

    pub async fn run(
        &self,
        split_index: usize,
        compaction_filter: impl CompactionFilter,
        filter_key_extractor: Arc<FilterKeyExtractorImpl>,
    ) -> HummockResult<CompactOutput> {
        let iter = self.build_sst_iter()?;
        self.compactor
            .compact_key_range_impl(split_index, iter, compaction_filter, filter_key_extractor)
            .await
    }

    /// Build the merge iterator based on the given input ssts.
    fn build_sst_iter(&self) -> HummockResult<impl HummockIterator<Direction = Forward>> {
        let mut table_iters = Vec::new();
        let read_options = Arc::new(SstableIteratorReadOptions { prefetch: true });

        for level in &self.compact_task.input_ssts {
            if level.table_infos.is_empty() {
                continue;
            }
            // Do not need to filter the table because manager has done it.

            if level.level_type == LevelType::Nonoverlapping as i32 {
                debug_assert!(can_concat(&level.table_infos.iter().collect_vec()));
                table_iters.push(ConcatSstableIterator::new(
                    level.table_infos.clone(),
                    self.compactor.context.sstable_store.clone(),
                    read_options.clone(),
                ));
            } else {
                for table_info in &level.table_infos {
                    table_iters.push(ConcatSstableIterator::new(
                        vec![table_info.clone()],
                        self.compactor.context.sstable_store.clone(),
                        read_options.clone(),
                    ));
                }
            }
        }
        Ok(UnorderedMergeIteratorInner::new(
            table_iters,
            self.compactor.context.stats.clone(),
        ))
    }
}

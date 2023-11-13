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

use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::constants::hummock::CompactionFilterFlag;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::prost_key_range::KeyRangeExt;
use risingwave_hummock_sdk::table_stats::TableStatsMap;
use risingwave_hummock_sdk::{EpochWithGap, KeyComparator};
use risingwave_pb::hummock::{compact_task, CompactTask, KeyRange as KeyRange_vec, SstableInfo};
use tokio::time::Instant;

pub use super::context::CompactorContext;
use crate::filter_key_extractor::FilterKeyExtractorImpl;
use crate::hummock::compactor::{
    MultiCompactionFilter, StateCleanUpCompactionFilter, TtlCompactionFilter,
};
use crate::hummock::multi_builder::TableBuilderFactory;
use crate::hummock::sstable::DEFAULT_ENTRY_SIZE;
use crate::hummock::{
    CachePolicy, FilterBuilder, GetObjectId, HummockResult, MemoryLimiter, SstableBuilder,
    SstableBuilderOptions, SstableWriterFactory, SstableWriterOptions,
};
use crate::monitor::StoreLocalStatistic;

pub struct RemoteBuilderFactory<W: SstableWriterFactory, F: FilterBuilder> {
    pub object_id_getter: Box<dyn GetObjectId>,
    pub limiter: Arc<MemoryLimiter>,
    pub options: SstableBuilderOptions,
    pub policy: CachePolicy,
    pub remote_rpc_cost: Arc<AtomicU64>,
    pub filter_key_extractor: Arc<FilterKeyExtractorImpl>,
    pub sstable_writer_factory: W,
    pub _phantom: PhantomData<F>,
}

#[async_trait::async_trait]
impl<W: SstableWriterFactory, F: FilterBuilder> TableBuilderFactory for RemoteBuilderFactory<W, F> {
    type Filter = F;
    type Writer = W::Writer;

    async fn open_builder(&mut self) -> HummockResult<SstableBuilder<Self::Writer, Self::Filter>> {
        let timer = Instant::now();
        let table_id = self.object_id_getter.get_new_sst_object_id().await?;
        let cost = (timer.elapsed().as_secs_f64() * 1000000.0).round() as u64;
        self.remote_rpc_cost.fetch_add(cost, Ordering::Relaxed);
        let writer_options = SstableWriterOptions {
            capacity_hint: Some(self.options.capacity + self.options.block_capacity),
            tracker: None,
            policy: self.policy,
        };
        let writer = self
            .sstable_writer_factory
            .create_sst_writer(table_id, writer_options)
            .await?;
        let builder = SstableBuilder::new(
            table_id,
            writer,
            Self::Filter::create(
                self.options.bloom_false_positive,
                self.options.capacity / DEFAULT_ENTRY_SIZE + 1,
            ),
            self.options.clone(),
            self.filter_key_extractor.clone(),
            Some(self.limiter.clone()),
        );
        Ok(builder)
    }
}

/// `CompactionStatistics` will count the results of each compact split
#[derive(Default, Debug)]
pub struct CompactionStatistics {
    // to report per-table metrics
    pub delta_drop_stat: TableStatsMap,

    // to calculate delete ratio
    pub iter_total_key_counts: u64,
    pub iter_drop_key_counts: u64,
}

impl CompactionStatistics {
    #[allow(dead_code)]
    fn delete_ratio(&self) -> Option<u64> {
        if self.iter_total_key_counts == 0 {
            return None;
        }

        Some(self.iter_drop_key_counts / self.iter_total_key_counts)
    }
}

#[derive(Clone, Default)]
pub struct TaskConfig {
    pub key_range: KeyRange,
    pub cache_policy: CachePolicy,
    pub gc_delete_keys: bool,
    pub watermark: u64,
    /// `stats_target_table_ids` decides whether a dropped key should be counted as table stats
    /// change. For an divided SST as input, a dropped key shouldn't be counted if its table id
    /// doesn't belong to this divided SST. See `Compactor::compact_and_build_sst`.
    pub stats_target_table_ids: Option<HashSet<u32>>,
    pub task_type: compact_task::TaskType,
    pub is_target_l0_or_lbase: bool,
    pub use_block_based_filter: bool,

    pub table_vnode_partition: HashMap<u32, u32>,
}

pub fn build_multi_compaction_filter(compact_task: &CompactTask) -> MultiCompactionFilter {
    use risingwave_common::catalog::TableOption;
    let mut multi_filter = MultiCompactionFilter::default();
    let compaction_filter_flag =
        CompactionFilterFlag::from_bits(compact_task.compaction_filter_mask).unwrap_or_default();
    if compaction_filter_flag.contains(CompactionFilterFlag::STATE_CLEAN) {
        let state_clean_up_filter = Box::new(StateCleanUpCompactionFilter::new(
            HashSet::from_iter(compact_task.existing_table_ids.clone()),
        ));

        multi_filter.register(state_clean_up_filter);
    }

    if compaction_filter_flag.contains(CompactionFilterFlag::TTL) {
        let id_to_ttl = compact_task
            .table_options
            .iter()
            .filter(|id_to_option| {
                let table_option: TableOption = id_to_option.1.into();
                table_option.retention_seconds.is_some()
            })
            .map(|id_to_option| (*id_to_option.0, id_to_option.1.retention_seconds))
            .collect();

        let ttl_filter = Box::new(TtlCompactionFilter::new(
            id_to_ttl,
            compact_task.current_epoch_time,
        ));
        multi_filter.register(ttl_filter);
    }

    multi_filter
}

const MAX_FILE_COUNT: usize = 32;

fn generate_splits_fast(
    sstable_infos: &Vec<SstableInfo>,
    compaction_size: u64,
    context: CompactorContext,
) -> HummockResult<Vec<KeyRange_vec>> {
    let worker_num = context.compaction_executor.worker_num();
    let parallel_compact_size = (context.storage_opts.parallel_compact_size_mb as u64) << 20;

    let parallelism = (compaction_size + parallel_compact_size - 1) / parallel_compact_size;

    let parallelism = std::cmp::min(
        worker_num,
        std::cmp::min(
            parallelism as usize,
            context.storage_opts.max_sub_compaction as usize,
        ),
    );
    let mut indexes = vec![];
    for sst in sstable_infos {
        let key_range = sst.key_range.as_ref().unwrap();
        indexes.push(
            FullKey {
                user_key: FullKey::decode(&key_range.left).user_key,
                epoch_with_gap: EpochWithGap::new_max_epoch(),
            }
            .encode(),
        );
        indexes.push(
            FullKey {
                user_key: FullKey::decode(&key_range.right).user_key,
                epoch_with_gap: EpochWithGap::new_max_epoch(),
            }
            .encode(),
        );
    }
    indexes.sort_by(|a, b| KeyComparator::compare_encoded_full_key(a.as_ref(), b.as_ref()));
    indexes.dedup();
    if indexes.len() <= parallelism {
        return Ok(vec![]);
    }
    let mut splits = vec![];
    splits.push(KeyRange_vec::new(vec![], vec![]));
    let parallel_key_count = indexes.len() / parallelism;
    let mut last_split_key_count = 0;
    for key in indexes {
        if last_split_key_count >= parallel_key_count {
            splits.last_mut().unwrap().right = key.clone();
            splits.push(KeyRange_vec::new(key.clone(), vec![]));
            last_split_key_count = 0;
        }
        last_split_key_count += 1;
    }
    Ok(splits)
}

pub async fn generate_splits(
    sstable_infos: &Vec<SstableInfo>,
    compaction_size: u64,
    context: CompactorContext,
) -> HummockResult<Vec<KeyRange_vec>> {
    let parallel_compact_size = (context.storage_opts.parallel_compact_size_mb as u64) << 20;
    if compaction_size > parallel_compact_size {
        if sstable_infos.len() > MAX_FILE_COUNT {
            return generate_splits_fast(sstable_infos, compaction_size, context);
        }
        let mut indexes = vec![];
        // preload the meta and get the smallest key to split sub_compaction
        for sstable_info in sstable_infos {
            indexes.extend(
                context
                    .sstable_store
                    .sstable(sstable_info, &mut StoreLocalStatistic::default())
                    .await?
                    .value()
                    .meta
                    .block_metas
                    .iter()
                    .map(|block| {
                        let data_size = block.len;
                        let full_key = FullKey {
                            user_key: FullKey::decode(&block.smallest_key).user_key,
                            epoch_with_gap: EpochWithGap::new_max_epoch(),
                        }
                        .encode();
                        (data_size as u64, full_key)
                    })
                    .collect_vec(),
            );
        }
        // sort by key, as for every data block has the same size;
        indexes.sort_by(|a, b| KeyComparator::compare_encoded_full_key(a.1.as_ref(), b.1.as_ref()));
        let mut splits = vec![];
        splits.push(KeyRange_vec::new(vec![], vec![]));

        let worker_num = context.compaction_executor.worker_num();

        let parallelism = std::cmp::min(
            worker_num as u64,
            std::cmp::min(
                indexes.len() as u64,
                context.storage_opts.max_sub_compaction as u64,
            ),
        );
        let sub_compaction_data_size =
            std::cmp::max(compaction_size / parallelism, parallel_compact_size);
        let parallelism = compaction_size / sub_compaction_data_size;

        if parallelism > 1 {
            let mut last_buffer_size = 0;
            let mut last_key: Vec<u8> = vec![];
            let mut remaining_size = indexes.iter().map(|block| block.0).sum::<u64>();
            for (data_size, key) in indexes {
                if last_buffer_size >= sub_compaction_data_size
                    && !last_key.eq(&key)
                    && remaining_size > parallel_compact_size
                {
                    splits.last_mut().unwrap().right = key.clone();
                    splits.push(KeyRange_vec::new(key.clone(), vec![]));
                    last_buffer_size = data_size;
                } else {
                    last_buffer_size += data_size;
                }
                remaining_size -= data_size;
                last_key = key;
            }
            return Ok(splits);
        }
    }

    Ok(vec![])
}

pub fn estimate_task_output_capacity(context: CompactorContext, task: &CompactTask) -> usize {
    let max_target_file_size = context.storage_opts.sstable_size_mb as usize * (1 << 20);
    let total_input_uncompressed_file_size = task
        .input_ssts
        .iter()
        .flat_map(|level| level.table_infos.iter())
        .map(|table| table.uncompressed_file_size)
        .sum::<u64>();

    let capacity = std::cmp::min(task.target_file_size as usize, max_target_file_size);
    std::cmp::min(capacity, total_input_uncompressed_file_size as usize)
}

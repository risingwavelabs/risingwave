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

use std::collections::{BTreeMap, HashMap};
use std::marker::PhantomData;
use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::constants::hummock::CompactionFilterFlag;
use risingwave_hummock_sdk::compact_task::CompactTask;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::table_stats::TableStatsMap;
use risingwave_hummock_sdk::{EpochWithGap, KeyComparator, can_concat};
use risingwave_pb::hummock::compact_task::PbTaskType;
use risingwave_pb::hummock::{BloomFilterType, PbLevelType, PbTableSchema};
use tokio::time::Instant;

pub use super::context::CompactorContext;
use crate::compaction_catalog_manager::CompactionCatalogAgentRef;
use crate::hummock::compactor::{
    ConcatSstableIterator, MultiCompactionFilter, TaskProgress, TtlCompactionFilter,
};
use crate::hummock::iterator::{
    Forward, HummockIterator, MergeIterator, NonPkPrefixSkipWatermarkIterator,
    NonPkPrefixSkipWatermarkState, PkPrefixSkipWatermarkIterator, PkPrefixSkipWatermarkState,
    UserIterator,
};
use crate::hummock::multi_builder::TableBuilderFactory;
use crate::hummock::sstable::DEFAULT_ENTRY_SIZE;
use crate::hummock::{
    CachePolicy, FilterBuilder, GetObjectId, HummockResult, MemoryLimiter, SstableBuilder,
    SstableBuilderOptions, SstableWriterFactory, SstableWriterOptions,
};
use crate::monitor::StoreLocalStatistic;

pub struct RemoteBuilderFactory<W: SstableWriterFactory, F: FilterBuilder> {
    pub object_id_getter: Arc<dyn GetObjectId>,
    pub limiter: Arc<MemoryLimiter>,
    pub options: SstableBuilderOptions,
    pub policy: CachePolicy,
    pub remote_rpc_cost: Arc<AtomicU64>,
    pub compaction_catalog_agent_ref: CompactionCatalogAgentRef,
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
            self.compaction_catalog_agent_ref.clone(),
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
    pub(crate) key_range: KeyRange,
    pub(crate) cache_policy: CachePolicy,
    pub(crate) gc_delete_keys: bool,
    pub(crate) retain_multiple_version: bool,
    pub(crate) use_block_based_filter: bool,

    pub(crate) table_vnode_partition: BTreeMap<TableId, u32>,
    /// `TableId` -> `TableSchema`
    /// Schemas in `table_schemas` are at least as new as the one used to create `input_ssts`.
    /// For a table with schema existing in `table_schemas`, its columns not in `table_schemas` but in `input_ssts` can be safely dropped.
    pub(crate) table_schemas: HashMap<TableId, PbTableSchema>,
    /// `disable_drop_column_optimization` should only be set in benchmark.
    pub(crate) disable_drop_column_optimization: bool,
}

impl TaskConfig {
    #[cfg(any(test, feature = "test"))]
    pub fn for_test(
        key_range: KeyRange,
        cache_policy: CachePolicy,
        gc_delete_keys: bool,
        use_block_based_filter: bool,
        table_schemas: HashMap<TableId, PbTableSchema>,
    ) -> Self {
        Self {
            key_range,
            cache_policy,
            gc_delete_keys,
            retain_multiple_version: false,
            use_block_based_filter,
            table_vnode_partition: BTreeMap::default(),
            table_schemas,
            disable_drop_column_optimization: false,
        }
    }

    #[cfg(any(test, feature = "test"))]
    pub fn with_disable_drop_column_optimization(mut self, disable: bool) -> Self {
        self.disable_drop_column_optimization = disable;
        self
    }
}

pub fn build_multi_compaction_filter(compact_task: &CompactTask) -> MultiCompactionFilter {
    let mut multi_filter = MultiCompactionFilter::default();
    let compaction_filter_flag =
        CompactionFilterFlag::from_bits(compact_task.compaction_filter_mask).unwrap_or_default();
    // STATE_CLEAN is handled by compact-task table-id normalization and iterator-level block
    // pruning. Keep the flag as a no-op here for compatibility with existing task configs.

    if compaction_filter_flag.contains(CompactionFilterFlag::TTL) {
        let id_to_ttl = compact_task
            .table_options
            .iter()
            .filter_map(|(id, option)| {
                option
                    .retention_seconds
                    .and_then(|ttl| if ttl > 0 { Some((*id, ttl)) } else { None })
            })
            .collect();

        let ttl_filter = Box::new(TtlCompactionFilter::new(
            id_to_ttl,
            compact_task.current_epoch_time,
        ));
        multi_filter.register(ttl_filter);
    }

    multi_filter
}

fn generate_splits_fast(
    sstable_infos: &[&SstableInfo],
    compaction_size: u64,
    context: &CompactorContext,
    max_sub_compaction: u32,
) -> Vec<KeyRange> {
    let worker_num = context.compaction_executor.worker_num();
    let parallel_compact_size = (context.storage_opts.parallel_compact_size_mb as u64) << 20;

    let parallelism = calculate_task_parallelism_impl(
        worker_num,
        parallel_compact_size,
        compaction_size,
        max_sub_compaction,
    );
    let mut indexes = vec![];
    for sst in sstable_infos {
        let key_range = &sst.key_range;
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
        return vec![];
    }

    let mut splits = vec![];
    splits.push(KeyRange::default());
    let parallel_key_count = indexes.len() / parallelism;
    let mut last_split_key_count = 0;
    for key in indexes {
        if last_split_key_count >= parallel_key_count {
            splits.last_mut().unwrap().right = Bytes::from(key.clone());
            splits.push(KeyRange::new(Bytes::from(key.clone()), Bytes::default()));
            last_split_key_count = 0;
        }
        last_split_key_count += 1;
    }

    splits
}

pub async fn generate_splits(
    sstable_infos: &[&SstableInfo],
    compaction_size: u64,
    context: &CompactorContext,
    max_sub_compaction: u32,
) -> HummockResult<Vec<KeyRange>> {
    let parallel_compact_size = (context.storage_opts.parallel_compact_size_mb as u64) << 20;
    if compaction_size > parallel_compact_size {
        if sstable_infos.len() > context.storage_opts.compactor_max_preload_meta_file_count {
            return Ok(generate_splits_fast(
                sstable_infos,
                compaction_size,
                context,
                max_sub_compaction,
            ));
        }
        let mut indexes = vec![];
        // preload the meta and get the smallest key to split sub_compaction
        for sstable_info in sstable_infos {
            let sstable = context
                .sstable_store
                .sstable(sstable_info, &mut StoreLocalStatistic::default())
                .await?;
            indexes.extend(sstable.meta.block_metas.iter().map(|block| {
                let data_size = block.len;
                let full_key = FullKey {
                    user_key: FullKey::decode(&block.smallest_key).user_key,
                    epoch_with_gap: EpochWithGap::new_max_epoch(),
                }
                .encode();
                (data_size as u64, full_key)
            }));
        }
        // sort by key, as for every data block has the same size;
        indexes.sort_by(|a, b| KeyComparator::compare_encoded_full_key(a.1.as_ref(), b.1.as_ref()));
        let mut splits = vec![];
        splits.push(KeyRange::default());

        let parallelism = calculate_task_parallelism_impl(
            context.compaction_executor.worker_num(),
            parallel_compact_size,
            compaction_size,
            max_sub_compaction,
        );

        let sub_compaction_data_size =
            std::cmp::max(compaction_size / parallelism as u64, parallel_compact_size);

        if parallelism > 1 {
            let mut last_buffer_size = 0;
            let mut last_key: Vec<u8> = vec![];
            let mut remaining_size = indexes.iter().map(|block| block.0).sum::<u64>();
            for (data_size, key) in indexes {
                if last_buffer_size >= sub_compaction_data_size
                    && !last_key.eq(&key)
                    && remaining_size > parallel_compact_size
                {
                    splits.last_mut().unwrap().right = Bytes::from(key.clone());
                    splits.push(KeyRange::new(Bytes::from(key.clone()), Bytes::default()));
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
        .read_input_ssts()
        .map(|table| table.uncompressed_file_size)
        .sum::<u64>();

    let capacity = std::cmp::min(task.target_file_size as usize, max_target_file_size);
    std::cmp::min(capacity, total_input_uncompressed_file_size as usize)
}

/// Compare result of compaction task and input. The data saw by user shall not change after applying compaction result.
pub async fn check_compaction_result(
    compact_task: &CompactTask,
    context: CompactorContext,
    compaction_catalog_agent_ref: CompactionCatalogAgentRef,
) -> HummockResult<bool> {
    // This check method does not consider dropped keys by compaction filter.
    if compact_task.contains_ttl() {
        return Ok(true);
    }

    let mut table_iters = Vec::new();

    for level in &compact_task.input_ssts {
        if level.level_type == PbLevelType::Nonoverlapping {
            let tables = level.read_sstable_infos().cloned().collect_vec();
            if tables.is_empty() {
                continue;
            }
            debug_assert!(can_concat(&tables));

            table_iters.push(ConcatSstableIterator::new(
                tables,
                KeyRange::inf(),
                context.sstable_store.clone(),
                Arc::new(TaskProgress::default()),
                context.storage_opts.compactor_iter_max_io_retry_times,
            ));
        } else {
            for table_info in level.read_sstable_infos().cloned() {
                table_iters.push(ConcatSstableIterator::new(
                    vec![table_info],
                    KeyRange::inf(),
                    context.sstable_store.clone(),
                    Arc::new(TaskProgress::default()),
                    context.storage_opts.compactor_iter_max_io_retry_times,
                ));
            }
        }
    }

    let iter = MergeIterator::for_compactor(table_iters);
    let left_iter = {
        let skip_watermark_iter = PkPrefixSkipWatermarkIterator::new(
            iter,
            PkPrefixSkipWatermarkState::from_safe_epoch_watermarks(
                compact_task.pk_prefix_table_watermarks.clone(),
            ),
        );

        let combine_iter = NonPkPrefixSkipWatermarkIterator::new(
            skip_watermark_iter,
            NonPkPrefixSkipWatermarkState::from_safe_epoch_watermarks(
                compact_task.non_pk_prefix_table_watermarks.clone(),
                compaction_catalog_agent_ref.clone(),
            ),
        );

        UserIterator::new(
            combine_iter,
            (Bound::Unbounded, Bound::Unbounded),
            u64::MAX,
            0,
            None,
        )
    };
    let iter = ConcatSstableIterator::new(
        compact_task.sorted_output_ssts.clone(),
        KeyRange::inf(),
        context.sstable_store.clone(),
        Arc::new(TaskProgress::default()),
        context.storage_opts.compactor_iter_max_io_retry_times,
    );
    let right_iter = {
        let skip_watermark_iter = PkPrefixSkipWatermarkIterator::new(
            iter,
            PkPrefixSkipWatermarkState::from_safe_epoch_watermarks(
                compact_task.pk_prefix_table_watermarks.clone(),
            ),
        );

        let combine_iter = NonPkPrefixSkipWatermarkIterator::new(
            skip_watermark_iter,
            NonPkPrefixSkipWatermarkState::from_safe_epoch_watermarks(
                compact_task.non_pk_prefix_table_watermarks.clone(),
                compaction_catalog_agent_ref,
            ),
        );

        UserIterator::new(
            combine_iter,
            (Bound::Unbounded, Bound::Unbounded),
            u64::MAX,
            0,
            None,
        )
    };

    check_result(left_iter, right_iter).await
}

pub async fn check_flush_result<I: HummockIterator<Direction = Forward>>(
    left_iter: UserIterator<I>,
    sort_ssts: Vec<SstableInfo>,
    context: CompactorContext,
) -> HummockResult<bool> {
    let iter = ConcatSstableIterator::new(
        sort_ssts,
        KeyRange::inf(),
        context.sstable_store.clone(),
        Arc::new(TaskProgress::default()),
        0,
    );
    let right_iter = UserIterator::new(
        iter,
        (Bound::Unbounded, Bound::Unbounded),
        u64::MAX,
        0,
        None,
    );
    check_result(left_iter, right_iter).await
}

async fn check_result<
    I1: HummockIterator<Direction = Forward>,
    I2: HummockIterator<Direction = Forward>,
>(
    mut left_iter: UserIterator<I1>,
    mut right_iter: UserIterator<I2>,
) -> HummockResult<bool> {
    left_iter.rewind().await?;
    right_iter.rewind().await?;
    let mut right_count = 0;
    let mut left_count = 0;
    while left_iter.is_valid() && right_iter.is_valid() {
        if left_iter.key() != right_iter.key() {
            tracing::error!(
                "The key of input and output not equal. key: {:?} vs {:?}",
                left_iter.key(),
                right_iter.key()
            );
            return Ok(false);
        }
        if left_iter.value() != right_iter.value() {
            tracing::error!(
                "The value of input and output not equal. key: {:?}, value: {:?} vs {:?}",
                left_iter.key(),
                left_iter.value(),
                right_iter.value()
            );
            return Ok(false);
        }
        left_iter.next().await?;
        right_iter.next().await?;
        left_count += 1;
        right_count += 1;
    }
    while left_iter.is_valid() {
        left_count += 1;
        left_iter.next().await?;
    }
    while right_iter.is_valid() {
        right_count += 1;
        right_iter.next().await?;
    }
    if left_count != right_count {
        tracing::error!(
            "The key count of input and output not equal: {} vs {}",
            left_count,
            right_count
        );
        return Ok(false);
    }
    Ok(true)
}

pub fn optimize_by_copy_block(compact_task: &CompactTask, context: &CompactorContext) -> bool {
    let input_ssts = compact_task.read_input_ssts().collect_vec();
    let compaction_size = input_ssts_size(&input_ssts);
    optimize_by_copy_block_with_input(compact_task, context, &input_ssts, compaction_size)
}

fn optimize_by_copy_block_with_input(
    compact_task: &CompactTask,
    context: &CompactorContext,
    input_ssts: &[&SstableInfo],
    compaction_size: u64,
) -> bool {
    let all_ssts_are_blocked_filter = input_ssts
        .iter()
        .all(|table_info| table_info.bloom_filter_kind == BloomFilterType::Blocked);

    let delete_key_count = input_ssts
        .iter()
        .map(|table_info| table_info.stale_key_count + table_info.range_tombstone_count)
        .sum::<u64>();
    let total_key_count = input_ssts
        .iter()
        .map(|table_info| table_info.total_key_count)
        .sum::<u64>();

    let single_table = compact_task
        .get_table_ids_from_input_ssts()
        .exactly_one()
        .is_ok();
    context.storage_opts.enable_fast_compaction
        && all_ssts_are_blocked_filter
        && !compact_task.contains_range_tombstone()
        && !compact_task.contains_ttl()
        && !compact_task.contains_split_sst()
        && single_table
        && compact_task.target_level > 0
        && compact_task.input_ssts.len() == 2
        && compaction_size < context.storage_opts.compactor_fast_max_compact_task_size
        && delete_key_count * 100
            < context.storage_opts.compactor_fast_max_compact_delete_ratio as u64 * total_key_count
        && compact_task.task_type == PbTaskType::Dynamic
}

pub async fn generate_splits_for_task(
    compact_task: &mut CompactTask,
    context: &CompactorContext,
    optimize_by_copy_block: bool,
) -> HummockResult<()> {
    let input_ssts = compact_task.read_input_ssts().collect_vec();
    let compaction_size = input_ssts_size(&input_ssts);

    if !optimize_by_copy_block {
        let splits = generate_splits(
            &input_ssts,
            compaction_size,
            context,
            compact_task.max_sub_compaction,
        )
        .await?;
        if !splits.is_empty() {
            compact_task.splits = splits;
        }
        return Ok(());
    }

    Ok(())
}

pub fn metrics_report_for_task(compact_task: &CompactTask, context: &CompactorContext) {
    let group_label = compact_task.compaction_group_id.to_string();
    let cur_level_label = compact_task.input_ssts[0].level_idx.to_string();

    let (select_size, select_count) = read_sstable_size_and_count(
        compact_task
            .input_ssts
            .iter()
            .filter(|level| level.level_idx != compact_task.target_level)
            .flat_map(|level| level.read_sstable_infos()),
    );
    let (target_level_read_bytes, target_count) = read_sstable_size_and_count(
        compact_task
            .input_ssts
            .iter()
            .filter(|level| level.level_idx == compact_task.target_level)
            .flat_map(|level| level.read_sstable_infos()),
    );

    context
        .compactor_metrics
        .compact_read_current_level
        .with_label_values(&[&group_label, &cur_level_label])
        .inc_by(select_size);
    context
        .compactor_metrics
        .compact_read_sstn_current_level
        .with_label_values(&[&group_label, &cur_level_label])
        .inc_by(select_count as u64);

    let next_level_label = compact_task.target_level.to_string();
    context
        .compactor_metrics
        .compact_read_next_level
        .with_label_values(&[&group_label, &next_level_label])
        .inc_by(target_level_read_bytes);
    context
        .compactor_metrics
        .compact_read_sstn_next_level
        .with_label_values(&[&group_label, &next_level_label])
        .inc_by(target_count as u64);
}

fn read_sstable_size_and_count<'a>(
    sstable_infos: impl IntoIterator<Item = &'a SstableInfo>,
) -> (u64, usize) {
    sstable_infos
        .into_iter()
        .fold((0, 0), |(size, count), table_info| {
            (size + table_info.sst_size, count + 1)
        })
}

pub fn calculate_task_parallelism(compact_task: &CompactTask, context: &CompactorContext) -> usize {
    let input_ssts = compact_task.read_input_ssts().collect_vec();
    let compaction_size = input_ssts_size(&input_ssts);
    let optimize_by_copy_block =
        optimize_by_copy_block_with_input(compact_task, context, &input_ssts, compaction_size);

    if optimize_by_copy_block {
        return 1;
    }

    let parallel_compact_size = (context.storage_opts.parallel_compact_size_mb as u64) << 20;
    calculate_task_parallelism_impl(
        context.compaction_executor.worker_num(),
        parallel_compact_size,
        compaction_size,
        compact_task.max_sub_compaction,
    )
}

fn input_ssts_size(input_ssts: &[&SstableInfo]) -> u64 {
    input_ssts
        .iter()
        .map(|table_info| table_info.sst_size)
        .sum()
}

pub fn calculate_task_parallelism_impl(
    worker_num: usize,
    parallel_compact_size: u64,
    compaction_size: u64,
    max_sub_compaction: u32,
) -> usize {
    let parallelism = compaction_size.div_ceil(parallel_compact_size);
    worker_num.min(parallelism.min(max_sub_compaction as u64) as usize)
}

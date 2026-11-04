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

use await_tree::{InstrumentAwait, SpanExt};
use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::config::meta::default::compaction_config;
use risingwave_common::constants::hummock::CompactionFilterFlag;
use risingwave_hummock_sdk::compact_task::CompactTask;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::table_stats::{TableStats, TableStatsMap};
use risingwave_hummock_sdk::{EpochWithGap, KeyComparator, can_concat};
use risingwave_pb::hummock::compact_task::PbTaskType;
use risingwave_pb::hummock::{
    PbLevelType, PbSstableFilterLayout, PbSstableFilterType, PbTableSchema,
};
use tokio::time::Instant;

pub use super::context::CompactorContext;
use crate::compaction_catalog_manager::CompactionCatalogAgentRef;
use crate::hummock::compactor::{
    CompactionFilter, ConcatSstableIterator, MultiCompactionFilter, TaskProgress,
    TtlCompactionFilter,
};
use crate::hummock::iterator::{
    Forward, HummockIterator, MergeIterator, NonPkPrefixSkipWatermarkIterator,
    NonPkPrefixSkipWatermarkState, PkPrefixSkipWatermarkIterator, PkPrefixSkipWatermarkState,
    UserIterator,
};
use crate::hummock::multi_builder::{CapacitySplitTableBuilder, TableBuilderFactory};
use crate::hummock::value::HummockValue;
use crate::hummock::{
    CachePolicy, FilterBuilder, GetObjectId, HummockResult, MemoryLimiter, SstableBuilder,
    SstableBuilderOptions, SstableWriterFactory, SstableWriterOptions,
    StreamingSstableWriterFactory,
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

pub(super) type StreamingTableBuilder<B> =
    CapacitySplitTableBuilder<RemoteBuilderFactory<StreamingSstableWriterFactory, B>>;

pub(super) fn new_output_builder<B: FilterBuilder>(
    context: &CompactorContext,
    object_id_getter: Arc<dyn GetObjectId>,
    options: SstableBuilderOptions,
    policy: CachePolicy,
    remote_rpc_cost: Arc<AtomicU64>,
    catalog: CompactionCatalogAgentRef,
    task_progress: Option<Arc<TaskProgress>>,
    table_vnode_partition: BTreeMap<TableId, u32>,
) -> StreamingTableBuilder<B> {
    CapacitySplitTableBuilder::new(
        RemoteBuilderFactory {
            object_id_getter,
            limiter: context.memory_limiter.clone(),
            options,
            policy,
            remote_rpc_cost,
            compaction_catalog_agent_ref: catalog.clone(),
            sstable_writer_factory: StreamingSstableWriterFactory::new(
                context.sstable_store.clone(),
            ),
            _phantom: PhantomData,
        },
        context.compactor_metrics.clone(),
        task_progress,
        table_vnode_partition,
        context
            .storage_opts
            .compactor_concurrent_uploading_sst_count,
        catalog,
    )
}

#[async_trait::async_trait]
impl<W: SstableWriterFactory, F: FilterBuilder> TableBuilderFactory for RemoteBuilderFactory<W, F> {
    type Filter = F;
    type Writer = W::Writer;

    async fn open_builder(&mut self) -> HummockResult<SstableBuilder<Self::Writer, Self::Filter>> {
        let timer = Instant::now();
        let table_id = self
            .object_id_getter
            .get_new_sst_object_id()
            .instrument_await("remote_builder_get_new_sst_object_id".verbose())
            .await?;
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
            .instrument_await("remote_builder_create_sst_writer".verbose())
            .await?;
        let builder = SstableBuilder::new(
            table_id,
            writer,
            Self::Filter::create(self.options.filter_builder_options()),
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

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum RowAction {
    Keep,
    Drop,
    DropTombstone,
}

/// Policy and accounting for decoded rows. Callers own key tracking, watermark ordering
/// and the decision to copy a physical block.
pub(super) struct CompactionRows<C> {
    pub(super) filter: C,
    gc_delete_keys: bool,
    retain_multiple_version: bool,
    pub(super) statistics: CompactionStatistics,
    last_table_id: Option<TableId>,
    pub(super) table_stats: TableStats,
}

impl<C: CompactionFilter> CompactionRows<C> {
    pub(super) fn new(filter: C, gc_delete_keys: bool, retain_multiple_version: bool) -> Self {
        Self {
            filter,
            gc_delete_keys,
            retain_multiple_version,
            statistics: CompactionStatistics::default(),
            last_table_id: None,
            table_stats: TableStats::default(),
        }
    }

    #[inline]
    pub(super) fn action(
        &mut self,
        key: FullKey<&[u8]>,
        value: HummockValue<&[u8]>,
        first_version: bool,
    ) -> RowAction {
        // Memtable spill can give a PUT and DELETE the same pure epoch.
        // The caller identifies the first version using the full key order.
        if !self.retain_multiple_version {
            if self.gc_delete_keys && value.is_delete() {
                return RowAction::DropTombstone;
            }
            if !first_version {
                return RowAction::Drop;
            }
        }
        if self.filter.should_delete(key) {
            RowAction::Drop
        } else {
            RowAction::Keep
        }
    }

    #[inline]
    pub(super) fn record(
        &mut self,
        key: FullKey<&[u8]>,
        value: HummockValue<&[u8]>,
        dropped: bool,
    ) {
        let table_id = key.user_key.table_id;
        if self.last_table_id != Some(table_id) {
            self.flush_table_stats();
            self.last_table_id = Some(table_id);
        }
        if dropped {
            self.statistics.iter_drop_key_counts += 1;
            self.table_stats.total_key_count -= 1;
            self.table_stats.total_key_size -= key.encoded_len() as i64;
            self.table_stats.total_value_size -= value.encoded_len() as i64;
        }
    }

    fn flush_table_stats(&mut self) {
        if let Some(table_id) = self.last_table_id.take() {
            self.statistics
                .delta_drop_stat
                .insert(table_id, std::mem::take(&mut self.table_stats));
        }
    }

    pub(super) fn finish(mut self) -> CompactionStatistics {
        self.flush_table_stats();
        self.statistics
    }
}

#[derive(Clone, Default)]
pub struct TaskConfig {
    pub(crate) key_range: KeyRange,
    pub(crate) cache_policy: CachePolicy,
    pub(crate) gc_delete_keys: bool,
    pub(crate) retain_multiple_version: bool,
    /// Resolved output SST filter layout from `CompactTask::sstable_filter_layout_for_output`.
    pub(crate) sstable_filter_layout: PbSstableFilterLayout,
    pub(crate) sstable_filter_type: PbSstableFilterType,

    pub(crate) table_vnode_partition: BTreeMap<TableId, u32>,
    /// `TableId` -> `TableSchema`
    /// Schemas in `table_schemas` are at least as new as the one used to create `input_ssts`.
    /// For a table with schema existing in `table_schemas`, its columns not in `table_schemas` but in `input_ssts` can be safely dropped.
    pub(crate) table_schemas: HashMap<TableId, PbTableSchema>,
    /// `disable_drop_column_optimization` should only be set in benchmark.
    pub(crate) disable_drop_column_optimization: bool,
}

impl TaskConfig {
    /// Construct a compact task config for tests and benchmarks.
    pub fn for_test(
        key_range: KeyRange,
        cache_policy: CachePolicy,
        gc_delete_keys: bool,
        sstable_filter_layout: PbSstableFilterLayout,
        table_schemas: HashMap<TableId, PbTableSchema>,
    ) -> Self {
        Self {
            key_range,
            cache_policy,
            gc_delete_keys,
            retain_multiple_version: false,
            sstable_filter_layout,
            sstable_filter_type: PbSstableFilterType::SstableFilterXor16,
            table_vnode_partition: BTreeMap::default(),
            table_schemas,
            disable_drop_column_optimization: false,
        }
    }

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

fn generate_splits_fast(sstable_infos: &[&SstableInfo], parallelism: usize) -> Vec<KeyRange> {
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

    let parallel_key_count = indexes.len() / parallelism;
    key_ranges(
        indexes.into_iter().step_by(parallel_key_count).skip(1),
        parallelism,
    )
}

fn key_ranges(boundaries: impl Iterator<Item = Vec<u8>>, parallelism: usize) -> Vec<KeyRange> {
    let mut splits = vec![KeyRange::inf()];
    for key in boundaries.take(parallelism - 1).map(Bytes::from) {
        splits.last_mut().unwrap().right = key.clone();
        splits.push(KeyRange::new(key, Bytes::default()));
    }
    splits
}

async fn generate_splits(
    sstable_infos: &[&SstableInfo],
    compaction_size: u64,
    context: &CompactorContext,
    parallelism: usize,
) -> HummockResult<Vec<KeyRange>> {
    let parallel_compact_size = (context.storage_opts.parallel_compact_size_mb as u64) << 20;
    if parallelism > 1 && compaction_size > parallel_compact_size {
        if sstable_infos.len() > context.storage_opts.compactor_max_preload_meta_file_count {
            return Ok(generate_splits_fast(sstable_infos, parallelism));
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
        let sub_compaction_data_size =
            std::cmp::max(compaction_size / parallelism as u64, parallel_compact_size);

        let mut buffered_size = 0;
        let mut last_key = &[][..];
        let mut remaining_size = indexes.iter().map(|block| block.0).sum::<u64>();
        let boundaries = indexes.iter().filter_map(|(data_size, key)| {
            let split = buffered_size >= sub_compaction_data_size
                && last_key != key.as_slice()
                && remaining_size > parallel_compact_size;
            if split {
                buffered_size = 0;
            }
            buffered_size += data_size;
            remaining_size -= data_size;
            last_key = key;
            split.then(|| key.clone())
        });
        return Ok(key_ranges(boundaries, parallelism));
    }

    Ok(vec![])
}

fn estimate_task_output_capacity(context: &CompactorContext, task: &CompactTask) -> usize {
    let max_target_file_size = context.storage_opts.sstable_size_mb as usize * (1 << 20);
    let total_input_uncompressed_file_size = task
        .read_input_ssts()
        .map(|table| table.uncompressed_file_size)
        .sum::<u64>();

    let capacity = std::cmp::min(task.target_file_size as usize, max_target_file_size);
    std::cmp::min(capacity, total_input_uncompressed_file_size as usize)
}

pub fn estimate_output_key_count_by_size(
    total_key_count: u64,
    total_size: u64,
    output_capacity: usize,
) -> usize {
    if total_key_count == 0 {
        return 0;
    }
    if total_size == 0 {
        return total_key_count.try_into().unwrap_or(usize::MAX);
    }
    if output_capacity == 0 {
        return 0;
    }

    // Estimate how many keys one target-sized output SST will contain by using the input key
    // density. This replaces the old task-level key-count heuristic for filter layout decisions:
    // large tasks can now still produce plain-filter output SSTs when each output is small.
    let estimated = (total_key_count as u128 * output_capacity as u128)
        .div_ceil(total_size as u128)
        .min(total_key_count as u128);
    estimated.try_into().unwrap_or(usize::MAX)
}

fn estimate_output_key_count_for_input_ssts<'a>(
    input_ssts: impl Iterator<Item = &'a SstableInfo>,
    output_capacity: usize,
) -> usize {
    let (total_key_count, total_uncompressed_size) =
        input_ssts.fold((0u64, 0u64), |(key_count, size), sst| {
            (
                key_count + sst.total_key_count,
                size + sst.uncompressed_file_size,
            )
        });

    estimate_output_key_count_by_size(total_key_count, total_uncompressed_size, output_capacity)
}

pub(super) fn normal_output_options(
    task: &CompactTask,
    context: &CompactorContext,
) -> (SstableBuilderOptions, PbSstableFilterLayout) {
    let mut options = output_options(task, context, estimate_task_output_capacity(context, task));
    options.max_vnode_key_range_bytes = task.effective_max_vnode_key_range_bytes();
    let layout = task.sstable_filter_layout_for_output(options.estimated_output_key_count() as u64);
    (options, layout)
}

pub(super) fn fast_output_options(
    task: &CompactTask,
    context: &CompactorContext,
) -> SstableBuilderOptions {
    let mut options = output_options(task, context, task.target_file_size as usize);
    options.max_vnode_key_range_bytes = None;
    options
}

fn output_options(
    task: &CompactTask,
    context: &CompactorContext,
    capacity: usize,
) -> SstableBuilderOptions {
    let mut options = SstableBuilderOptions::from(context.storage_opts.as_ref());
    options.compression_algorithm = task.compression_algorithm.into();
    options.capacity = capacity;
    options.estimated_output_key_count = Some(estimate_output_key_count_for_input_ssts(
        task.read_input_ssts(),
        capacity,
    ));
    options.filter_hash_prealloc_key_count_cap =
        blocked_xor_filter_key_count_threshold(task.blocked_xor_filter_kv_count_threshold);
    options
}

pub fn blocked_xor_filter_key_count_threshold(
    blocked_xor_filter_kv_count_threshold: Option<u64>,
) -> usize {
    blocked_xor_filter_kv_count_threshold
        .unwrap_or(compaction_config::DEFAULT_BLOCKED_XOR_FILTER_KV_COUNT_THRESHOLD)
        .try_into()
        .unwrap_or(usize::MAX)
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

/// A task's execution choices, computed from its descriptors before any SST reads.
pub(super) struct CompactionPlan {
    pub(super) copy_blocks: bool,
    pub(super) parallelism: usize,
    pub(super) output_options: SstableBuilderOptions,
    pub(super) output_filter_layout: PbSstableFilterLayout,
    /// Memory admission keeps the ordinary output capacity estimate for both runners.
    pub(super) memory_sst_capacity: usize,
    input_size: u64,
}

impl CompactionPlan {
    pub(super) fn new(task: &CompactTask, context: &CompactorContext) -> Self {
        let input_ssts = task.read_input_ssts().collect_vec();
        let input_size = input_ssts.iter().map(|sst| sst.sst_size).sum();
        let (normal_options, output_filter_layout) = normal_output_options(task, context);
        let memory_sst_capacity = normal_options.capacity;
        let copy_blocks =
            optimize_by_copy_block(task, context, &input_ssts, input_size, output_filter_layout);
        let (parallelism, output_options) = if copy_blocks {
            (1, fast_output_options(task, context))
        } else {
            let parallelism = input_size
                .div_ceil(u64::from(context.storage_opts.parallel_compact_size_mb) << 20)
                .min(u64::from(task.max_sub_compaction)) as usize;
            (
                parallelism
                    .min(context.compaction_executor.worker_num())
                    .max(1),
                normal_options,
            )
        };
        Self {
            copy_blocks,
            parallelism,
            output_options,
            output_filter_layout,
            memory_sst_capacity,
            input_size,
        }
    }
}

fn optimize_by_copy_block(
    compact_task: &CompactTask,
    context: &CompactorContext,
    input_ssts: &[&SstableInfo],
    compaction_size: u64,
    output_filter_layout: PbSstableFilterLayout,
) -> bool {
    let all_ssts_are_blocked_filter = input_ssts
        .iter()
        .all(|table_info| table_info.filter_layout == PbSstableFilterLayout::Blocked);
    let current_filter_type = compact_task.sstable_filter_type;
    let all_ssts_match_filter_type = input_ssts
        .iter()
        .all(|table_info| table_info.filter_type == current_filter_type);
    // Fast compaction path can only preserve blocked filters by copying block payloads (and their
    // per-block filter bytes). If the output-SST-level heuristic now wants a plain filter, fall back
    // to the normal compaction path to rebuild filters. This intentionally lets tasks that were
    // previously classified as blocked by total task key count move back to plain output filters.
    let delete_key_count = input_ssts
        .iter()
        .map(|table_info| table_info.stale_key_count + table_info.range_tombstone_count)
        .sum::<u64>();
    let total_key_count = input_ssts
        .iter()
        .map(|table_info| table_info.total_key_count)
        .sum::<u64>();

    let single_table = compact_task.get_table_ids_from_input_ssts().count() == 1;
    context.storage_opts.enable_fast_compaction
        && matches!(
            current_filter_type,
            PbSstableFilterType::SstableFilterXor8 | PbSstableFilterType::SstableFilterXor16
        )
        && all_ssts_are_blocked_filter
        && all_ssts_match_filter_type
        && output_filter_layout == PbSstableFilterLayout::Blocked
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

pub(super) async fn generate_splits_for_task(
    compact_task: &mut CompactTask,
    context: &CompactorContext,
    plan: &CompactionPlan,
) -> HummockResult<()> {
    if plan.parallelism > 1 {
        let input_ssts = compact_task.read_input_ssts().collect_vec();
        let splits =
            generate_splits(&input_ssts, plan.input_size, context, plan.parallelism).await?;
        if !splits.is_empty() {
            compact_task.splits = splits;
        }
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_common::catalog::TableId;
    use risingwave_hummock_sdk::level::InputLevel;
    use risingwave_hummock_sdk::sstable_info::{SstableInfo, SstableInfoInner};
    use risingwave_pb::hummock::compact_task::PbTaskType;
    use risingwave_pb::hummock::{PbLevelType, PbSstableFilterLayout, PbSstableFilterType};

    use super::{
        CompactTask, CompactionPlan, CompactorContext, KeyRange, estimate_output_key_count_by_size,
        generate_splits_fast,
    };
    use crate::hummock::compactor::new_compaction_await_tree_reg_ref;
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::test_utils::test_key_of;
    use crate::monitor::CompactorMetrics;
    use crate::opts::StorageOpts;

    fn test_sstable(
        table_id: TableId,
        total_key_count: u64,
        filter_type: PbSstableFilterType,
    ) -> risingwave_hummock_sdk::sstable_info::SstableInfo {
        SstableInfoInner {
            object_id: 1.into(),
            sst_id: 1.into(),
            table_ids: vec![table_id],
            total_key_count,
            sst_size: 1024,
            uncompressed_file_size: 1024,
            filter_type,
            filter_layout: PbSstableFilterLayout::Blocked,
            ..Default::default()
        }
        .into()
    }

    async fn test_context() -> CompactorContext {
        CompactorContext::new_local_compact_context(
            Arc::new(StorageOpts::default()),
            mock_sstable_store().await,
            Arc::new(CompactorMetrics::unused()),
            Some(new_compaction_await_tree_reg_ref(
                await_tree::Config::default(),
            )),
        )
    }

    fn test_compact_task(
        layout: PbSstableFilterLayout,
        blocked_xor_filter_kv_count_threshold: Option<u64>,
        filter_type: PbSstableFilterType,
    ) -> CompactTask {
        let table_id = TableId::new(1);
        CompactTask {
            input_ssts: vec![
                InputLevel {
                    level_idx: 1,
                    level_type: PbLevelType::Nonoverlapping,
                    table_infos: vec![test_sstable(table_id, 10, filter_type)],
                },
                InputLevel {
                    level_idx: 2,
                    level_type: PbLevelType::Nonoverlapping,
                    table_infos: vec![test_sstable(table_id, 10, filter_type)],
                },
            ],
            existing_table_ids: vec![table_id],
            target_level: 2,
            target_file_size: 1024,
            task_type: PbTaskType::Dynamic,
            sstable_filter_type: filter_type,
            sstable_filter_layout: layout,
            blocked_xor_filter_kv_count_threshold,
            ..Default::default()
        }
    }

    #[test]
    fn test_estimate_output_key_count_by_size_scales_to_output_sst() {
        let estimated_key_count =
            estimate_output_key_count_by_size(1024 * 1024, 512 * 1024 * 1024, 128 * 1024 * 1024);

        assert_eq!(estimated_key_count, 256 * 1024);
        assert_eq!(estimate_output_key_count_by_size(100, 0, 0), 100);
    }

    #[test]
    fn test_sst_boundary_splits_respect_reserved_parallelism() {
        let ssts: Vec<SstableInfo> = (0..33)
            .map(|index| {
                SstableInfoInner {
                    key_range: KeyRange::new(
                        test_key_of(index * 2).encode().into(),
                        test_key_of(index * 2 + 1).encode().into(),
                    ),
                    ..Default::default()
                }
                .into()
            })
            .collect();
        let ssts: Vec<_> = ssts.iter().collect();
        let splits = generate_splits_fast(&ssts, 4);
        assert_eq!(splits.len(), 4);
        assert!(splits.first().unwrap().left.is_empty());
        assert!(splits.last().unwrap().right.is_empty());
        assert!(splits.windows(2).all(|pair| pair[0].right == pair[1].left));
    }

    #[tokio::test]
    async fn test_optimize_by_copy_block_respects_layout_policy() {
        let context = test_context().await;
        let compact_task = test_compact_task(
            PbSstableFilterLayout::Plain,
            Some(1),
            PbSstableFilterType::SstableFilterXor16,
        );

        assert!(!CompactionPlan::new(&compact_task, &context).copy_blocks);

        let compact_task = test_compact_task(
            PbSstableFilterLayout::Auto,
            Some(1024),
            PbSstableFilterType::SstableFilterXor16,
        );

        assert!(!CompactionPlan::new(&compact_task, &context).copy_blocks);
    }

    #[tokio::test]
    async fn test_optimize_by_copy_block_supports_blocked_xor_filters() {
        let context = test_context().await;
        let compact_task = test_compact_task(
            PbSstableFilterLayout::Blocked,
            Some(1024),
            PbSstableFilterType::SstableFilterXor16,
        );

        assert!(CompactionPlan::new(&compact_task, &context).copy_blocks);

        let compact_task = test_compact_task(
            PbSstableFilterLayout::Blocked,
            Some(1024),
            PbSstableFilterType::SstableFilterXor8,
        );

        assert!(CompactionPlan::new(&compact_task, &context).copy_blocks);
    }
}

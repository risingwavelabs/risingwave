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
use std::sync::Arc;

use await_tree::InstrumentAwait;
use bytes::Bytes;
use futures::{stream, FutureExt, StreamExt};
use itertools::Itertools;
use risingwave_hummock_sdk::compact::{
    compact_task_to_string, estimate_memory_for_compact_task, statistics_compact_task,
};
use risingwave_hummock_sdk::key::{FullKey, PointRange};
use risingwave_hummock_sdk::key_range::{KeyRange, KeyRangeCommon};
use risingwave_hummock_sdk::table_stats::{add_table_stats_map, TableStats, TableStatsMap};
use risingwave_hummock_sdk::{can_concat, HummockEpoch};
use risingwave_pb::hummock::compact_task::{TaskStatus, TaskType};
use risingwave_pb::hummock::{BloomFilterType, CompactTask, LevelType, SstableInfo};
use tokio::sync::oneshot::Receiver;

use super::task_progress::TaskProgress;
use super::{CompactionStatistics, TaskConfig};
use crate::filter_key_extractor::FilterKeyExtractorImpl;
use crate::hummock::compactor::compaction_utils::{
    build_multi_compaction_filter, estimate_task_output_capacity, generate_splits,
};
use crate::hummock::compactor::iterator::ConcatSstableIterator;
use crate::hummock::compactor::task_progress::TaskProgressGuard;
use crate::hummock::compactor::{
    fast_compactor_runner, CompactOutput, CompactionFilter, Compactor, CompactorContext,
};
use crate::hummock::iterator::{Forward, HummockIterator, UnorderedMergeIteratorInner};
use crate::hummock::multi_builder::{CapacitySplitTableBuilder, TableBuilderFactory};
use crate::hummock::sstable::CompactionDeleteRangesBuilder;
use crate::hummock::value::HummockValue;
use crate::hummock::{
    BlockedXor16FilterBuilder, CachePolicy, CompactionDeleteRanges, CompressionAlgorithm,
    GetObjectId, HummockResult, MonotonicDeleteEvent, SstableBuilderOptions, SstableStoreRef,
};
use crate::monitor::{CompactorMetrics, StoreLocalStatistic};

pub struct CompactorRunner {
    compact_task: CompactTask,
    compactor: Compactor,
    sstable_store: SstableStoreRef,
    key_range: KeyRange,
    split_index: usize,
}

impl CompactorRunner {
    pub fn new(
        split_index: usize,
        context: CompactorContext,
        task: CompactTask,
        object_id_getter: Box<dyn GetObjectId>,
    ) -> Self {
        let mut options: SstableBuilderOptions = context.storage_opts.as_ref().into();
        options.compression_algorithm = match task.compression_algorithm {
            0 => CompressionAlgorithm::None,
            1 => CompressionAlgorithm::Lz4,
            _ => CompressionAlgorithm::Zstd,
        };

        options.capacity = estimate_task_output_capacity(context.clone(), &task);
        let kv_count = task
            .input_ssts
            .iter()
            .flat_map(|level| level.table_infos.iter())
            .map(|sst| sst.total_key_count)
            .sum::<u64>() as usize;
        let use_block_based_filter =
            BlockedXor16FilterBuilder::is_kv_count_too_large(kv_count) || task.target_level > 0;

        let key_range = KeyRange {
            left: Bytes::copy_from_slice(task.splits[split_index].get_left()),
            right: Bytes::copy_from_slice(task.splits[split_index].get_right()),
            right_exclusive: true,
        };

        let compactor = Compactor::new(
            context.clone(),
            options,
            TaskConfig {
                key_range: key_range.clone(),
                cache_policy: CachePolicy::NotFill,
                gc_delete_keys: task.gc_delete_keys,
                watermark: task.watermark,
                stats_target_table_ids: Some(HashSet::from_iter(task.existing_table_ids.clone())),
                task_type: task.task_type(),
                is_target_l0_or_lbase: task.target_level == 0
                    || task.target_level == task.base_level,
                split_by_table: task.split_by_state_table,
                split_weight_by_vnode: task.split_weight_by_vnode,
                use_block_based_filter,
            },
            object_id_getter,
        );

        Self {
            compactor,
            compact_task: task,
            sstable_store: context.sstable_store,
            key_range,
            split_index,
        }
    }

    pub async fn run(
        &self,
        compaction_filter: impl CompactionFilter,
        filter_key_extractor: Arc<FilterKeyExtractorImpl>,
        del_agg: Arc<CompactionDeleteRanges>,
        task_progress: Arc<TaskProgress>,
    ) -> HummockResult<CompactOutput> {
        let iter = self.build_sst_iter(task_progress.clone())?;
        let (ssts, compaction_stat) = self
            .compactor
            .compact_key_range(
                iter,
                compaction_filter,
                del_agg,
                filter_key_extractor,
                Some(task_progress),
                Some(self.compact_task.task_id),
                Some(self.split_index),
            )
            .await?;
        Ok((self.split_index, ssts, compaction_stat))
    }

    // This is a clippy bug, see https://github.com/rust-lang/rust-clippy/issues/11380.
    // TODO: remove `allow` here after the issued is closed.
    #[expect(clippy::needless_pass_by_ref_mut)]
    pub async fn build_delete_range_iter<F: CompactionFilter>(
        sstable_infos: &Vec<SstableInfo>,
        sstable_store: &SstableStoreRef,
        filter: &mut F,
    ) -> HummockResult<Arc<CompactionDeleteRanges>> {
        let mut builder = CompactionDeleteRangesBuilder::default();
        let mut local_stats = StoreLocalStatistic::default();

        for table_info in sstable_infos {
            let table = sstable_store.sstable(table_info, &mut local_stats).await?;
            let mut range_tombstone_list = table.value().meta.monotonic_tombstone_events.clone();
            range_tombstone_list.iter_mut().for_each(|tombstone| {
                if filter.should_delete(FullKey::from_user_key(
                    tombstone.event_key.left_user_key.as_ref(),
                    tombstone.new_epoch,
                )) {
                    tombstone.new_epoch = HummockEpoch::MAX;
                }
            });
            builder.add_delete_events(range_tombstone_list);
        }

        let aggregator = builder.build_for_compaction();
        Ok(aggregator)
    }

    /// Build the merge iterator based on the given input ssts.
    fn build_sst_iter(
        &self,
        task_progress: Arc<TaskProgress>,
    ) -> HummockResult<impl HummockIterator<Direction = Forward>> {
        let mut table_iters = Vec::new();
        let compact_io_retry_time = self
            .compactor
            .context
            .storage_opts
            .compact_iter_recreate_timeout_ms;

        for level in &self.compact_task.input_ssts {
            if level.table_infos.is_empty() {
                continue;
            }

            // Do not need to filter the table because manager has done it.
            if level.level_type == LevelType::Nonoverlapping as i32 {
                debug_assert!(can_concat(&level.table_infos));
                let tables = level
                    .table_infos
                    .iter()
                    .filter(|table_info| {
                        let key_range = KeyRange::from(table_info.key_range.as_ref().unwrap());
                        let table_ids = &table_info.table_ids;
                        let exist_table = table_ids.iter().any(|table_id| {
                            self.compact_task.existing_table_ids.contains(table_id)
                        });

                        self.key_range.full_key_overlap(&key_range) && exist_table
                    })
                    .cloned()
                    .collect_vec();
                table_iters.push(ConcatSstableIterator::new(
                    self.compact_task.existing_table_ids.clone(),
                    tables,
                    self.compactor.task_config.key_range.clone(),
                    self.sstable_store.clone(),
                    task_progress.clone(),
                    compact_io_retry_time,
                ));
            } else {
                for table_info in &level.table_infos {
                    let key_range = KeyRange::from(table_info.key_range.as_ref().unwrap());
                    let table_ids = &table_info.table_ids;
                    let exist_table = table_ids
                        .iter()
                        .any(|table_id| self.compact_task.existing_table_ids.contains(table_id));

                    if !self.key_range.full_key_overlap(&key_range) || !exist_table {
                        continue;
                    }
                    table_iters.push(ConcatSstableIterator::new(
                        self.compact_task.existing_table_ids.clone(),
                        vec![table_info.clone()],
                        self.compactor.task_config.key_range.clone(),
                        self.sstable_store.clone(),
                        task_progress.clone(),
                        compact_io_retry_time,
                    ));
                }
            }
        }
        Ok(UnorderedMergeIteratorInner::for_compactor(table_iters))
    }
}

/// Handles a compaction task and reports its status to hummock manager.
/// Always return `Ok` and let hummock manager handle errors.
pub async fn compact(
    compactor_context: CompactorContext,
    mut compact_task: CompactTask,
    mut shutdown_rx: Receiver<()>,
    object_id_getter: Box<dyn GetObjectId>,
) -> (CompactTask, HashMap<u32, TableStats>) {
    let context = compactor_context.clone();
    let group_label = compact_task.compaction_group_id.to_string();
    let cur_level_label = compact_task.input_ssts[0].level_idx.to_string();
    let select_table_infos = compact_task
        .input_ssts
        .iter()
        .filter(|level| level.level_idx != compact_task.target_level)
        .flat_map(|level| level.table_infos.iter())
        .collect_vec();
    let target_table_infos = compact_task
        .input_ssts
        .iter()
        .filter(|level| level.level_idx == compact_task.target_level)
        .flat_map(|level| level.table_infos.iter())
        .collect_vec();
    let select_size = select_table_infos
        .iter()
        .map(|table| table.file_size)
        .sum::<u64>();
    context
        .compactor_metrics
        .compact_read_current_level
        .with_label_values(&[&group_label, &cur_level_label])
        .inc_by(select_size);
    context
        .compactor_metrics
        .compact_read_sstn_current_level
        .with_label_values(&[&group_label, &cur_level_label])
        .inc_by(select_table_infos.len() as u64);

    let target_level_read_bytes = target_table_infos.iter().map(|t| t.file_size).sum::<u64>();
    let next_level_label = compact_task.target_level.to_string();
    context
        .compactor_metrics
        .compact_read_next_level
        .with_label_values(&[&group_label, next_level_label.as_str()])
        .inc_by(target_level_read_bytes);
    context
        .compactor_metrics
        .compact_read_sstn_next_level
        .with_label_values(&[&group_label, next_level_label.as_str()])
        .inc_by(target_table_infos.len() as u64);

    let timer = context
        .compactor_metrics
        .compact_task_duration
        .with_label_values(&[
            &group_label,
            &compact_task.input_ssts[0].level_idx.to_string(),
        ])
        .start_timer();

    let mut multi_filter = build_multi_compaction_filter(&compact_task);

    let mut compact_table_ids = compact_task
        .input_ssts
        .iter()
        .flat_map(|level| level.table_infos.iter())
        .flat_map(|sst| sst.table_ids.clone())
        .collect_vec();
    compact_table_ids.sort();
    compact_table_ids.dedup();
    let single_table = compact_table_ids.len() == 1;

    let existing_table_ids: HashSet<u32> =
        HashSet::from_iter(compact_task.existing_table_ids.clone());
    let compact_table_ids = HashSet::from_iter(
        compact_table_ids
            .into_iter()
            .filter(|table_id| existing_table_ids.contains(table_id)),
    );
    let multi_filter_key_extractor = match compactor_context
        .filter_key_extractor_manager
        .acquire(compact_table_ids.clone())
        .await
    {
        Err(e) => {
            tracing::error!("Failed to fetch filter key extractor tables [{:?}], it may caused by some RPC error {:?}", compact_task.existing_table_ids, e);
            let task_status = TaskStatus::ExecuteFailed;
            return compact_done(compact_task, context.clone(), vec![], task_status);
        }
        Ok(extractor) => extractor,
    };

    if let FilterKeyExtractorImpl::Multi(multi) = &multi_filter_key_extractor {
        let found_tables = multi.get_existing_table_ids();
        let removed_tables = compact_table_ids
            .iter()
            .filter(|table_id| !found_tables.contains(table_id))
            .collect_vec();
        if !removed_tables.is_empty() {
            tracing::error!("Failed to fetch filter key extractor tables [{:?}. [{:?}] may be removed by meta-service. ", compact_table_ids, removed_tables);
            let task_status = TaskStatus::ExecuteFailed;
            return compact_done(compact_task, context.clone(), vec![], task_status);
        }
    }

    let multi_filter_key_extractor = Arc::new(multi_filter_key_extractor);
    let has_tombstone = compact_task
        .input_ssts
        .iter()
        .flat_map(|level| level.table_infos.iter())
        .any(|sst| sst.range_tombstone_count > 0);
    let has_ttl = compact_task
        .table_options
        .iter()
        .any(|(_, table_option)| table_option.retention_seconds > 0);
    let mut task_status = TaskStatus::Success;
    // skip sst related to non-existent able_id to reduce io
    let sstable_infos = compact_task
        .input_ssts
        .iter()
        .flat_map(|level| level.table_infos.iter())
        .filter(|table_info| {
            let table_ids = &table_info.table_ids;
            table_ids
                .iter()
                .any(|table_id| existing_table_ids.contains(table_id))
        })
        .cloned()
        .collect_vec();
    let compaction_size = sstable_infos
        .iter()
        .map(|table_info| table_info.file_size)
        .sum::<u64>();
    let all_ssts_are_blocked_filter = sstable_infos
        .iter()
        .all(|table_info| table_info.bloom_filter_kind() == BloomFilterType::Blocked);
    let optimize_by_copy_block = context.storage_opts.enable_fast_compaction
        && all_ssts_are_blocked_filter
        && !has_tombstone
        && !has_ttl
        && single_table
        && compact_task.target_level > 0
        && compact_task.input_ssts.len() == 2
        && compact_task.task_type() == TaskType::Dynamic;
    if !optimize_by_copy_block {
        match generate_splits(&sstable_infos, compaction_size, context.clone()).await {
            Ok(splits) => {
                if !splits.is_empty() {
                    compact_task.splits = splits;
                }
            }
            Err(e) => {
                tracing::warn!("Failed to generate_splits {:#?}", e);
                task_status = TaskStatus::ExecuteFailed;
                return compact_done(compact_task, context.clone(), vec![], task_status);
            }
        }
    }
    let compact_task_statistics = statistics_compact_task(&compact_task);
    // Number of splits (key ranges) is equal to number of compaction tasks
    let parallelism = compact_task.splits.len();
    assert_ne!(parallelism, 0, "splits cannot be empty");
    let mut output_ssts = Vec::with_capacity(parallelism);
    let mut compaction_futures = vec![];
    let mut abort_handles = vec![];
    let task_progress_guard =
        TaskProgressGuard::new(compact_task.task_id, context.task_progress_manager.clone());
    let delete_range_agg = match CompactorRunner::build_delete_range_iter(
        &sstable_infos,
        &compactor_context.sstable_store,
        &mut multi_filter,
    )
    .await
    {
        Ok(agg) => agg,
        Err(err) => {
            tracing::warn!("Failed to build delete range aggregator {:#?}", err);
            task_status = TaskStatus::ExecuteFailed;
            return compact_done(compact_task, context.clone(), vec![], task_status);
        }
    };

    let capacity = estimate_task_output_capacity(context.clone(), &compact_task);

    let task_memory_capacity_with_parallelism = estimate_memory_for_compact_task(
        &compact_task,
        (context.storage_opts.block_size_kb as u64) * (1 << 10),
        context
            .storage_opts
            .object_store_recv_buffer_size
            .unwrap_or(6 * 1024 * 1024) as u64,
        capacity as u64,
        context.sstable_store.store().support_streaming_upload(),
    ) * compact_task.splits.len() as u64;

    tracing::info!(
            "Ready to handle compaction group {} task: {} compact_task_statistics {:?} target_level {} compression_algorithm {:?} table_ids {:?} parallelism {} task_memory_capacity_with_parallelism {}",
                compact_task.compaction_group_id,
                compact_task.task_id,
                compact_task_statistics,
                compact_task.target_level,
                compact_task.compression_algorithm,
                compact_task.existing_table_ids,
                parallelism,
                task_memory_capacity_with_parallelism
            );

    // If the task does not have enough memory, it should cancel the task and let the meta
    // reschedule it, so that it does not occupy the compactor's resources.
    let memory_detector = context
        .memory_limiter
        .try_require_memory(task_memory_capacity_with_parallelism);
    if memory_detector.is_none() {
        tracing::warn!(
                "Not enough memory to serve the task {} task_memory_capacity_with_parallelism {}  memory_usage {} memory_quota {}",
                compact_task.task_id,
                task_memory_capacity_with_parallelism,
                context.memory_limiter.get_memory_usage(),
                context.memory_limiter.quota()
            );
        task_status = TaskStatus::NoAvailResourceCanceled;
        return compact_done(compact_task, context.clone(), output_ssts, task_status);
    }

    context.compactor_metrics.compact_task_pending_num.inc();
    if optimize_by_copy_block {
        let runner = fast_compactor_runner::CompactorRunner::new(
            context.clone(),
            compact_task.clone(),
            multi_filter_key_extractor.clone(),
            object_id_getter.clone(),
            task_progress_guard.progress.clone(),
        );
        let ret = match runner.run().await {
            Err(e) => Err(e),
            Ok(outputs) => {
                Compactor::report_progress(
                    context.clone(),
                    Some(task_progress_guard.progress.clone()),
                    outputs,
                )
                .await
            }
        };

        match ret {
            Ok(ssts) => {
                output_ssts.push((0, ssts, CompactionStatistics::default()));
            }
            Err(e) => {
                task_status = TaskStatus::ExecuteFailed;
                tracing::warn!(
                    "Compaction task {} failed with error: {:#?}",
                    compact_task.task_id,
                    e
                );
            }
        }

        context.compactor_metrics.compact_task_pending_num.dec();
        // After a compaction is done, mutate the compaction task.
        let (compact_task, table_stats) =
            compact_done(compact_task, context.clone(), output_ssts, task_status);
        let cost_time = timer.stop_and_record() * 1000.0;
        tracing::info!(
            "Finished compaction task in {:?}ms: {}",
            cost_time,
            compact_task_to_string(&compact_task)
        );
        return (compact_task, table_stats);
    }
    for (split_index, _) in compact_task.splits.iter().enumerate() {
        let filter = multi_filter.clone();
        let multi_filter_key_extractor = multi_filter_key_extractor.clone();
        let compactor_runner = CompactorRunner::new(
            split_index,
            compactor_context.clone(),
            compact_task.clone(),
            object_id_getter.clone(),
        );
        let del_agg = delete_range_agg.clone();
        let task_progress = task_progress_guard.progress.clone();
        let runner = async move {
            compactor_runner
                .run(filter, multi_filter_key_extractor, del_agg, task_progress)
                .await
        };
        let traced = match context.await_tree_reg.as_ref() {
            None => runner.right_future(),
            Some(await_tree_reg) => await_tree_reg
                .write()
                .register(
                    format!("{}-{}", compact_task.task_id, split_index),
                    format!(
                        "Compaction Task {} Split {} ",
                        compact_task.task_id, split_index
                    ),
                )
                .instrument(runner)
                .left_future(),
        };
        let handle = tokio::spawn(traced);
        abort_handles.push(handle.abort_handle());
        compaction_futures.push(handle);
    }

    let mut buffered = stream::iter(compaction_futures).buffer_unordered(parallelism);
    loop {
        tokio::select! {
            _ = &mut shutdown_rx => {
                tracing::warn!("Compaction task cancelled externally:\n{}", compact_task_to_string(&compact_task));
                task_status = TaskStatus::ManualCanceled;
                break;
            }
            future_result = buffered.next() => {
                match future_result {
                    Some(Ok(Ok((split_index, ssts, compact_stat)))) => {
                        output_ssts.push((split_index, ssts, compact_stat));
                    }
                    Some(Ok(Err(e))) => {
                        task_status = TaskStatus::ExecuteFailed;
                        tracing::warn!(
                            "Compaction task {} failed with error: {:#?}",
                            compact_task.task_id,
                            e
                        );
                        break;
                    }
                    Some(Err(e)) => {
                        task_status = TaskStatus::JoinHandleFailed;
                        tracing::warn!(
                            "Compaction task {} failed with join handle error: {:#?}",
                            compact_task.task_id,
                            e
                        );
                        break;
                    }
                    None => break,
                }
            }
        }
    }

    drop(memory_detector);

    if task_status != TaskStatus::Success {
        for abort_handle in abort_handles {
            abort_handle.abort();
        }
        output_ssts.clear();
    }
    // Sort by split/key range index.
    if !output_ssts.is_empty() {
        output_ssts.sort_by_key(|(split_index, ..)| *split_index);
    }

    // After a compaction is done, mutate the compaction task.
    let (compact_task, table_stats) =
        compact_done(compact_task, context.clone(), output_ssts, task_status);
    let cost_time = timer.stop_and_record() * 1000.0;
    tracing::info!(
        "Finished compaction task in {:?}ms: {}",
        cost_time,
        compact_task_to_string(&compact_task)
    );
    context.compactor_metrics.compact_task_pending_num.dec();
    for level in &compact_task.input_ssts {
        for table in &level.table_infos {
            context.sstable_store.delete_cache(table.get_object_id());
        }
    }
    (compact_task, table_stats)
}

/// Fills in the compact task and tries to report the task result to meta node.
fn compact_done(
    mut compact_task: CompactTask,
    context: CompactorContext,
    output_ssts: Vec<CompactOutput>,
    task_status: TaskStatus,
) -> (CompactTask, HashMap<u32, TableStats>) {
    let mut table_stats_map = TableStatsMap::default();
    compact_task.set_task_status(task_status);
    compact_task
        .sorted_output_ssts
        .reserve(compact_task.splits.len());
    let mut compaction_write_bytes = 0;
    for (
        _,
        ssts,
        CompactionStatistics {
            delta_drop_stat, ..
        },
    ) in output_ssts
    {
        add_table_stats_map(&mut table_stats_map, &delta_drop_stat);
        for sst_info in ssts {
            compaction_write_bytes += sst_info.file_size();
            compact_task.sorted_output_ssts.push(sst_info.sst_info);
        }
    }

    let group_label = compact_task.compaction_group_id.to_string();
    let level_label = compact_task.target_level.to_string();
    context
        .compactor_metrics
        .compact_write_bytes
        .with_label_values(&[&group_label, level_label.as_str()])
        .inc_by(compaction_write_bytes);
    context
        .compactor_metrics
        .compact_write_sstn
        .with_label_values(&[&group_label, level_label.as_str()])
        .inc_by(compact_task.sorted_output_ssts.len() as u64);

    (compact_task, table_stats_map)
}

pub async fn compact_and_build_sst<F>(
    sst_builder: &mut CapacitySplitTableBuilder<F>,
    del_agg: Arc<CompactionDeleteRanges>,
    task_config: &TaskConfig,
    compactor_metrics: Arc<CompactorMetrics>,
    mut iter: impl HummockIterator<Direction = Forward>,
    mut compaction_filter: impl CompactionFilter,
    task_progress: Option<Arc<TaskProgress>>,
) -> HummockResult<CompactionStatistics>
where
    F: TableBuilderFactory,
{
    let mut del_iter = del_agg.iter();
    if !task_config.key_range.left.is_empty() {
        let full_key = FullKey::decode(&task_config.key_range.left);
        iter.seek(full_key)
            .verbose_instrument_await("iter_seek")
            .await?;
        del_iter.seek(full_key.user_key);
        if !task_config.gc_delete_keys
            && del_iter.is_valid()
            && del_iter.earliest_epoch() != HummockEpoch::MAX
        {
            sst_builder
                .add_monotonic_delete(MonotonicDeleteEvent {
                    event_key: PointRange::from_user_key(full_key.user_key.to_vec(), false),
                    new_epoch: del_iter.earliest_epoch(),
                })
                .await?;
        }
    } else {
        iter.rewind().verbose_instrument_await("rewind").await?;
        del_iter.rewind();
    };

    let end_key = if task_config.key_range.right.is_empty() {
        FullKey::default()
    } else {
        FullKey::decode(&task_config.key_range.right).to_vec()
    };
    let max_key = end_key.to_ref();

    let mut last_key = FullKey::default();
    let mut watermark_can_see_last_key = false;
    let mut user_key_last_delete_epoch = HummockEpoch::MAX;
    let mut local_stats = StoreLocalStatistic::default();

    // Keep table stats changes due to dropping KV.
    let mut table_stats_drop = TableStatsMap::default();
    let mut last_table_stats = TableStats::default();
    let mut last_table_id = None;
    let mut compaction_statistics = CompactionStatistics::default();
    let mut progress_key_num: u64 = 0;
    const PROGRESS_KEY_INTERVAL: u64 = 100;
    while iter.is_valid() {
        progress_key_num += 1;

        if let Some(task_progress) = task_progress.as_ref() && progress_key_num >= PROGRESS_KEY_INTERVAL {
                task_progress.inc_progress_key(progress_key_num);
                progress_key_num = 0;
            }

        let mut iter_key = iter.key();
        compaction_statistics.iter_total_key_counts += 1;

        let mut is_new_user_key =
            last_key.is_empty() || iter_key.user_key != last_key.user_key.as_ref();

        let mut drop = false;
        let epoch = iter_key.epoch;
        let value = iter.value();
        if is_new_user_key {
            if !max_key.is_empty() && iter_key >= max_key {
                break;
            }
            last_key.set(iter_key);
            watermark_can_see_last_key = false;
            user_key_last_delete_epoch = HummockEpoch::MAX;
            if value.is_delete() {
                local_stats.skip_delete_key_count += 1;
            }
        } else {
            local_stats.skip_multi_version_key_count += 1;
        }

        if last_table_id.map_or(true, |last_table_id| {
            last_table_id != last_key.user_key.table_id.table_id
        }) {
            if let Some(last_table_id) = last_table_id.take() {
                table_stats_drop.insert(last_table_id, std::mem::take(&mut last_table_stats));
            }
            last_table_id = Some(last_key.user_key.table_id.table_id);
        }

        let target_extended_user_key = PointRange::from_user_key(iter_key.user_key, false);
        while del_iter.is_valid() && del_iter.key().as_ref().le(&target_extended_user_key) {
            del_iter.update_range();
            if !task_config.gc_delete_keys {
                sst_builder
                    .add_monotonic_delete(MonotonicDeleteEvent {
                        event_key: del_iter.key().clone(),
                        new_epoch: del_iter.earliest_epoch(),
                    })
                    .await?;
            }
            del_iter.next();
        }
        let earliest_range_delete_which_can_see_iter_key = del_iter.earliest_delete_since(epoch);

        // Among keys with same user key, only retain keys which satisfy `epoch` >= `watermark`.
        // If there is no keys whose epoch is equal or greater than `watermark`, keep the latest
        // key which satisfies `epoch` < `watermark`
        // in our design, frontend avoid to access keys which had be deleted, so we dont
        // need to consider the epoch when the compaction_filter match (it
        // means that mv had drop)
        if (epoch <= task_config.watermark && task_config.gc_delete_keys && value.is_delete())
            || (epoch < task_config.watermark
                && (watermark_can_see_last_key
                    || earliest_range_delete_which_can_see_iter_key <= task_config.watermark))
        {
            drop = true;
        }

        if !drop && compaction_filter.should_delete(iter_key) {
            drop = true;
        }

        if epoch <= task_config.watermark {
            watermark_can_see_last_key = true;
        }
        if drop {
            compaction_statistics.iter_drop_key_counts += 1;

            let should_count = match task_config.stats_target_table_ids.as_ref() {
                Some(target_table_ids) => {
                    target_table_ids.contains(&last_key.user_key.table_id.table_id)
                }
                None => true,
            };
            if should_count {
                last_table_stats.total_key_count -= 1;
                last_table_stats.total_key_size -= last_key.encoded_len() as i64;
                last_table_stats.total_value_size -= iter.value().encoded_len() as i64;
            }
            iter.next()
                .verbose_instrument_await("iter_next_in_drop")
                .await?;
            continue;
        }

        if value.is_delete() {
            user_key_last_delete_epoch = epoch;
        } else if earliest_range_delete_which_can_see_iter_key < user_key_last_delete_epoch {
            debug_assert!(
                iter_key.epoch < earliest_range_delete_which_can_see_iter_key
                    && earliest_range_delete_which_can_see_iter_key < user_key_last_delete_epoch
            );
            user_key_last_delete_epoch = earliest_range_delete_which_can_see_iter_key;

            // In each SST, since a union set of delete ranges is constructed and thus original
            // delete ranges are replaced with the union set and not used in read, we lose exact
            // information about whether a key is deleted by a delete range in
            // the same SST. Therefore we need to construct a corresponding
            // delete key to represent this.
            iter_key.epoch = earliest_range_delete_which_can_see_iter_key;
            sst_builder
                .add_full_key(iter_key, HummockValue::Delete, is_new_user_key)
                .verbose_instrument_await("add_full_key_delete")
                .await?;
            last_table_stats.total_key_count += 1;
            last_table_stats.total_key_size += iter_key.encoded_len() as i64;
            last_table_stats.total_value_size += 1;
            iter_key.epoch = epoch;
            is_new_user_key = false;
        }

        // Don't allow two SSTs to share same user key
        sst_builder
            .add_full_key(iter_key, value, is_new_user_key)
            .verbose_instrument_await("add_full_key")
            .await?;

        iter.next().verbose_instrument_await("iter_next").await?;
    }

    if !task_config.gc_delete_keys {
        let extended_largest_user_key = PointRange::from_user_key(end_key.user_key.clone(), false);
        while del_iter.is_valid() {
            if !extended_largest_user_key.is_empty()
                && del_iter.key().ge(&extended_largest_user_key)
            {
                sst_builder
                    .add_monotonic_delete(MonotonicDeleteEvent {
                        event_key: extended_largest_user_key,
                        new_epoch: HummockEpoch::MAX,
                    })
                    .await?;
                break;
            }
            del_iter.update_range();
            sst_builder
                .add_monotonic_delete(MonotonicDeleteEvent {
                    event_key: del_iter.key().clone(),
                    new_epoch: del_iter.earliest_epoch(),
                })
                .await?;
            del_iter.next();
        }
    }

    if let Some(task_progress) = task_progress.as_ref() && progress_key_num > 0 {
            // Avoid losing the progress_key_num in the last Interval
            task_progress.inc_progress_key(progress_key_num);
        }

    if let Some(last_table_id) = last_table_id.take() {
        table_stats_drop.insert(last_table_id, std::mem::take(&mut last_table_stats));
    }
    iter.collect_local_statistic(&mut local_stats);
    local_stats.report_compactor(compactor_metrics.as_ref());
    compaction_statistics.delta_drop_stat = table_stats_drop;

    Ok(compaction_statistics)
}
#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use risingwave_common::catalog::TableId;
    use risingwave_hummock_sdk::key::UserKey;
    use risingwave_pb::hummock::InputLevel;

    use super::*;
    use crate::hummock::compactor::StateCleanUpCompactionFilter;
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::test_utils::{
        default_builder_opt_for_test, gen_test_sstable_with_range_tombstone,
    };
    use crate::hummock::{create_monotonic_events, DeleteRangeTombstone};

    #[tokio::test]
    async fn test_delete_range_aggregator_with_filter() {
        let sstable_store = mock_sstable_store();
        let kv_pairs = vec![];
        let range_tombstones = vec![
            DeleteRangeTombstone::new_for_test(
                TableId::new(1),
                b"abc".to_vec(),
                b"cde".to_vec(),
                1,
            ),
            DeleteRangeTombstone::new_for_test(
                TableId::new(2),
                b"abc".to_vec(),
                b"def".to_vec(),
                1,
            ),
        ];
        let mut sstable_info_1 = gen_test_sstable_with_range_tombstone(
            default_builder_opt_for_test(),
            1,
            kv_pairs.clone().into_iter(),
            range_tombstones.clone(),
            sstable_store.clone(),
        )
        .await
        .get_sstable_info();
        sstable_info_1.table_ids = vec![1];

        let mut sstable_info_2 = gen_test_sstable_with_range_tombstone(
            default_builder_opt_for_test(),
            2,
            kv_pairs.into_iter(),
            range_tombstones.clone(),
            sstable_store.clone(),
        )
        .await
        .get_sstable_info();
        sstable_info_2.table_ids = vec![2];

        let compact_task = CompactTask {
            input_ssts: vec![InputLevel {
                level_idx: 0,
                level_type: 0,
                table_infos: vec![sstable_info_1, sstable_info_2],
            }],
            existing_table_ids: vec![2],
            ..Default::default()
        };
        let mut state_clean_up_filter = StateCleanUpCompactionFilter::new(HashSet::from_iter(
            compact_task.existing_table_ids.clone(),
        ));

        let sstable_infos = compact_task
            .input_ssts
            .iter()
            .flat_map(|level| level.table_infos.iter())
            .filter(|table_info| {
                let table_ids = &table_info.table_ids;
                table_ids
                    .iter()
                    .any(|table_id| compact_task.existing_table_ids.contains(table_id))
            })
            .cloned()
            .collect_vec();

        let collector = CompactorRunner::build_delete_range_iter(
            &sstable_infos,
            &sstable_store,
            &mut state_clean_up_filter,
        )
        .await
        .unwrap();
        let ret = collector.get_tombstone_between(
            UserKey::<Bytes>::default().as_ref(),
            UserKey::<Bytes>::default().as_ref(),
        );

        assert_eq!(
            ret,
            create_monotonic_events(vec![range_tombstones[1].clone()])
        );
    }
}

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

mod compaction_executor;
mod compaction_filter;
pub mod compaction_utils;
mod compactor_runner;
mod context;
mod iterator;
mod shared_buffer_compact;
pub(super) mod task_progress;

use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::ops::Div;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub use compaction_executor::CompactionExecutor;
pub use compaction_filter::{
    CompactionFilter, DummyCompactionFilter, MultiCompactionFilter, StateCleanUpCompactionFilter,
    TtlCompactionFilter,
};
pub use context::CompactorContext;
use futures::future::try_join_all;
use futures::{stream, StreamExt};
pub use iterator::ConcatSstableIterator;
use itertools::Itertools;
use risingwave_hummock_sdk::compact::{compact_task_to_string, estimate_state_for_compaction};
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::table_stats::{add_table_stats_map, TableStats, TableStatsMap};
use risingwave_hummock_sdk::{HummockCompactionTaskId, HummockEpoch, LocalSstableInfo};
use risingwave_pb::hummock::compact_task::TaskStatus;
use risingwave_pb::hummock::subscribe_compact_tasks_response::Task;
use risingwave_pb::hummock::{
    CompactTask, CompactTaskProgress, CompactorWorkload, SubscribeCompactTasksResponse,
};
use risingwave_rpc_client::HummockMetaClient;
pub use shared_buffer_compact::{compact, merge_imms_in_memory};
use sysinfo::{CpuRefreshKind, ProcessExt, ProcessRefreshKind, RefreshKind, System, SystemExt};
use tokio::sync::oneshot::{Receiver, Sender};
use tokio::task::JoinHandle;

pub use self::compaction_utils::{CompactionStatistics, RemoteBuilderFactory, TaskConfig};
use self::task_progress::TaskProgress;
use super::multi_builder::CapacitySplitTableBuilder;
use super::value::HummockValue;
use super::{CompactionDeleteRanges, HummockResult, SstableBuilderOptions, Xor16FilterBuilder};
use crate::filter_key_extractor::FilterKeyExtractorImpl;
use crate::hummock::compactor::compaction_utils::{
    build_multi_compaction_filter, estimate_task_memory_capacity, generate_splits,
};
use crate::hummock::compactor::compactor_runner::CompactorRunner;
use crate::hummock::compactor::task_progress::TaskProgressGuard;
use crate::hummock::iterator::{Forward, HummockIterator};
use crate::hummock::multi_builder::{SplitTableOutput, TableBuilderFactory};
use crate::hummock::vacuum::Vacuum;
use crate::hummock::{
    validate_ssts, BatchSstableWriterFactory, FilterBuilder, HummockError, SstableWriterFactory,
    StreamingSstableWriterFactory, Xor8FilterBuilder,
};
use crate::monitor::{CompactorMetrics, StoreLocalStatistic};

/// Implementation of Hummock compaction.
pub struct Compactor {
    /// The context of the compactor.
    context: Arc<CompactorContext>,
    task_config: TaskConfig,
    options: SstableBuilderOptions,
    get_id_time: Arc<AtomicU64>,
}

pub type CompactOutput = (usize, Vec<LocalSstableInfo>, CompactionStatistics);

impl Compactor {
    /// Handles a compaction task and reports its status to hummock manager.
    /// Always return `Ok` and let hummock manager handle errors.
    pub async fn compact(
        compactor_context: Arc<CompactorContext>,
        mut compact_task: CompactTask,
        mut shutdown_rx: Receiver<()>,
    ) -> TaskStatus {
        let context = compactor_context.clone();
        // Set a watermark SST id to prevent full GC from accidentally deleting SSTs for in-progress
        // write op. The watermark is invalidated when this method exits.
        let tracker_id = match context
            .sstable_object_id_manager
            .add_watermark_object_id(None)
            .await
        {
            Ok(tracker_id) => tracker_id,
            Err(err) => {
                tracing::warn!("Failed to track pending SST object id. {:#?}", err);
                return TaskStatus::TrackSstObjectIdFailed;
            }
        };
        let sstable_object_id_manager_clone = context.sstable_object_id_manager.clone();
        let _guard = scopeguard::guard(
            (tracker_id, sstable_object_id_manager_clone),
            |(tracker_id, sstable_object_id_manager)| {
                sstable_object_id_manager.remove_watermark_object_id(tracker_id);
            },
        );

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

        let (need_quota, total_file_count, total_key_count) =
            estimate_state_for_compaction(&compact_task);

        let mut multi_filter = build_multi_compaction_filter(&compact_task);

        let mut compact_table_ids = compact_task
            .input_ssts
            .iter()
            .flat_map(|level| level.table_infos.iter())
            .flat_map(|sst| sst.table_ids.clone())
            .collect_vec();
        compact_table_ids.sort();
        compact_table_ids.dedup();

        let existing_table_ids: HashSet<u32> =
            HashSet::from_iter(compact_task.existing_table_ids.clone());
        let compact_table_ids = HashSet::from_iter(
            compact_table_ids
                .into_iter()
                .filter(|table_id| existing_table_ids.contains(table_id)),
        );
        let multi_filter_key_extractor = match context
            .filter_key_extractor_manager
            .acquire(compact_table_ids.clone())
            .await
        {
            Err(e) => {
                tracing::error!("Failed to fetch filter key extractor tables [{:?}], it may caused by some RPC error {:?}", compact_task.existing_table_ids, e);
                let task_status = TaskStatus::ExecuteFailed;
                Self::compact_done(&mut compact_task, context.clone(), vec![], task_status).await;
                return task_status;
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
                Self::compact_done(&mut compact_task, context.clone(), vec![], task_status).await;
                return task_status;
            }
        }

        let multi_filter_key_extractor = Arc::new(multi_filter_key_extractor);

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
        match generate_splits(&sstable_infos, compaction_size, context.clone()).await {
            Ok(splits) => {
                if !splits.is_empty() {
                    compact_task.splits = splits;
                }
            }

            Err(e) => {
                tracing::warn!("Failed to generate_splits {:#?}", e);
                task_status = TaskStatus::ExecuteFailed;
                Self::compact_done(&mut compact_task, context.clone(), vec![], task_status).await;
                return task_status;
            }
        }
        // Number of splits (key ranges) is equal to number of compaction tasks
        let parallelism = compact_task.splits.len();
        assert_ne!(parallelism, 0, "splits cannot be empty");
        let mut output_ssts = Vec::with_capacity(parallelism);
        let mut compaction_futures = vec![];
        let task_progress_guard =
            TaskProgressGuard::new(compact_task.task_id, context.task_progress_manager.clone());
        let delete_range_agg = match CompactorRunner::build_delete_range_iter(
            &sstable_infos,
            compact_task.gc_delete_keys,
            &compactor_context.sstable_store,
            &mut multi_filter,
        )
        .await
        {
            Ok(agg) => agg,
            Err(err) => {
                tracing::warn!("Failed to build delete range aggregator {:#?}", err);
                task_status = TaskStatus::ExecuteFailed;
                Self::compact_done(&mut compact_task, context.clone(), vec![], task_status).await;
                return task_status;
            }
        };

        let task_memory_capacity_with_parallelism =
            estimate_task_memory_capacity(context.clone(), &compact_task) * parallelism;

        tracing::info!(
                "Ready to handle compaction task: {} need memory: {} input_file_counts {} total_key_count {} target_level {} compression_algorithm {:?} parallelism {} task_memory_capacity_with_parallelism {}",
                compact_task.task_id,
                need_quota,
                total_file_count,
                total_key_count,
                compact_task.target_level,
                compact_task.compression_algorithm,
                parallelism,
                task_memory_capacity_with_parallelism
            );

        // If the task does not have enough memory, it should cancel the task and let the meta
        // reschedule it, so that it does not occupy the compactor's resources.
        let memory_detector = context
            .output_memory_limiter
            .try_require_memory(task_memory_capacity_with_parallelism as u64);
        if memory_detector.is_none() {
            tracing::warn!(
                "Not enough memory to serve the task {} task_memory_capacity_with_parallelism {}  memory_usage {} memory_quota {}",
                compact_task.task_id,
                task_memory_capacity_with_parallelism,
                context.output_memory_limiter.get_memory_usage(),
                context.output_memory_limiter.quota()
            );
            task_status = TaskStatus::NoAvailResourceCanceled;
            Self::compact_done(&mut compact_task, context.clone(), output_ssts, task_status).await;
            return task_status;
        }

        drop(memory_detector);
        context.compactor_metrics.compact_task_pending_num.inc();
        for (split_index, _) in compact_task.splits.iter().enumerate() {
            let filter = multi_filter.clone();
            let multi_filter_key_extractor = multi_filter_key_extractor.clone();
            let compactor_runner =
                CompactorRunner::new(split_index, compactor_context.clone(), compact_task.clone());
            let del_agg = delete_range_agg.clone();
            let task_progress = task_progress_guard.progress.clone();
            let handle = tokio::spawn(async move {
                compactor_runner
                    .run(filter, multi_filter_key_extractor, del_agg, task_progress)
                    .await
            });
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

        if task_status != TaskStatus::Success {
            output_ssts.clear();
        }
        // Sort by split/key range index.
        if !output_ssts.is_empty() {
            output_ssts.sort_by_key(|(split_index, ..)| *split_index);
        }

        // After a compaction is done, mutate the compaction task.
        Self::compact_done(&mut compact_task, context.clone(), output_ssts, task_status).await;
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
        task_status
    }

    /// Fills in the compact task and tries to report the task result to meta node.
    async fn compact_done(
        compact_task: &mut CompactTask,
        context: Arc<CompactorContext>,
        output_ssts: Vec<CompactOutput>,
        task_status: TaskStatus,
    ) {
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

        if let Err(e) = context
            .hummock_meta_client
            .report_compaction_task(compact_task.clone(), table_stats_map)
            .await
        {
            tracing::warn!(
                "Failed to report compaction task: {}, error: {}",
                compact_task.task_id,
                e
            );
        }
    }

    /// The background compaction thread that receives compaction tasks from hummock compaction
    /// manager and runs compaction tasks.
    #[cfg_attr(coverage, no_coverage)]
    pub fn start_compactor(
        compactor_context: Arc<CompactorContext>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
    ) -> (JoinHandle<()>, Sender<()>) {
        type CompactionShutdownMap = Arc<Mutex<HashMap<u64, Sender<()>>>>;
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        let stream_retry_interval = Duration::from_secs(30);
        let task_progress = compactor_context.task_progress_manager.clone();
        let task_progress_update_interval = Duration::from_millis(1000);
        let cpu_core_num = compactor_context.compaction_executor.worker_num() as u32;
        let mut system =
            System::new_with_specifics(RefreshKind::new().with_cpu(CpuRefreshKind::everything()));
        let pid = sysinfo::get_current_pid().unwrap();

        let join_handle = tokio::spawn(async move {
            let shutdown_map = CompactionShutdownMap::default();
            let mut min_interval = tokio::time::interval(stream_retry_interval);
            let mut task_progress_interval = tokio::time::interval(task_progress_update_interval);
            let mut workload_collect_interval = tokio::time::interval(Duration::from_secs(60));

            // This outer loop is to recreate stream.
            'start_stream: loop {
                tokio::select! {
                    // Wait for interval.
                    _ = min_interval.tick() => {},
                    // Shutdown compactor.
                    _ = &mut shutdown_rx => {
                        tracing::info!("Compactor is shutting down");
                        return;
                    }
                }

                let mut stream = match hummock_meta_client
                    .subscribe_compact_tasks(cpu_core_num)
                    .await
                {
                    Ok(stream) => {
                        tracing::debug!("Succeeded subscribe_compact_tasks.");
                        stream
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Subscribing to compaction tasks failed with error: {}. Will retry.",
                            e
                        );
                        continue 'start_stream;
                    }
                };

                let executor = compactor_context.compaction_executor.clone();
                let mut last_workload = CompactorWorkload::default();

                // This inner loop is to consume stream or report task progress.
                'consume_stream: loop {
                    let message: Option<Result<SubscribeCompactTasksResponse, _>> = tokio::select! {
                        _ = task_progress_interval.tick() => {
                            let mut progress_list = Vec::new();
                            for (&task_id, progress) in task_progress.lock().iter() {
                                progress_list.push(CompactTaskProgress {
                                    task_id,
                                    num_ssts_sealed: progress.num_ssts_sealed.load(Ordering::Relaxed),
                                    num_ssts_uploaded: progress.num_ssts_uploaded.load(Ordering::Relaxed),
                                    num_progress_key: progress.num_progress_key.load(Ordering::Relaxed),
                                });
                            }

                            if let Err(e) = hummock_meta_client.compactor_heartbeat(progress_list, last_workload.clone()).await {
                                // ignore any errors while trying to report task progress
                                tracing::warn!("Failed to report task progress. {e:?}");
                            }
                            continue;
                        }

                        _ = workload_collect_interval.tick() => {
                            let refresh_result = system.refresh_process_specifics(pid, ProcessRefreshKind::new().with_cpu());
                            debug_assert!(refresh_result);
                            let cpu = if let Some(process) = system.process(pid) {
                                process.cpu_usage().div(cpu_core_num as f32) as u32
                            } else {
                                tracing::warn!("fail to get process pid {:?}", pid);
                                0
                            };

                            tracing::debug!("compactor cpu usage {cpu}");
                            let workload = CompactorWorkload {
                                cpu,
                            };

                            last_workload = workload.clone();

                            continue;
                        }

                        message = stream.next() => {
                            message
                        },
                        _ = &mut shutdown_rx => {
                            tracing::info!("Compactor is shutting down");
                            return
                        }
                    };
                    match message {
                        Some(Ok(SubscribeCompactTasksResponse { task })) => {
                            let task = match task {
                                Some(task) => task,
                                None => continue 'consume_stream,
                            };

                            let shutdown = shutdown_map.clone();
                            let context = compactor_context.clone();
                            let meta_client = hummock_meta_client.clone();

                            executor.spawn(async move {
                                match task {
                                    Task::CompactTask(compact_task) => {
                                        let (tx, rx) = tokio::sync::oneshot::channel();
                                        let task_id = compact_task.task_id;
                                        shutdown.lock().unwrap().insert(task_id, tx);
                                        Compactor::compact(context, compact_task, rx).await;
                                        shutdown.lock().unwrap().remove(&task_id);
                                    }
                                    Task::VacuumTask(vacuum_task) => {
                                        Vacuum::vacuum(
                                            vacuum_task,
                                            context.sstable_store.clone(),
                                            meta_client,
                                        )
                                        .await;
                                    }
                                    Task::FullScanTask(full_scan_task) => {
                                        Vacuum::full_scan(
                                            full_scan_task,
                                            context.sstable_store.clone(),
                                            meta_client,
                                        )
                                        .await;
                                    }
                                    Task::ValidationTask(validation_task) => {
                                        validate_ssts(
                                            validation_task,
                                            context.sstable_store.clone(),
                                        )
                                        .await;
                                    }
                                    Task::CancelCompactTask(cancel_compact_task) => {
                                        if let Some(tx) = shutdown
                                            .lock()
                                            .unwrap()
                                            .remove(&cancel_compact_task.task_id)
                                        {
                                            if tx.send(()).is_err() {
                                                tracing::warn!(
                                                    "Cancellation of compaction task failed. task_id: {}",
                                                    cancel_compact_task.task_id
                                                );
                                        }
                                    } else {
                                        tracing::warn!(
                                                "Attempting to cancel non-existent compaction task. task_id: {}",
                                                cancel_compact_task.task_id
                                            );
                                    }
                                }
                            }
                            });
                        }
                        Some(Err(e)) => {
                            tracing::warn!("Failed to consume stream. {}", e.message());
                            continue 'start_stream;
                        }
                        _ => {
                            // The stream is exhausted
                            continue 'start_stream;
                        }
                    }
                }
            }
        });

        (join_handle, shutdown_tx)
    }

    pub async fn compact_and_build_sst<F>(
        sst_builder: &mut CapacitySplitTableBuilder<F>,
        task_config: &TaskConfig,
        compactor_metrics: Arc<CompactorMetrics>,
        mut iter: impl HummockIterator<Direction = Forward>,
        mut compaction_filter: impl CompactionFilter,
        task_progress: Option<Arc<TaskProgress>>,
    ) -> HummockResult<CompactionStatistics>
    where
        F: TableBuilderFactory,
    {
        let mut del_iter = sst_builder.del_agg.iter();

        if !task_config.key_range.left.is_empty() {
            let full_key = FullKey::decode(&task_config.key_range.left);
            iter.seek(full_key).await?;
            del_iter.seek(full_key.user_key);
        } else {
            iter.rewind().await?;
            del_iter.rewind();
        }

        let max_key = if task_config.key_range.right.is_empty() {
            FullKey::default()
        } else {
            FullKey::decode(&task_config.key_range.right).to_vec()
        };
        let max_key = max_key.to_ref();

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

            let earliest_range_delete_which_can_see_iter_key =
                del_iter.earliest_delete_which_can_see_key(iter_key.user_key, epoch);

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
                iter.next().await?;
                continue;
            }

            if value.is_delete() {
                user_key_last_delete_epoch = epoch;
            } else if earliest_range_delete_which_can_see_iter_key < user_key_last_delete_epoch {
                debug_assert!(
                    iter_key.epoch < earliest_range_delete_which_can_see_iter_key
                        && earliest_range_delete_which_can_see_iter_key
                            < user_key_last_delete_epoch
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
                    .await?;
                iter_key.epoch = epoch;
                is_new_user_key = false;
            }

            // Don't allow two SSTs to share same user key
            sst_builder
                .add_full_key(iter_key, value, is_new_user_key)
                .await?;

            iter.next().await?;
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
}

impl Compactor {
    /// Create a new compactor.
    pub fn new(
        context: Arc<CompactorContext>,
        options: SstableBuilderOptions,
        task_config: TaskConfig,
    ) -> Self {
        Self {
            context,
            options,
            task_config,
            get_id_time: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Compact the given key range and merge iterator.
    /// Upon a successful return, the built SSTs are already uploaded to object store.
    ///
    /// `task_progress` is only used for tasks on the compactor.
    async fn compact_key_range(
        &self,
        iter: impl HummockIterator<Direction = Forward>,
        compaction_filter: impl CompactionFilter,
        del_agg: Arc<CompactionDeleteRanges>,
        filter_key_extractor: Arc<FilterKeyExtractorImpl>,
        task_progress: Option<Arc<TaskProgress>>,
        task_id: Option<HummockCompactionTaskId>,
        split_index: Option<usize>,
    ) -> HummockResult<(Vec<LocalSstableInfo>, CompactionStatistics)> {
        // Monitor time cost building shared buffer to SSTs.
        let compact_timer = if self.context.is_share_buffer_compact {
            self.context
                .compactor_metrics
                .write_build_l0_sst_duration
                .start_timer()
        } else {
            self.context
                .compactor_metrics
                .compact_sst_duration
                .start_timer()
        };

        let (split_table_outputs, table_stats_map) = if self.options.capacity as u64
            > self.context.storage_opts.min_sst_size_for_streaming_upload
        {
            let factory = StreamingSstableWriterFactory::new(self.context.sstable_store.clone());
            if self.task_config.is_target_l0_or_lbase {
                self.compact_key_range_impl::<_, Xor16FilterBuilder>(
                    factory,
                    iter,
                    compaction_filter,
                    del_agg,
                    filter_key_extractor,
                    task_progress.clone(),
                )
                .await?
            } else {
                self.compact_key_range_impl::<_, Xor8FilterBuilder>(
                    factory,
                    iter,
                    compaction_filter,
                    del_agg,
                    filter_key_extractor,
                    task_progress.clone(),
                )
                .await?
            }
        } else {
            let factory = BatchSstableWriterFactory::new(self.context.sstable_store.clone());
            if self.task_config.is_target_l0_or_lbase {
                self.compact_key_range_impl::<_, Xor16FilterBuilder>(
                    factory,
                    iter,
                    compaction_filter,
                    del_agg,
                    filter_key_extractor,
                    task_progress.clone(),
                )
                .await?
            } else {
                self.compact_key_range_impl::<_, Xor8FilterBuilder>(
                    factory,
                    iter,
                    compaction_filter,
                    del_agg,
                    filter_key_extractor,
                    task_progress.clone(),
                )
                .await?
            }
        };

        compact_timer.observe_duration();

        let mut ssts = Vec::with_capacity(split_table_outputs.len());
        let mut upload_join_handles = vec![];

        for SplitTableOutput {
            sst_info,
            upload_join_handle,
        } in split_table_outputs
        {
            let sst_size = sst_info.file_size();
            ssts.push(sst_info);

            let tracker_cloned = task_progress.clone();
            let context_cloned = self.context.clone();
            upload_join_handles.push(async move {
                upload_join_handle
                    .await
                    .map_err(HummockError::sstable_upload_error)??;
                if let Some(tracker) = tracker_cloned {
                    tracker.inc_ssts_uploaded();
                }
                if context_cloned.is_share_buffer_compact {
                    context_cloned
                        .compactor_metrics
                        .shared_buffer_to_sstable_size
                        .observe(sst_size as _);
                } else {
                    context_cloned
                        .compactor_metrics
                        .compaction_upload_sst_counts
                        .inc();
                }
                Ok::<_, HummockError>(())
            });
        }

        // Check if there are any failed uploads. Report all of those SSTs.
        try_join_all(upload_join_handles).await?;
        self.context
            .compactor_metrics
            .get_table_id_total_time_duration
            .observe(self.get_id_time.load(Ordering::Relaxed) as f64 / 1000.0 / 1000.0);

        debug_assert!(ssts
            .iter()
            .all(|table_info| table_info.sst_info.get_table_ids().is_sorted()));

        if task_id.is_some() {
            // skip shared buffer compaction
            tracing::info!(
                "Finish Task {:?} split_index {:?} sst count {}",
                task_id,
                split_index,
                ssts.len()
            );
        }
        Ok((ssts, table_stats_map))
    }

    async fn compact_key_range_impl<F: SstableWriterFactory, B: FilterBuilder>(
        &self,
        writer_factory: F,
        iter: impl HummockIterator<Direction = Forward>,
        compaction_filter: impl CompactionFilter,
        del_agg: Arc<CompactionDeleteRanges>,
        filter_key_extractor: Arc<FilterKeyExtractorImpl>,
        task_progress: Option<Arc<TaskProgress>>,
    ) -> HummockResult<(Vec<SplitTableOutput>, CompactionStatistics)> {
        let builder_factory = RemoteBuilderFactory::<F, B> {
            sstable_object_id_manager: self.context.sstable_object_id_manager.clone(),
            limiter: self.context.output_memory_limiter.clone(),
            options: self.options.clone(),
            policy: self.task_config.cache_policy,
            remote_rpc_cost: self.get_id_time.clone(),
            filter_key_extractor,
            sstable_writer_factory: writer_factory,
            _phantom: PhantomData,
        };

        let mut sst_builder = CapacitySplitTableBuilder::new(
            builder_factory,
            self.context.compactor_metrics.clone(),
            task_progress.clone(),
            del_agg,
            self.task_config.key_range.clone(),
            self.task_config.is_target_l0_or_lbase,
            self.task_config.split_by_table,
            self.task_config.split_weight_by_vnode,
        );
        let compaction_statistics = Compactor::compact_and_build_sst(
            &mut sst_builder,
            &self.task_config,
            self.context.compactor_metrics.clone(),
            iter,
            compaction_filter,
            task_progress,
        )
        .await?;

        let ssts = sst_builder.finish().await?;

        Ok((ssts, compaction_statistics))
    }
}

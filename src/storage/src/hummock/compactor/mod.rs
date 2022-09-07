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

mod compaction_executor;
mod compaction_filter;
mod compactor_runner;
mod context;
mod iterator;
mod shared_buffer_compact;
mod sstable_store;

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::BytesMut;
pub use compaction_executor::CompactionExecutor;
pub use compaction_filter::{
    CompactionFilter, DummyCompactionFilter, MultiCompactionFilter, StateCleanUpCompactionFilter,
    TTLCompactionFilter,
};
pub use context::{CompactorContext, Context, TaskProgressTracker};
use futures::future::try_join_all;
use futures::{stream, StreamExt, TryFutureExt};
pub use iterator::ConcatSstableIterator;
use itertools::Itertools;
use risingwave_common::config::constant::hummock::CompactionFilterFlag;
use risingwave_hummock_sdk::compact::compact_task_to_string;
use risingwave_hummock_sdk::filter_key_extractor::FilterKeyExtractorImpl;
use risingwave_hummock_sdk::key::{get_epoch, FullKey};
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::VersionedComparator;
use risingwave_pb::hummock::compact_task::TaskStatus;
use risingwave_pb::hummock::subscribe_compact_tasks_response::Task;
use risingwave_pb::hummock::{
    CompactTask, CompactTaskProgress, LevelType, SstableInfo, SubscribeCompactTasksResponse,
};
use risingwave_rpc_client::HummockMetaClient;
pub use shared_buffer_compact::compact;
pub use sstable_store::{
    CompactorMemoryCollector, CompactorSstableStore, CompactorSstableStoreRef,
};
use tokio::sync::oneshot::{Receiver, Sender};
use tokio::task::JoinHandle;

use super::multi_builder::CapacitySplitTableBuilder;
use super::{HummockResult, SstableBuilderOptions, SstableWriterOptions};
use crate::hummock::compactor::compactor_runner::CompactorRunner;
use crate::hummock::iterator::{Forward, HummockIterator};
use crate::hummock::multi_builder::{SplitTableOutput, TableBuilderFactory};
use crate::hummock::utils::MemoryLimiter;
use crate::hummock::vacuum::Vacuum;
use crate::hummock::{
    validate_ssts, BatchSstableWriterFactory, CachePolicy, HummockError, SstableBuilder,
    SstableIdManagerRef, SstableWriterFactory, StreamingSstableWriterFactory,
};
use crate::monitor::{StateStoreMetrics, StoreLocalStatistic};

pub struct RemoteBuilderFactory<F: SstableWriterFactory> {
    sstable_id_manager: SstableIdManagerRef,
    limiter: Arc<MemoryLimiter>,
    options: SstableBuilderOptions,
    policy: CachePolicy,
    remote_rpc_cost: Arc<AtomicU64>,
    filter_key_extractor: Arc<FilterKeyExtractorImpl>,
    sstable_writer_factory: F,
}

#[async_trait::async_trait]
impl<F: SstableWriterFactory> TableBuilderFactory for RemoteBuilderFactory<F> {
    type Writer = F::Writer;

    async fn open_builder(&self) -> HummockResult<SstableBuilder<Self::Writer>> {
        // TODO: memory consumption may vary based on `SstableWriter`, `ObjectStore` and cache
        let tracker = self
            .limiter
            .require_memory(
                (self.options.capacity
                    + self.options.block_capacity
                    + self.options.estimate_bloom_filter_capacity) as u64,
            )
            .await
            .unwrap();
        let timer = Instant::now();
        let table_id = self.sstable_id_manager.get_new_sst_id().await?;
        let cost = (timer.elapsed().as_secs_f64() * 1000000.0).round() as u64;
        self.remote_rpc_cost.fetch_add(cost, Ordering::Relaxed);
        let writer_options = SstableWriterOptions {
            capacity_hint: Some(self.options.capacity + self.options.block_capacity),
            tracker: Some(tracker),
            policy: self.policy,
        };
        let writer = self
            .sstable_writer_factory
            .create_sst_writer(table_id, writer_options)
            .await?;
        let builder = SstableBuilder::new(
            table_id,
            writer,
            self.options.clone(),
            self.filter_key_extractor.clone(),
        );
        Ok(builder)
    }
}

#[derive(Clone)]
pub struct TaskConfig {
    pub key_range: KeyRange,
    pub cache_policy: CachePolicy,
    pub gc_delete_keys: bool,
    pub watermark: u64,
}

#[derive(Clone)]
/// Implementation of Hummock compaction.
pub struct Compactor {
    /// The context of the compactor.
    context: Arc<Context>,
    task_config: TaskConfig,
    options: SstableBuilderOptions,
}

pub type CompactOutput = (usize, Vec<SstableInfo>);

impl Compactor {
    /// Handles a compaction task and reports its status to hummock manager.
    /// Always return `Ok` and let hummock manager handle errors.
    pub async fn compact(
        compactor_context: Arc<CompactorContext>,
        mut compact_task: CompactTask,
        mut shutdown_rx: Receiver<()>,
    ) -> TaskStatus {
        let context = compactor_context.context.clone();
        // Set a watermark SST id to prevent full GC from accidentally deleting SSTs for in-progress
        // write op. The watermark is invalidated when this method exits.
        let tracker_id = match context.sstable_id_manager.add_watermark_sst_id(None).await {
            Ok(tracker_id) => tracker_id,
            Err(err) => {
                tracing::warn!("Failed to track pending SST id. {:#?}", err);
                return TaskStatus::Failed;
            }
        };
        let sstable_id_manager_clone = context.sstable_id_manager.clone();
        let _guard = scopeguard::guard(
            (tracker_id, sstable_id_manager_clone),
            |(tracker_id, sstable_id_manager)| {
                sstable_id_manager.remove_watermark_sst_id(tracker_id);
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
        context
            .stats
            .compact_read_current_level
            .with_label_values(&[group_label.as_str(), cur_level_label.as_str()])
            .inc_by(
                select_table_infos
                    .iter()
                    .map(|table| table.file_size)
                    .sum::<u64>(),
            );
        context
            .stats
            .compact_read_sstn_current_level
            .with_label_values(&[group_label.as_str(), cur_level_label.as_str()])
            .inc_by(select_table_infos.len() as u64);

        let sec_level_read_bytes = target_table_infos.iter().map(|t| t.file_size).sum::<u64>();
        let next_level_label = compact_task.target_level.to_string();
        context
            .stats
            .compact_read_next_level
            .with_label_values(&[group_label.as_str(), next_level_label.as_str()])
            .inc_by(sec_level_read_bytes);
        context
            .stats
            .compact_read_sstn_next_level
            .with_label_values(&[group_label.as_str(), next_level_label.as_str()])
            .inc_by(target_table_infos.len() as u64);

        let timer = context
            .stats
            .compact_task_duration
            .with_label_values(&[compact_task.input_ssts[0].level_idx.to_string().as_str()])
            .start_timer();

        let need_quota = estimate_memory_use_for_compaction(&compact_task);
        tracing::info!(
            "Ready to handle compaction task: {} need memory: {}",
            compact_task.task_id,
            need_quota
        );

        let multi_filter = build_multi_compaction_filter(&compact_task);

        let multi_filter_key_extractor = context
            .filter_key_extractor_manager
            .acquire(HashSet::from_iter(compact_task.existing_table_ids.clone()))
            .await;
        let multi_filter_key_extractor = Arc::new(multi_filter_key_extractor);

        // Number of splits (key ranges) is equal to number of compaction tasks
        let parallelism = compact_task.splits.len();
        assert_ne!(parallelism, 0, "splits cannot be empty");
        context.stats.compact_task_pending_num.inc();
        let mut task_status = TaskStatus::Success;
        let mut output_ssts = Vec::with_capacity(parallelism);
        let mut compaction_futures = vec![];

        for (split_index, _) in compact_task.splits.iter().enumerate() {
            let filter = multi_filter.clone();
            let multi_filter_key_extractor = multi_filter_key_extractor.clone();
            let compactor_runner = CompactorRunner::new(
                split_index,
                compactor_context.as_ref(),
                compact_task.clone(),
            );
            let handle = tokio::spawn(async move {
                compactor_runner
                    .run(filter, multi_filter_key_extractor)
                    .await
            });
            compaction_futures.push(handle);
        }

        let mut buffered = stream::iter(compaction_futures).buffer_unordered(parallelism);
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => {
                    tracing::warn!("Compaction task cancelled externally:\n{}", compact_task_to_string(&compact_task));
                    task_status = TaskStatus::Failed;
                    break;
                }
                future_result = buffered.next() => {
                    match future_result {
                        Some(Ok(Ok((split_index, ssts)))) => {
                            output_ssts.push((split_index, ssts));
                        }
                        Some(Ok(Err(e))) => {
                            task_status = TaskStatus::Failed;
                            tracing::warn!(
                                "Compaction task {} failed with error: {:#?}",
                                compact_task.task_id,
                                e
                            );
                            break;
                        }
                        Some(Err(e)) => {
                            task_status = TaskStatus::Failed;
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

        // Sort by split/key range index.
        output_ssts.sort_by_key(|(split_index, _)| *split_index);

        sync_point::on("BEFORE_COMPACT_REPORT").await;
        // After a compaction is done, mutate the compaction task.
        Self::compact_done(&mut compact_task, context.clone(), output_ssts, task_status).await;
        sync_point::on("AFTER_COMPACT_REPORT").await;
        let cost_time = timer.stop_and_record() * 1000.0;
        tracing::info!(
            "Finished compaction task in {:?}ms: \n{}",
            cost_time,
            compact_task_to_string(&compact_task)
        );
        context.stats.compact_task_pending_num.dec();
        for level in &compact_task.input_ssts {
            for table in &level.table_infos {
                context.sstable_store.delete_cache(table.id);
            }
        }
        task_status
    }

    /// Fill in the compact task and let hummock manager know the compaction output ssts.
    async fn compact_done(
        compact_task: &mut CompactTask,
        context: Arc<Context>,
        output_ssts: Vec<CompactOutput>,
        task_status: TaskStatus,
    ) {
        compact_task.set_task_status(task_status);
        compact_task
            .sorted_output_ssts
            .reserve(compact_task.splits.len());
        let mut compaction_write_bytes = 0;
        for (_, ssts) in output_ssts {
            for sst_info in ssts {
                compaction_write_bytes += sst_info.file_size;
                compact_task.sorted_output_ssts.push(sst_info);
            }
        }

        let group_label = compact_task.compaction_group_id.to_string();
        let level_label = compact_task.target_level.to_string();
        context
            .stats
            .compact_write_bytes
            .with_label_values(&[group_label.as_str(), level_label.as_str()])
            .inc_by(compaction_write_bytes);
        context
            .stats
            .compact_write_sstn
            .with_label_values(&[group_label.as_str(), level_label.as_str()])
            .inc_by(compact_task.sorted_output_ssts.len() as u64);

        if let Err(e) = context
            .hummock_meta_client
            .report_compaction_task(compact_task.clone())
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
    pub fn start_compactor(
        compactor_context: Arc<CompactorContext>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        max_concurrent_task_number: u64,
    ) -> (JoinHandle<()>, Sender<()>) {
        type CompactionShutdownMap = Arc<Mutex<HashMap<u64, Sender<()>>>>;
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        let stream_retry_interval = Duration::from_secs(60);
        let task_progress = compactor_context.context.task_progress.clone();
        let task_progress_update_interval = Duration::from_millis(1000);
        let join_handle = tokio::spawn(async move {
            let shutdown_map = CompactionShutdownMap::default();
            let mut min_interval = tokio::time::interval(stream_retry_interval);
            let mut task_progress_interval = tokio::time::interval(task_progress_update_interval);
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
                    .subscribe_compact_tasks(max_concurrent_task_number)
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
                let executor = compactor_context.context.compaction_executor.clone();

                // This inner loop is to consume stream.
                'consume_stream: loop {
                    let message = tokio::select! {
                        _ = task_progress_interval.tick() => {
                            let mut progress_list = Vec::new();
                            #[allow(clippy::significant_drop_in_scrutinee)]
                            for (&task_id, progress) in task_progress.lock().unwrap().iter() {
                                progress_list.push(CompactTaskProgress {
                                    task_id,
                                    num_ssts_sealed: progress.num_ssts_sealed,
                                    num_ssts_uploaded: progress.num_ssts_uploaded,
                                });
                            }
                            if let Err(e) = hummock_meta_client.report_compaction_task_progress(progress_list).await {
                                // ignore any errors while trying to report task progress
                                tracing::warn!("Failed to report task progress. {e:?}");
                            }
                            continue;
                        }
                        message = stream.message() => {
                            message
                        },
                        // Shutdown compactor
                        _ = &mut shutdown_rx => {
                            tracing::info!("Compactor is shutting down");
                            return
                        }
                    };
                    match message {
                        // The inner Some is the side effect of generated code.
                        Ok(Some(SubscribeCompactTasksResponse { task })) => {
                            let task = match task {
                                Some(task) => task,
                                None => continue 'consume_stream,
                            };

                            let shutdown = shutdown_map.clone();
                            let context = compactor_context.clone();
                            let meta_client = hummock_meta_client.clone();
                            executor.execute(async move {
                                match task {
                                    Task::CompactTask(compact_task) => {
                                        let (tx, rx) = tokio::sync::oneshot::channel();
                                        let task_id = compact_task.task_id;
                                        shutdown
                                            .lock()
                                            .unwrap()
                                            .insert(task_id, tx);
                                        Compactor::compact(context, compact_task, rx).await;
                                        shutdown.lock().unwrap().remove(&task_id);
                                    }
                                    Task::VacuumTask(vacuum_task) => {
                                        Vacuum::vacuum(
                                            vacuum_task,
                                            context.context.sstable_store.clone(),
                                            meta_client,
                                        )
                                        .await;
                                    }
                                    Task::FullScanTask(full_scan_task) => {
                                        Vacuum::full_scan(
                                            full_scan_task,
                                            context.context.sstable_store.clone(),
                                            meta_client,
                                        )
                                        .await;
                                    }
                                    Task::ValidationTask(validation_task) => {
                                        validate_ssts(
                                            validation_task,
                                            context.context.sstable_store.clone(),
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
                        Err(e) => {
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
        stats: Arc<StateStoreMetrics>,
        mut iter: impl HummockIterator<Direction = Forward>,
        mut compaction_filter: impl CompactionFilter,
    ) -> HummockResult<()>
    where
        F: TableBuilderFactory,
    {
        if !task_config.key_range.left.is_empty() {
            iter.seek(&task_config.key_range.left).await?;
        } else {
            iter.rewind().await?;
        }

        let mut last_key = BytesMut::new();
        let mut watermark_can_see_last_key = false;

        while iter.is_valid() {
            let iter_key = iter.key();

            let is_new_user_key =
                last_key.is_empty() || !VersionedComparator::same_user_key(iter_key, &last_key);

            let mut drop = false;
            let epoch = get_epoch(iter_key);
            if is_new_user_key {
                if !task_config.key_range.right.is_empty()
                    && VersionedComparator::compare_key(iter_key, &task_config.key_range.right)
                        != std::cmp::Ordering::Less
                {
                    break;
                }

                last_key.clear();
                last_key.extend_from_slice(iter_key);
                watermark_can_see_last_key = false;
            }

            // Among keys with same user key, only retain keys which satisfy `epoch` >= `watermark`.
            // If there is no keys whose epoch is equal or greater than `watermark`, keep the latest key which
            // satisfies `epoch` < `watermark`
            // in our design, frontend avoid to access keys which had be deleted, so we dont
            // need to consider the epoch when the compaction_filter match (it
            // means that mv had drop)
            if (epoch <= task_config.watermark
                && task_config.gc_delete_keys
                && iter.value().is_delete())
                || (epoch < task_config.watermark && watermark_can_see_last_key)
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
                iter.next().await?;
                continue;
            }

            // Don't allow two SSTs to share same user key
            sst_builder
                .add_full_key(FullKey::from_slice(iter_key), iter.value(), is_new_user_key)
                .await?;

            iter.next().await?;
        }
        let mut local_stats = StoreLocalStatistic::default();
        iter.collect_local_statistic(&mut local_stats);
        local_stats.report(stats.as_ref());
        Ok(())
    }
}

impl Compactor {
    /// Create a new compactor.
    pub fn new(
        context: Arc<Context>,
        options: SstableBuilderOptions,
        key_range: KeyRange,
        cache_policy: CachePolicy,
        gc_delete_keys: bool,
        watermark: u64,
    ) -> Self {
        Self {
            context,
            options,
            task_config: TaskConfig {
                key_range,
                cache_policy,
                gc_delete_keys,
                watermark,
            },
        }
    }

    /// Compact the given key range and merge iterator.
    /// Upon a successful return, the built SSTs are already uploaded to object store.
    async fn compact_key_range(
        &self,
        iter: impl HummockIterator<Direction = Forward>,
        compaction_filter: impl CompactionFilter,
        filter_key_extractor: Arc<FilterKeyExtractorImpl>,
        task_progress_tracker: Option<TaskProgressTracker>,
    ) -> HummockResult<Vec<SstableInfo>> {
        let get_id_time = Arc::new(AtomicU64::new(0));
        // Monitor time cost building shared buffer to SSTs.
        let compact_timer = if self.context.is_share_buffer_compact {
            self.context.stats.write_build_l0_sst_duration.start_timer()
        } else {
            self.context.stats.compact_sst_duration.start_timer()
        };

        let split_table_outputs = if self.options.capacity as u64
            > self.context.options.min_sst_size_for_streaming_upload
        {
            self.compact_key_range_impl(
                StreamingSstableWriterFactory::new(self.context.sstable_store.clone()),
                iter,
                compaction_filter,
                filter_key_extractor,
                get_id_time.clone(),
                task_progress_tracker.clone(),
            )
            .await?
        } else {
            self.compact_key_range_impl(
                BatchSstableWriterFactory::new(self.context.sstable_store.clone()),
                iter,
                compaction_filter,
                filter_key_extractor,
                get_id_time.clone(),
                task_progress_tracker.clone(),
            )
            .await?
        };

        compact_timer.observe_duration();

        let mut ssts = Vec::with_capacity(split_table_outputs.len());
        let mut upload_join_handles = vec![];

        for SplitTableOutput {
            bloom_filter_size,
            sst_info,
            upload_join_handle,
        } in split_table_outputs
        {
            // Bloom filter occuppy per thousand keys.
            self.context
                .filter_key_extractor_manager
                .update_bloom_filter_avg_size(sst_info.file_size as usize, bloom_filter_size);
            let sst_size = sst_info.file_size;
            ssts.push(sst_info);

            let tracker_cloned = task_progress_tracker.clone();
            upload_join_handles.push(upload_join_handle.and_then(|_| async move {
                if let Some(tracker) = tracker_cloned {
                    tracker.inc_ssts_uploaded();
                }
                Ok(())
            }));

            if self.context.is_share_buffer_compact {
                self.context
                    .stats
                    .shared_buffer_to_sstable_size
                    .observe(sst_size as _);
            } else {
                self.context.stats.compaction_upload_sst_counts.inc();
            }
        }

        try_join_all(upload_join_handles)
            .await
            .map_err(|e| HummockError::other(format!("fail to upload sst data: {:?}", e)))?;

        self.context
            .stats
            .get_table_id_total_time_duration
            .observe(get_id_time.load(Ordering::Relaxed) as f64 / 1000.0 / 1000.0);
        Ok(ssts)
    }

    async fn compact_key_range_impl<F: SstableWriterFactory>(
        &self,
        writer_factory: F,
        iter: impl HummockIterator<Direction = Forward>,
        compaction_filter: impl CompactionFilter,
        filter_key_extractor: Arc<FilterKeyExtractorImpl>,
        get_id_time: Arc<AtomicU64>,
        task_progress_tracker: Option<TaskProgressTracker>,
    ) -> HummockResult<Vec<SplitTableOutput>> {
        let builder_factory = RemoteBuilderFactory {
            sstable_id_manager: self.context.sstable_id_manager.clone(),
            limiter: self.context.read_memory_limiter.clone(),
            options: self.options.clone(),
            policy: self.task_config.cache_policy,
            remote_rpc_cost: get_id_time,
            filter_key_extractor,
            sstable_writer_factory: writer_factory,
        };

        let mut sst_builder = CapacitySplitTableBuilder::new(
            builder_factory,
            self.context.stats.clone(),
            task_progress_tracker,
        );
        Compactor::compact_and_build_sst(
            &mut sst_builder,
            &self.task_config,
            self.context.stats.clone(),
            iter,
            compaction_filter,
        )
        .await?;
        sst_builder.finish()
    }
}

pub fn estimate_memory_use_for_compaction(task: &CompactTask) -> u64 {
    let mut total_memory_size = 0;
    for level in &task.input_ssts {
        if level.level_type == LevelType::Nonoverlapping as i32 {
            if let Some(table) = level.table_infos.first() {
                total_memory_size += table.file_size * task.splits.len() as u64;
            }
        } else {
            for table in &level.table_infos {
                total_memory_size += table.file_size;
            }
        }
    }
    total_memory_size
}

fn build_multi_compaction_filter(compact_task: &CompactTask) -> MultiCompactionFilter {
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

        let ttl_filter = Box::new(TTLCompactionFilter::new(
            id_to_ttl,
            compact_task.current_epoch_time,
        ));
        multi_filter.register(ttl_filter);
    }

    multi_filter
}

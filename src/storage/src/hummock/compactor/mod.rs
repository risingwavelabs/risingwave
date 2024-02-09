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
use risingwave_pb::compactor::{dispatch_compaction_task_request, DispatchCompactionTaskRequest};
use risingwave_pb::hummock::report_compaction_task_request::{
    Event as ReportCompactionTaskEvent, HeartBeat as SharedHeartBeat,
    ReportTask as ReportSharedTask,
};
use risingwave_pb::hummock::{ReportFullScanTaskRequest, ReportVacuumTaskRequest};
use risingwave_rpc_client::GrpcCompactorProxyClient;
use tokio::sync::mpsc;
use tonic::Request;

pub mod compactor_runner;
mod context;
pub mod fast_compactor_runner;
mod iterator;
mod shared_buffer_compact;
pub(super) mod task_progress;

use std::collections::{HashMap, VecDeque};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use await_tree::InstrumentAwait;
pub use compaction_executor::CompactionExecutor;
pub use compaction_filter::{
    CompactionFilter, DummyCompactionFilter, MultiCompactionFilter, StateCleanUpCompactionFilter,
    TtlCompactionFilter,
};
pub use context::CompactorContext;
use futures::{pin_mut, StreamExt};
pub use iterator::{ConcatSstableIterator, SstableStreamIterator};
use more_asserts::assert_ge;
use risingwave_hummock_sdk::table_stats::to_prost_table_stats_map;
use risingwave_hummock_sdk::{HummockCompactionTaskId, LocalSstableInfo};
use risingwave_pb::hummock::compact_task::TaskStatus;
use risingwave_pb::hummock::subscribe_compaction_event_request::{
    Event as RequestEvent, HeartBeat, PullTask, ReportTask,
};
use risingwave_pb::hummock::subscribe_compaction_event_response::Event as ResponseEvent;
use risingwave_pb::hummock::{
    CompactTaskProgress, ReportCompactionTaskRequest, SubscribeCompactionEventRequest,
    SubscribeCompactionEventResponse,
};
use risingwave_rpc_client::HummockMetaClient;
pub use shared_buffer_compact::{compact, merge_imms_in_memory};
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
use tokio::time::Instant;

pub use self::compaction_utils::{CompactionStatistics, RemoteBuilderFactory, TaskConfig};
pub use self::task_progress::TaskProgress;
use super::multi_builder::CapacitySplitTableBuilder;
use super::{
    CompactionDeleteRanges, GetObjectId, HummockResult, SstableBuilderOptions,
    SstableObjectIdManager, Xor16FilterBuilder,
};
use crate::filter_key_extractor::{
    FilterKeyExtractorImpl, FilterKeyExtractorManager, StaticFilterKeyExtractorManager,
};
use crate::hummock::compactor::compactor_runner::compact_and_build_sst;
use crate::hummock::iterator::{Forward, HummockIterator};
use crate::hummock::multi_builder::SplitTableOutput;
use crate::hummock::vacuum::Vacuum;
use crate::hummock::{
    validate_ssts, BatchSstableWriterFactory, BlockedXor16FilterBuilder, FilterBuilder,
    HummockError, SharedComapctorObjectIdManager, SstableWriterFactory,
    StreamingSstableWriterFactory,
};
use crate::monitor::CompactorMetrics;

/// Implementation of Hummock compaction.
pub struct Compactor {
    /// The context of the compactor.
    context: CompactorContext,
    object_id_getter: Box<dyn GetObjectId>,
    task_config: TaskConfig,
    options: SstableBuilderOptions,
    get_id_time: Arc<AtomicU64>,
}

pub type CompactOutput = (usize, Vec<LocalSstableInfo>, CompactionStatistics);

impl Compactor {
    /// Create a new compactor.
    pub fn new(
        context: CompactorContext,
        options: SstableBuilderOptions,
        task_config: TaskConfig,
        object_id_getter: Box<dyn GetObjectId>,
    ) -> Self {
        Self {
            context,
            options,
            task_config,
            get_id_time: Arc::new(AtomicU64::new(0)),
            object_id_getter,
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

        let (split_table_outputs, table_stats_map) = if self
            .context
            .sstable_store
            .store()
            .support_streaming_upload()
        {
            let factory = StreamingSstableWriterFactory::new(self.context.sstable_store.clone());
            if self.task_config.use_block_based_filter {
                self.compact_key_range_impl::<_, BlockedXor16FilterBuilder>(
                    factory,
                    iter,
                    compaction_filter,
                    del_agg,
                    filter_key_extractor,
                    task_progress.clone(),
                    self.object_id_getter.clone(),
                )
                .verbose_instrument_await("compact")
                .await?
            } else {
                self.compact_key_range_impl::<_, Xor16FilterBuilder>(
                    factory,
                    iter,
                    compaction_filter,
                    del_agg,
                    filter_key_extractor,
                    task_progress.clone(),
                    self.object_id_getter.clone(),
                )
                .verbose_instrument_await("compact")
                .await?
            }
        } else {
            let factory = BatchSstableWriterFactory::new(self.context.sstable_store.clone());
            if self.task_config.use_block_based_filter {
                self.compact_key_range_impl::<_, BlockedXor16FilterBuilder>(
                    factory,
                    iter,
                    compaction_filter,
                    del_agg,
                    filter_key_extractor,
                    task_progress.clone(),
                    self.object_id_getter.clone(),
                )
                .verbose_instrument_await("compact")
                .await?
            } else {
                self.compact_key_range_impl::<_, Xor16FilterBuilder>(
                    factory,
                    iter,
                    compaction_filter,
                    del_agg,
                    filter_key_extractor,
                    task_progress.clone(),
                    self.object_id_getter.clone(),
                )
                .verbose_instrument_await("compact")
                .await?
            }
        };

        compact_timer.observe_duration();

        let ssts = Self::report_progress(
            self.context.compactor_metrics.clone(),
            task_progress,
            split_table_outputs,
            self.context.is_share_buffer_compact,
        )
        .await?;

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

    pub async fn report_progress(
        metrics: Arc<CompactorMetrics>,
        task_progress: Option<Arc<TaskProgress>>,
        split_table_outputs: Vec<SplitTableOutput>,
        is_share_buffer_compact: bool,
    ) -> HummockResult<Vec<LocalSstableInfo>> {
        let mut ssts = Vec::with_capacity(split_table_outputs.len());
        let mut rets = vec![];

        for SplitTableOutput {
            sst_info,
            upload_join_handle,
        } in split_table_outputs
        {
            let sst_size = sst_info.file_size();
            ssts.push(sst_info);
            let ret = upload_join_handle
                .verbose_instrument_await("upload")
                .await
                .map_err(HummockError::sstable_upload_error);
            rets.push(ret);
            if let Some(tracker) = &task_progress {
                tracker.inc_ssts_uploaded();
                tracker
                    .num_pending_write_io
                    .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            }
            if is_share_buffer_compact {
                metrics.shared_buffer_to_sstable_size.observe(sst_size as _);
            } else {
                metrics.compaction_upload_sst_counts.inc();
            }
        }
        for ret in rets {
            ret??;
        }
        Ok(ssts)
    }

    async fn compact_key_range_impl<F: SstableWriterFactory, B: FilterBuilder>(
        &self,
        writer_factory: F,
        iter: impl HummockIterator<Direction = Forward>,
        compaction_filter: impl CompactionFilter,
        del_agg: Arc<CompactionDeleteRanges>,
        filter_key_extractor: Arc<FilterKeyExtractorImpl>,
        task_progress: Option<Arc<TaskProgress>>,
        object_id_getter: Box<dyn GetObjectId>,
    ) -> HummockResult<(Vec<SplitTableOutput>, CompactionStatistics)> {
        let builder_factory = RemoteBuilderFactory::<F, B> {
            object_id_getter,
            limiter: self.context.memory_limiter.clone(),
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
            self.task_config.is_target_l0_or_lbase,
            self.task_config.split_by_table,
            self.task_config.split_weight_by_vnode,
        );
        let compaction_statistics = compact_and_build_sst(
            &mut sst_builder,
            del_agg,
            &self.task_config,
            self.context.compactor_metrics.clone(),
            iter,
            compaction_filter,
            task_progress,
        )
        .verbose_instrument_await("compact_and_build_sst")
        .await?;

        let ssts = sst_builder
            .finish()
            .verbose_instrument_await("builder_finish")
            .await?;

        Ok((ssts, compaction_statistics))
    }
}

/// The background compaction thread that receives compaction tasks from hummock compaction
/// manager and runs compaction tasks.
#[cfg_attr(coverage, coverage(off))]
pub fn start_compactor(
    compactor_context: CompactorContext,
    hummock_meta_client: Arc<dyn HummockMetaClient>,
    sstable_object_id_manager: Arc<SstableObjectIdManager>,
    filter_key_extractor_manager: FilterKeyExtractorManager,
) -> (JoinHandle<()>, Sender<()>) {
    type CompactionShutdownMap = Arc<Mutex<HashMap<u64, Sender<()>>>>;
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    let stream_retry_interval = Duration::from_secs(30);
    let task_progress = compactor_context.task_progress_manager.clone();
    let periodic_event_update_interval = Duration::from_millis(1000);
    let cpu_core_num = compactor_context.compaction_executor.worker_num() as u32;
    let running_task_count = compactor_context.running_task_count.clone();
    let pull_task_ack = Arc::new(AtomicBool::new(true));

    assert_ge!(
        compactor_context.storage_opts.compactor_max_task_multiplier,
        0.0
    );
    let max_pull_task_count = (cpu_core_num as f32
        * compactor_context.storage_opts.compactor_max_task_multiplier)
        .ceil() as u32;

    let join_handle = tokio::spawn(async move {
        let shutdown_map = CompactionShutdownMap::default();
        let mut min_interval = tokio::time::interval(stream_retry_interval);
        let mut periodic_event_interval = tokio::time::interval(periodic_event_update_interval);

        // This outer loop is to recreate stream.
        'start_stream: loop {
            // reset state
            pull_task_ack.store(true, Ordering::SeqCst);
            tokio::select! {
                // Wait for interval.
                _ = min_interval.tick() => {},
                // Shutdown compactor.
                _ = &mut shutdown_rx => {
                    tracing::info!("Compactor is shutting down");
                    return;
                }
            }

            let (request_sender, response_event_stream) =
                match hummock_meta_client.subscribe_compaction_event().await {
                    Ok((request_sender, response_event_stream)) => {
                        tracing::debug!("Succeeded subscribe_compaction_event.");
                        (request_sender, response_event_stream)
                    }

                    Err(e) => {
                        tracing::warn!(
                            "Subscribing to compaction tasks failed with error: {}. Will retry.",
                            e
                        );
                        continue 'start_stream;
                    }
                };

            pin_mut!(response_event_stream);

            let executor = compactor_context.compaction_executor.clone();
            let sstable_object_id_manager = sstable_object_id_manager.clone();

            // This inner loop is to consume stream or report task progress.
            let mut event_loop_iteration_now = Instant::now();
            'consume_stream: loop {
                {
                    // report
                    compactor_context
                        .compactor_metrics
                        .compaction_event_loop_iteration_latency
                        .observe(event_loop_iteration_now.elapsed().as_millis() as _);
                    event_loop_iteration_now = Instant::now();
                }

                let running_task_count = running_task_count.clone();
                let pull_task_ack = pull_task_ack.clone();
                let request_sender = request_sender.clone();
                let event: Option<Result<SubscribeCompactionEventResponse, _>> = tokio::select! {
                    _ = periodic_event_interval.tick() => {
                        let progress_list = get_task_progress(task_progress.clone());

                        if let Err(e) = request_sender.send(SubscribeCompactionEventRequest {
                            event: Some(RequestEvent::HeartBeat(
                                HeartBeat {
                                    progress: progress_list
                                }
                            )),
                            create_at: SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .expect("Clock may have gone backwards")
                                .as_millis() as u64,
                        }) {
                            tracing::warn!("Failed to report task progress. {e:?}");
                            // re subscribe stream
                            continue 'start_stream;
                        }


                        let mut pending_pull_task_count = 0;
                        if pull_task_ack.load(Ordering::SeqCst) {
                            // reset pending_pull_task_count when all pending task had been refill
                            pending_pull_task_count = {
                                // assert_ge!(max_pull_task_count, running_task_count.load(Ordering::SeqCst));
                                let running_task_count = running_task_count.load(Ordering::SeqCst);
                                if max_pull_task_count > running_task_count {
                                    max_pull_task_count - running_task_count
                                } else {
                                    0
                                }
                            };

                            if pending_pull_task_count > 0 {
                                if let Err(e) = request_sender.send(SubscribeCompactionEventRequest {
                                    event: Some(RequestEvent::PullTask(
                                        PullTask {
                                            pull_task_count: pending_pull_task_count,
                                        }
                                    )),
                                    create_at: SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .expect("Clock may have gone backwards")
                                        .as_millis() as u64,
                                }) {
                                    tracing::warn!("Failed to pull task {e:?}");

                                    // re subscribe stream
                                    continue 'start_stream;
                                } else {
                                    pull_task_ack.store(false, Ordering::SeqCst);
                                }
                            }
                        }

                        tracing::info!(
                            running_task_count = %running_task_count.load(Ordering::Relaxed),
                            pull_task_ack = %pull_task_ack.load(Ordering::Relaxed),
                            pending_pull_task_count = %pending_pull_task_count
                        );

                        continue;
                    }
                    event = response_event_stream.next() => {
                        event
                    }

                    _ = &mut shutdown_rx => {
                        tracing::info!("Compactor is shutting down");
                        return
                    }
                };

                match event {
                    Some(Ok(SubscribeCompactionEventResponse { event, create_at })) => {
                        let event = match event {
                            Some(event) => event,
                            None => continue 'consume_stream,
                        };
                        let shutdown = shutdown_map.clone();
                        let context = compactor_context.clone();
                        let consumed_latency_ms = SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .expect("Clock may have gone backwards")
                            .as_millis() as u64
                            - create_at;
                        context
                            .compactor_metrics
                            .compaction_event_consumed_latency
                            .observe(consumed_latency_ms as _);

                        let meta_client = hummock_meta_client.clone();
                        let sstable_object_id_manager = sstable_object_id_manager.clone();
                        let filter_key_extractor_manager = filter_key_extractor_manager.clone();
                        executor.spawn(async move {
                                let running_task_count = running_task_count.clone();
                                match event {
                                    ResponseEvent::CompactTask(compact_task)  => {
                                        running_task_count.fetch_add(1, Ordering::SeqCst);
                                        let (tx, rx) = tokio::sync::oneshot::channel();
                                        let task_id = compact_task.task_id;
                                        shutdown.lock().unwrap().insert(task_id, tx);
                                        let (compact_task, table_stats) = match sstable_object_id_manager.add_watermark_object_id(None).await
                                        {
                                            Ok(tracker_id) => {
                                                let sstable_object_id_manager_clone = sstable_object_id_manager.clone();
                                                let _guard = scopeguard::guard(
                                                    (tracker_id, sstable_object_id_manager_clone),
                                                    |(tracker_id, sstable_object_id_manager)| {
                                                        sstable_object_id_manager.remove_watermark_object_id(tracker_id);
                                                    },
                                                );
                                                compactor_runner::compact(context, compact_task, rx, Box::new(sstable_object_id_manager.clone()), filter_key_extractor_manager.clone()).await
                                            },
                                            Err(err) => {
                                                tracing::warn!("Failed to track pending SST object id. {:#?}", err);
                                                let mut compact_task = compact_task;
                                                // return TaskStatus::TrackSstObjectIdFailed;
                                                compact_task.set_task_status(TaskStatus::TrackSstObjectIdFailed);
                                                (compact_task, HashMap::default())
                                            }
                                        };
                                        shutdown.lock().unwrap().remove(&task_id);
                                        running_task_count.fetch_sub(1, Ordering::SeqCst);

                                        if let Err(e) = request_sender.send(SubscribeCompactionEventRequest {
                                            event: Some(RequestEvent::ReportTask(
                                                ReportTask {
                                                    task_id: compact_task.task_id,
                                                    task_status: compact_task.task_status,
                                                    sorted_output_ssts: compact_task.sorted_output_ssts,
                                                    table_stats_change:to_prost_table_stats_map(table_stats),
                                                }
                                            )),
                                            create_at: SystemTime::now()
                                                .duration_since(std::time::UNIX_EPOCH)
                                                .expect("Clock may have gone backwards")
                                                .as_millis() as u64,
                                        }) {
                                            tracing::warn!("Failed to report task {task_id:?} . {e:?}");
                                        }
                                    }
                                    ResponseEvent::VacuumTask(vacuum_task) => {
                                        match Vacuum::handle_vacuum_task(
                                            context.sstable_store.clone(),
                                            &vacuum_task.sstable_object_ids,
                                        )
                                        .await{
                                            Ok(_) => {
                                                Vacuum::report_vacuum_task(vacuum_task, meta_client).await;
                                            }
                                            Err(e) => {
                                                tracing::warn!("Failed to vacuum task: {:#?}", e)
                                            }
                                        }
                                    }
                                    ResponseEvent::FullScanTask(full_scan_task) => {
                                        match Vacuum::handle_full_scan_task(full_scan_task, context.sstable_store.clone()).await {
                                            Ok((object_ids, total_object_count, total_object_size)) => {
                                                Vacuum::report_full_scan_task(object_ids, total_object_count, total_object_size, meta_client).await;
                                            }
                                            Err(e) => {
                                                tracing::warn!("Failed to iter object: {:#?}", e);
                                            }
                                        }
                                    }
                                    ResponseEvent::ValidationTask(validation_task) => {
                                        validate_ssts(
                                            validation_task,
                                            context.sstable_store.clone(),
                                        )
                                        .await;
                                    }
                                    ResponseEvent::CancelCompactTask(cancel_compact_task) => {
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

                                    ResponseEvent::PullTaskAck(_pull_task_ack) => {
                                        // set flag
                                        pull_task_ack.store(true, Ordering::SeqCst);
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

/// The background compaction thread that receives compaction tasks from hummock compaction
/// manager and runs compaction tasks.
#[cfg_attr(coverage, coverage(off))]
pub fn start_shared_compactor(
    grpc_proxy_client: GrpcCompactorProxyClient,
    mut receiver: mpsc::UnboundedReceiver<Request<DispatchCompactionTaskRequest>>,
    context: CompactorContext,
) -> (JoinHandle<()>, Sender<()>) {
    type CompactionShutdownMap = Arc<Mutex<HashMap<u64, Sender<()>>>>;
    let task_progress = context.task_progress_manager.clone();
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    let periodic_event_update_interval = Duration::from_millis(1000);

    let join_handle = tokio::spawn(async move {
        let shutdown_map = CompactionShutdownMap::default();

        let mut periodic_event_interval = tokio::time::interval(periodic_event_update_interval);
        let executor = context.compaction_executor.clone();
        let report_heartbeat_client = grpc_proxy_client.clone();
        'consume_stream: loop {
            let request: Option<Request<DispatchCompactionTaskRequest>> = tokio::select! {
                _ = periodic_event_interval.tick() => {
                    let progress_list = get_task_progress(task_progress.clone());
                    let report_compaction_task_request = ReportCompactionTaskRequest{
                        event: Some(ReportCompactionTaskEvent::HeartBeat(
                            SharedHeartBeat {
                                progress: progress_list
                            }
                        )),
                     };
                    if let Err(e) = report_heartbeat_client.report_compaction_task(report_compaction_task_request).await{
                        tracing::warn!("Failed to report heartbeat {:#?}", e);
                    }
                    continue
                }


                _ = &mut shutdown_rx => {
                    tracing::info!("Compactor is shutting down");
                    return
                }

                request = receiver.recv() => {
                    request
                }

            };
            match request {
                Some(request) => {
                    let context = context.clone();
                    let shutdown = shutdown_map.clone();

                    let cloned_grpc_proxy_client = grpc_proxy_client.clone();
                    executor.spawn(async move {
                        let DispatchCompactionTaskRequest {
                            tables,
                            output_object_ids,
                            task: dispatch_task,
                        } = request.into_inner();
                        let id_to_tables = tables.into_iter().fold(HashMap::new(), |mut acc, table| {
                            acc.insert(table.id, table);
                            acc
                        });
                        let static_filter_key_extractor_manager: Arc<StaticFilterKeyExtractorManager> =
                            Arc::new(StaticFilterKeyExtractorManager::new(id_to_tables));
                        let filter_key_extractor_manager =
                            FilterKeyExtractorManager::StaticFilterKeyExtractorManager(
                                static_filter_key_extractor_manager,
                            );

                        let mut output_object_ids_deque: VecDeque<_> = VecDeque::new();
                        output_object_ids_deque.extend(output_object_ids);
                        let shared_compactor_object_id_manager =
                            SharedComapctorObjectIdManager::new(output_object_ids_deque, cloned_grpc_proxy_client.clone(), context.storage_opts.sstable_id_remote_fetch_number);
                            match dispatch_task.unwrap() {
                                dispatch_compaction_task_request::Task::CompactTask(compact_task) => {
                                    context.running_task_count.fetch_add(1, Ordering::SeqCst);
                                    let (tx, rx) = tokio::sync::oneshot::channel();
                                    let task_id = compact_task.task_id;
                                    shutdown.lock().unwrap().insert(task_id, tx);

                                    let (compact_task, table_stats) = compactor_runner::compact(
                                        context.clone(),
                                        compact_task,
                                        rx,
                                        Box::new(shared_compactor_object_id_manager),
                                        filter_key_extractor_manager.clone(),
                                    )
                                    .await;
                                    shutdown.lock().unwrap().remove(&task_id);
                                    context.running_task_count.fetch_sub(1, Ordering::SeqCst);
                                    let report_compaction_task_request = ReportCompactionTaskRequest {
                                        event: Some(ReportCompactionTaskEvent::ReportTask(ReportSharedTask {
                                            compact_task: Some(compact_task),
                                            table_stats_change: to_prost_table_stats_map(table_stats),
                                        })),
                                    };

                                    match cloned_grpc_proxy_client
                                        .report_compaction_task(report_compaction_task_request)
                                        .await
                                    {
                                        Ok(_) => {}
                                        Err(e) => tracing::warn!("Failed to report task {task_id:?} . {e:?}"),
                                    }
                                }
                                dispatch_compaction_task_request::Task::VacuumTask(vacuum_task) => {
                                    match Vacuum::handle_vacuum_task(
                                        context.sstable_store.clone(),
                                        &vacuum_task.sstable_object_ids,
                                    )
                                    .await
                                    {
                                        Ok(_) => {
                                            let report_vacuum_task_request = ReportVacuumTaskRequest {
                                                vacuum_task: Some(vacuum_task),
                                            };
                                            match cloned_grpc_proxy_client.report_vacuum_task(report_vacuum_task_request).await {
                                                Ok(_) => tracing::info!("Finished vacuuming SSTs"),
                                                Err(e) => tracing::warn!("Failed to report vacuum task: {:#?}", e),
                                            }
                                        }
                                        Err(e) => {
                                            tracing::warn!("Failed to vacuum task: {:#?}", e)
                                        }
                                    }
                                }
                                dispatch_compaction_task_request::Task::FullScanTask(full_scan_task) => {
                                    match Vacuum::handle_full_scan_task(full_scan_task, context.sstable_store.clone())
                                        .await
                                    {
                                        Ok((object_ids, total_object_count, total_object_size)) => {
                                            let report_full_scan_task_request = ReportFullScanTaskRequest {
                                                object_ids,
                                                total_object_count,
                                                total_object_size,
                                            };
                                            match cloned_grpc_proxy_client
                                                .report_full_scan_task(report_full_scan_task_request)
                                                .await
                                            {
                                                Ok(_) => tracing::info!("Finished full scan SSTs"),
                                                Err(e) => tracing::warn!("Failed to report full scan task: {:#?}", e),
                                            }
                                        }
                                        Err(e) => {
                                            tracing::warn!("Failed to iter object: {:#?}", e);
                                        }
                                    }
                                }
                                dispatch_compaction_task_request::Task::ValidationTask(validation_task) => {
                                    validate_ssts(validation_task, context.sstable_store.clone()).await;
                                }
                                dispatch_compaction_task_request::Task::CancelCompactTask(cancel_compact_task) => {
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
                None => continue 'consume_stream,
            }
        }
    });
    (join_handle, shutdown_tx)
}

fn get_task_progress(
    task_progress: Arc<
        parking_lot::lock_api::Mutex<parking_lot::RawMutex, HashMap<u64, Arc<TaskProgress>>>,
    >,
) -> Vec<CompactTaskProgress> {
    let mut progress_list = Vec::new();
    for (&task_id, progress) in &*task_progress.lock() {
        progress_list.push(CompactTaskProgress {
            task_id,
            num_ssts_sealed: progress.num_ssts_sealed.load(Ordering::Relaxed),
            num_ssts_uploaded: progress.num_ssts_uploaded.load(Ordering::Relaxed),
            num_progress_key: progress.num_progress_key.load(Ordering::Relaxed),
            num_pending_read_io: progress.num_pending_read_io.load(Ordering::Relaxed) as u64,
            num_pending_write_io: progress.num_pending_write_io.load(Ordering::Relaxed) as u64,
        });
    }
    progress_list
}

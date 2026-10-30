// Copyright 2025 RisingWave Labs
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

use std::collections::VecDeque;
use std::time::Duration;

use either::Either;
use futures::stream;
use iceberg::scan::FileScanTask;
use itertools::Itertools;
use risingwave_common::array::Op;
use risingwave_common::catalog::{
    ICEBERG_FILE_PATH_COLUMN_NAME, ICEBERG_FILE_POS_COLUMN_NAME, ROW_ID_COLUMN_NAME,
};
use risingwave_common::config::StreamingConfig;
use risingwave_common::id::SourceId;
use risingwave_common::metrics::GLOBAL_ERROR_METRICS;
use risingwave_common::types::{Scalar, ScalarRef};
use risingwave_connector::source::ConnectorProperties;
use risingwave_connector::source::iceberg::{
    GLOBAL_ICEBERG_SCAN_METRICS, IcebergFileScanMetrics, IcebergScanMetricsLabels, IcebergScanOpts,
    PersistedFileScanTask, scan_task_to_chunk_with_deletes,
};
use risingwave_connector::source::reader::desc::SourceDesc;
use thiserror_ext::AsReport;

use super::batch_iceberg_state::{FileAssignments, FileEntry};
use super::{Backoff, LoadRetryPolicy, StartKind};
use crate::executor::LoadAction;
use crate::executor::prelude::*;
use crate::executor::source::{
    ChunksWithState, StreamSourceCore, pad_row_id, prune_additional_cols,
};
use crate::executor::stream_reader::StreamReaderWithPause;
use crate::task::{FragmentId, LoadProgress, LocalBarrierManager};

#[derive(PartialEq, Eq)]
enum FetchPhase {
    Idle,
    /// More file assignments may arrive from upstream.
    Loading,
    /// Listing is finished; read the remaining files.
    Draining,
    /// The load gave up after repeated errors.
    Failed,
}

struct FetchState {
    retry_policy: LoadRetryPolicy,
    phase: FetchPhase,

    /// Number of files currently being fetched in the active batch reader
    splits_on_fetch: usize,

    /// Queue of files waiting to be processed
    file_queue: VecDeque<FileEntry>,

    /// Files the current batch reader has not finished yet. On a fetch error they are re-queued;
    /// finished files are already marked done and are never read again.
    in_flight_files: Vec<FileEntry>,

    /// Delays retries after consecutive errors; once the backoff runs out, the load fails.
    backoff: Backoff,
}

impl FetchState {
    fn new(retry_policy: LoadRetryPolicy) -> Self {
        Self {
            retry_policy,
            phase: FetchPhase::Idle,
            splits_on_fetch: 0,
            file_queue: VecDeque::new(),
            in_flight_files: Vec::new(),
            backoff: retry_policy.backoff(),
        }
    }

    /// Clear previous work and start a new load from the persisted pending files.
    fn start_load(&mut self, pending: Vec<FileEntry>) {
        tracing::info!(
            "start_load: clearing file_queue_len={}, in_flight_files_len={}, splits_on_fetch={}, resuming pending_len={}",
            self.file_queue.len(),
            self.in_flight_files.len(),
            self.splits_on_fetch,
            pending.len()
        );
        self.replace_pending(pending);
        self.backoff = self.retry_policy.backoff();
        self.phase = FetchPhase::Loading;
    }

    fn replace_pending(&mut self, pending: Vec<FileEntry>) {
        self.file_queue = pending.into();
        self.in_flight_files.clear();
        self.splits_on_fetch = 0;
    }

    fn finish_listing(&mut self) {
        if self.phase == FetchPhase::Loading {
            self.phase = FetchPhase::Draining;
        }
    }

    /// Check if we should report completion to the barrier manager.
    fn should_report_finished(&self) -> bool {
        self.splits_on_fetch == 0
            && self.file_queue.is_empty()
            && self.in_flight_files.is_empty()
            && self.phase == FetchPhase::Draining
    }

    /// Return to idle once the load has reported its outcome.
    fn mark_idle(&mut self) {
        self.phase = FetchPhase::Idle;
    }

    /// Stop the load after repeated errors and drop the remaining files.
    fn mark_failed(&mut self) {
        self.file_queue.clear();
        self.in_flight_files.clear();
        self.splits_on_fetch = 0;
        self.phase = FetchPhase::Failed;
    }

    fn is_failed(&self) -> bool {
        self.phase == FetchPhase::Failed
    }

    /// Check if we should start a new batch reader.
    fn should_start_batch_reader(&self, need_rebuild: bool) -> bool {
        matches!(self.phase, FetchPhase::Loading | FetchPhase::Draining)
            && (need_rebuild || (self.splits_on_fetch == 0 && !self.file_queue.is_empty()))
    }

    /// Records that `path` was read in full and resets the backoff.
    fn mark_file_fetched(&mut self, path: &str) {
        self.splits_on_fetch -= 1;
        self.in_flight_files.retain(|(file, _)| file != path);
        self.backoff = self.retry_policy.backoff();
    }

    /// Handle fetch error with at-least-once recovery.
    /// Re-queues the unfinished in-flight files to ensure no file is skipped.
    fn handle_error_recovery(&mut self) {
        if !self.in_flight_files.is_empty() {
            // Re-queue in-flight files to the front (reverse to maintain original order)
            for file in self.in_flight_files.drain(..).rev() {
                self.file_queue.push_front(file);
            }
        }
        self.splits_on_fetch = 0;
    }

    /// Enqueue new file assignments from upstream.
    fn enqueue_files(&mut self, files: impl IntoIterator<Item = FileEntry>) {
        self.file_queue.extend(files);
    }
}

// ============================================================================
// Column Indices Helper
// ============================================================================

/// Indices of special columns that need to be pruned from output.
struct ColumnIndices {
    file_path_idx: usize,
    file_pos_idx: usize,
    /// Set for tables without a primary key
    row_id_idx: Option<usize>,
}

impl ColumnIndices {
    fn from_source_desc(source_desc: &SourceDesc) -> Self {
        let file_path_idx = source_desc
            .columns
            .iter()
            .position(|c| c.name == ICEBERG_FILE_PATH_COLUMN_NAME)
            .expect("file path column not found");
        let file_pos_idx = source_desc
            .columns
            .iter()
            .position(|c| c.name == ICEBERG_FILE_POS_COLUMN_NAME)
            .expect("file pos column not found");
        let row_id_idx = source_desc
            .columns
            .iter()
            .position(|c| c.name == ROW_ID_COLUMN_NAME);

        Self {
            file_path_idx,
            file_pos_idx,
            row_id_idx,
        }
    }

    fn to_prune(&self) -> [usize; 2] {
        [self.file_path_idx, self.file_pos_idx]
    }
}

// ============================================================================
// Batch Iceberg Fetch Executor
// ============================================================================

/// Executor that fetches data from Iceberg files discovered by an upstream list executor.
///
///
/// # Load
///
/// 1. Receives `Load { Start }`. A bounded load resumes from its persisted assignments; a full
///    reload clears them and starts over.
/// 2. Receives file chunks from the upstream list executor. Each file is persisted as pending and
///    queued, unless it is already assigned (pending or done).
/// 3. On each barrier, starts a batch reader if files are pending.
/// 4. Marks a file done in the same epoch that emits its rows, so each file is emitted once.
/// 5. Receives `Load { ListFinish }`, which marks listing as complete.
/// 6. When all queued files are read, reports `ReaderDone`.
///
/// # At-Least-Once Semantics
///
/// On fetch errors, unfinished in-flight files are re-queued to ensure no file is skipped.
/// A file that failed partway may be read again, but a file marked done is never re-read.
pub struct BatchIcebergFetchExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,

    /// Core component for managing external streaming source state
    stream_source_core: Option<StreamSourceCore<S>>,

    /// Upstream list executor that provides file scan tasks
    upstream: Option<Executor>,

    /// Barrier manager for reporting load finished
    barrier_manager: LocalBarrierManager,

    streaming_config: Arc<StreamingConfig>,

    retry_policy: LoadRetryPolicy,
    start_kind: StartKind,

    scan_metrics: Option<IcebergScanMetricsLabels>,
    file_scan_metrics: Option<IcebergFileScanMetrics>,
}

impl<S: StateStore> BatchIcebergFetchExecutor<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        actor_ctx: ActorContextRef,
        stream_source_core: StreamSourceCore<S>,
        upstream: Executor,
        barrier_manager: LocalBarrierManager,
        streaming_config: Arc<StreamingConfig>,
        retry_policy: LoadRetryPolicy,
        start_kind: StartKind,
    ) -> Self {
        Self {
            actor_ctx,
            stream_source_core: Some(stream_source_core),
            upstream: Some(upstream),
            barrier_manager,
            streaming_config,
            retry_policy,
            start_kind,
            scan_metrics: None,
            file_scan_metrics: None,
        }
    }
}

impl<S: StateStore> BatchIcebergFetchExecutor<S> {
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        // Initialize upstream and wait for first barrier
        let mut upstream = self.upstream.take().unwrap().execute();
        let first_barrier = expect_first_barrier(&mut upstream).await?;
        let pause_on_startup = first_barrier.is_pause_on_startup();
        let mut core = self.stream_source_core.take().unwrap();
        let mut assignments = FileAssignments::new(core.split_state_store);
        let first_epoch = first_barrier.epoch;
        yield Message::Barrier(first_barrier);
        assignments.handler.init_epoch(first_epoch).await?;

        // Initialize source description
        let source_desc = core
            .source_desc_builder
            .take()
            .unwrap()
            .build()
            .map_err(StreamExecutorError::connector_error)?;

        // Find column indices for pruning
        let column_indices = ColumnIndices::from_source_desc(&source_desc);

        // Initialize metrics context
        let iceberg_metrics = &GLOBAL_ICEBERG_SCAN_METRICS;
        let iceberg_table_name = match &source_desc.source.config {
            ConnectorProperties::Iceberg(props) => props.table.table_name().to_owned(),
            _ => unreachable!("BatchIcebergFetchExecutor must be built with Iceberg properties"),
        };
        let source_id_str = core.source_id.to_string();
        let source_name_str = core.source_name.clone();
        let scan_metrics = self
            .scan_metrics
            .insert(IcebergScanMetricsLabels::new(
                source_id_str,
                source_name_str,
                iceberg_table_name.clone(),
            ))
            .clone();
        let file_scan_metrics = self
            .file_scan_metrics
            .insert(IcebergFileScanMetrics::new(
                iceberg_metrics,
                &iceberg_table_name,
            ))
            .clone();

        // Initialize state and stream reader
        let mut state = FetchState::new(self.retry_policy);
        let mut stream = StreamReaderWithPause::<true, ChunksWithState>::new(
            upstream,
            stream::pending().boxed(),
        );

        if pause_on_startup {
            stream.pause_stream();
        }

        // Main processing loop
        while let Some(msg) = stream.next().await {
            match msg {
                // ----- Error Handling with At-Least-Once Recovery -----
                Err(e) => {
                    tracing::error!(error = %e.as_report(), "Fetch Error");

                    GLOBAL_ERROR_METRICS.user_source_error.report([
                        e.variant_name().to_owned(),
                        core.source_id.to_string(),
                        core.source_name.clone(),
                        self.actor_ctx.fragment_id.to_string(),
                    ]);

                    scan_metrics.record_fetch_error();

                    let Some(delay) = state.backoff.next() else {
                        tracing::error!(
                            source_id = %core.source_id,
                            fragment_id = %self.actor_ctx.fragment_id,
                            error = %e.as_report(),
                            "load failed after repeated fetch errors"
                        );
                        state.mark_failed();
                        stream.replace_data_stream(stream::pending().boxed());
                        continue;
                    };

                    let in_flight_count = state.in_flight_files.len();
                    state.handle_error_recovery();

                    if in_flight_count > 0 {
                        tracing::info!(
                            source_id = %core.source_id,
                            fragment_id = %self.actor_ctx.fragment_id,
                            in_flight_count = %in_flight_count,
                            "re-queued unfinished files for retry"
                        );
                    }

                    tracing::info!(
                        source_id = %core.source_id,
                        fragment_id = %self.actor_ctx.fragment_id,
                        remaining_files = %state.file_queue.len(),
                        ?delay,
                        "attempting to recover from fetch error, will retry after the delay"
                    );

                    Self::start_batch_reader(
                        &mut state,
                        &mut stream,
                        source_desc.clone(),
                        &self.streaming_config,
                        file_scan_metrics.clone(),
                        Some(delay),
                    )?;
                    scan_metrics.set_inflight_file_count(state.splits_on_fetch);

                    continue;
                }

                // ----- Upstream Messages (barriers, file assignments) -----
                Ok(Either::Left(msg)) => match msg {
                    Message::Barrier(barrier) => {
                        let mut need_rebuild = Self::handle_barrier_mutations(
                            &barrier,
                            core.source_id,
                            self.actor_ctx.fragment_id,
                            self.start_kind,
                            &mut state,
                            &mut stream,
                            &mut assignments,
                        )
                        .await?;

                        if barrier.is_checkpoint() && state.is_failed() {
                            self.barrier_manager.report_load_progress(
                                barrier.epoch,
                                self.actor_ctx.id,
                                core.source_id,
                                LoadProgress::Failed,
                            );
                            state.mark_idle();
                        }

                        if barrier.is_checkpoint()
                            && state.should_report_finished()
                            && barrier.as_update_vnode_bitmap(self.actor_ctx.id).is_none()
                        {
                            tracing::info!(
                                ?barrier.epoch,
                                actor_id = %self.actor_ctx.id,
                                source_id = %core.source_id,
                                fragment_id = %self.actor_ctx.fragment_id,
                                "Reporting load finished"
                            );
                            self.barrier_manager.report_load_progress(
                                barrier.epoch,
                                self.actor_ctx.id,
                                core.source_id,
                                LoadProgress::ReaderDone,
                            );
                            state.mark_idle();
                        }

                        let post_commit = assignments
                            .handler
                            .commit_may_update_vnode_bitmap(barrier.epoch)
                            .await?;
                        let update_vnodes = barrier.as_update_vnode_bitmap(self.actor_ctx.id);

                        yield Message::Barrier(barrier);

                        // Our vnodes changed mid-load: requeue from the pending files we now own,
                        // and restart the reader so it stops reading files that moved to another actor.
                        if post_commit
                            .post_yield_barrier(update_vnodes)
                            .await?
                            .is_some()
                            && matches!(state.phase, FetchPhase::Loading | FetchPhase::Draining)
                        {
                            state.replace_pending(assignments.pending().await?);
                            need_rebuild = true;
                        }

                        if state.should_start_batch_reader(need_rebuild) {
                            Self::start_batch_reader(
                                &mut state,
                                &mut stream,
                                source_desc.clone(),
                                &self.streaming_config,
                                file_scan_metrics.clone(),
                                None,
                            )?;

                            scan_metrics.set_inflight_file_count(state.splits_on_fetch);
                        }
                    }

                    Message::Chunk(chunk) => {
                        let files = Self::parse_file_assignments(&chunk);
                        tracing::debug!("Received {} file assignments from upstream", files.len());
                        for (path, task) in files {
                            if assignments.assign(&path, task.clone()).await? {
                                state.enqueue_files([(path, task)]);
                            }
                        }
                        assignments.handler.try_flush().await?;
                    }

                    Message::Watermark(_) => unreachable!(),
                },

                // ----- Fetched Data from Iceberg Files -----
                Ok(Either::Right(ChunksWithState {
                    chunks,
                    data_file_path,
                    ..
                })) => {
                    assignments.mark_done(&data_file_path).await?;
                    state.mark_file_fetched(&data_file_path);
                    tracing::debug!(file = %data_file_path, "finished batch iceberg file");

                    scan_metrics.set_inflight_file_count(state.splits_on_fetch);

                    for chunk in &chunks {
                        let pruned = prune_additional_cols(
                            chunk,
                            &column_indices.to_prune(),
                            &source_desc.columns,
                        );

                        let chunk = match column_indices.row_id_idx {
                            Some(row_id_idx) => pad_row_id(pruned, row_id_idx),
                            None => pruned,
                        };

                        yield Message::Chunk(chunk);
                    }
                }
            }
        }
    }

    /// Handle barrier mutations and return whether reader needs to be rebuilt.
    async fn handle_barrier_mutations(
        barrier: &Barrier,
        source_id: SourceId,
        fragment_id: FragmentId,
        start_kind: StartKind,
        state: &mut FetchState,
        stream: &mut StreamReaderWithPause<true, ChunksWithState>,
        assignments: &mut FileAssignments<S>,
    ) -> StreamExecutorResult<bool> {
        let Some(mutation) = barrier.mutation.as_deref() else {
            return Ok(false);
        };

        Ok(match mutation {
            Mutation::Pause => {
                stream.pause_stream();
                false
            }
            Mutation::Resume => {
                stream.resume_stream();
                false
            }
            Mutation::Load { target, action } if target.addresses(source_id, fragment_id) => {
                match action {
                    LoadAction::Start => {
                        tracing::info!(
                            ?barrier.epoch,
                            source_id = %source_id,
                            is_checkpoint = barrier.is_checkpoint(),
                            "load started"
                        );

                        if start_kind == StartKind::Reset {
                            assignments.clear().await?;
                        }

                        state.start_load(assignments.pending().await?);
                        stream.replace_data_stream(stream::pending().boxed());

                        true
                    }
                    LoadAction::ListFinish => {
                        tracing::info!(
                            ?barrier.epoch,
                            source_id = %source_id,
                            is_checkpoint = barrier.is_checkpoint(),
                            "listing finished, reading the remaining files"
                        );
                        state.finish_listing();
                        false
                    }
                }
            }
            _ => false,
        })
    }

    /// Parse file assignments from an upstream chunk.
    fn parse_file_assignments(chunk: &StreamChunk) -> Vec<FileEntry> {
        chunk
            .data_chunk()
            .rows()
            .map(|row| {
                let file_name = row.datum_at(0).unwrap().into_utf8().to_owned();
                let scan_task = row.datum_at(1).unwrap().into_jsonb().to_owned_scalar();
                (file_name, scan_task)
            })
            .collect()
    }

    /// Start a new batch reader for pending files.
    fn start_batch_reader(
        state: &mut FetchState,
        stream: &mut StreamReaderWithPause<true, ChunksWithState>,
        source_desc: SourceDesc,
        streaming_config: &StreamingConfig,
        file_scan_metrics: IcebergFileScanMetrics,
        delay: Option<Duration>,
    ) -> StreamExecutorResult<()> {
        // Clear previous in-flight files (should already be empty on success, re-queued on error)
        state.in_flight_files.clear();

        // Collect batch of files to process
        let batch_size = streaming_config.developer.iceberg_fetch_batch_size as usize;
        let mut batch = Vec::with_capacity(batch_size);

        for _ in 0..batch_size {
            let Some(file_entry) = state.file_queue.pop_front() else {
                break;
            };
            // Track as in-flight for at-least-once recovery
            state.in_flight_files.push(file_entry.clone());
            batch.push(PersistedFileScanTask::decode(file_entry.1.as_scalar_ref())?);
        }

        if batch.is_empty() {
            tracing::info!("Batch is empty, setting stream to pending");
            stream.replace_data_stream(stream::pending().boxed());
        } else {
            tracing::debug!("Starting batch reader with {} files", batch.len());
            state.splits_on_fetch += batch.len();

            let batch_reader = Self::build_batched_stream_reader(
                source_desc,
                batch,
                delay,
                streaming_config.developer.chunk_size,
                file_scan_metrics,
            );
            stream.replace_data_stream(batch_reader.boxed());
        }

        Ok(())
    }

    /// Build a stream reader that reads multiple Iceberg files in sequence, after `delay` if set.
    #[try_stream(ok = ChunksWithState, error = StreamExecutorError)]
    async fn build_batched_stream_reader(
        source_desc: SourceDesc,
        tasks: Vec<FileScanTask>,
        delay: Option<Duration>,
        chunk_size: usize,
        file_scan_metrics: IcebergFileScanMetrics,
    ) {
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }

        let properties = match source_desc.source.config.clone() {
            ConnectorProperties::Iceberg(props) => props,
            _ => unreachable!("Expected Iceberg connector properties"),
        };
        let table = properties.load_table().await?;

        for task in tasks {
            let data_file_path = task.data_file_path.clone();
            tracing::debug!(file = %data_file_path, "reading batch iceberg file");
            let mut chunks = vec![];
            #[for_await]
            for chunk_result in scan_task_to_chunk_with_deletes(
                table.clone(),
                task,
                IcebergScanOpts::new(chunk_size, true, true, true),
                Some(file_scan_metrics.clone()),
            ) {
                let chunk = chunk_result?;
                let ops = itertools::repeat_n(Op::Insert, chunk.capacity()).collect_vec();
                chunks.push(StreamChunk::from_parts(ops, chunk));
            }

            yield ChunksWithState {
                chunks,
                data_file_path,
                last_read_pos: None,
            };
        }
    }
}

impl<S: StateStore> Execute for BatchIcebergFetchExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }
}

impl<S: StateStore> Debug for BatchIcebergFetchExecutor<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(core) = &self.stream_source_core {
            f.debug_struct("BatchIcebergFetchExecutor")
                .field("source_id", &core.source_id)
                .field("column_ids", &core.column_ids)
                .finish()
        } else {
            f.debug_struct("BatchIcebergFetchExecutor").finish()
        }
    }
}

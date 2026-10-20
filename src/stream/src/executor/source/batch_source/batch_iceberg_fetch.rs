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
use std::ops::Bound;
use std::time::Duration;

use either::Either;
use futures::stream;
use iceberg::scan::FileScanTask;
use itertools::Itertools;
use risingwave_common::array::{DataChunk, Op, SerialArray};
use risingwave_common::bail;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{
    ICEBERG_FILE_PATH_COLUMN_NAME, ICEBERG_FILE_POS_COLUMN_NAME, ROW_ID_COLUMN_NAME,
};
use risingwave_common::config::StreamingConfig;
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::id::{SourceId, TableId};
use risingwave_common::metrics::GLOBAL_ERROR_METRICS;
use risingwave_common::types::{JsonbVal, Scalar, ScalarRef, Serial};
use risingwave_connector::source::ConnectorProperties;
use risingwave_connector::source::iceberg::{
    GLOBAL_ICEBERG_SCAN_METRICS, IcebergFileScanMetrics, IcebergScanMetricsLabels, IcebergScanOpts,
    PersistedFileScanTask, scan_task_to_chunk_with_deletes,
};
use risingwave_connector::source::reader::desc::SourceDesc;
use risingwave_pb::stream_plan::bounded_load_mutation::Action as BoundedLoadAction;
use risingwave_pb::stream_service::barrier_complete_response::bounded_load_progress::{
    LoadFailed, Progress, ReaderDone,
};
use risingwave_storage::store::PrefetchOptions;
use thiserror_ext::AsReport;

use super::load_mode::{Backoff, LoadMode};
use crate::executor::prelude::*;
use crate::executor::source::{
    ChunksWithState, SourceStateTableHandler, StreamSourceCore, prune_additional_cols,
};
use crate::executor::stream_reader::StreamReaderWithPause;
use crate::task::{ActorId, LocalBarrierManager};

/// Type alias for file entries in the queue: (`file_name`, `scan_task_json`)
type FileEntry = (String, JsonbVal);

#[derive(PartialEq, Eq)]
enum FetchPhase {
    Idle,
    /// More file assignments may arrive from upstream.
    Loading,
    /// Listing is sealed; finish the remaining files.
    Draining,
    /// The load gave up after repeated errors.
    Failed,
}

struct FetchState {
    mode: LoadMode,
    phase: FetchPhase,

    /// Number of files currently being fetched in the active batch reader
    splits_on_fetch: usize,

    /// Queue of files waiting to be processed
    file_queue: VecDeque<FileEntry>,

    /// Files currently being fetched by the batch reader.
    /// Used for at-least-once recovery: on error, these files are re-queued.
    in_flight_files: Vec<FileEntry>,

    /// Delays retries after consecutive errors; once a bounded load's backoff runs out, it fails.
    backoff: Backoff,
}

impl FetchState {
    fn new(mode: LoadMode) -> Self {
        Self {
            mode,
            phase: FetchPhase::Idle,
            splits_on_fetch: 0,
            file_queue: VecDeque::new(),
            in_flight_files: Vec::new(),
            backoff: mode.backoff(),
        }
    }

    /// Clear previous work and start a new load.
    fn start_load(&mut self) {
        tracing::info!(
            "start_load: clearing file_queue_len={}, in_flight_files_len={}, splits_on_fetch={}",
            self.file_queue.len(),
            self.in_flight_files.len(),
            self.splits_on_fetch
        );
        self.file_queue.clear();
        self.in_flight_files.clear();
        self.splits_on_fetch = 0;
        self.backoff = self.mode.backoff();
        self.phase = FetchPhase::Loading;
    }

    fn seal_listing(&mut self) {
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

    fn is_loading(&self) -> bool {
        matches!(self.phase, FetchPhase::Loading | FetchPhase::Draining)
    }

    /// Check if we should start a new batch reader.
    fn should_start_batch_reader(&self, need_rebuild: bool) -> bool {
        matches!(self.phase, FetchPhase::Loading | FetchPhase::Draining)
            && (need_rebuild || (self.splits_on_fetch == 0 && !self.file_queue.is_empty()))
    }

    /// Mark one file as successfully fetched and resets the backoff.
    /// The file leaves `in_flight_files`, so a later error does not read it again.
    fn mark_file_fetched(&mut self, file_name: &str) {
        self.splits_on_fetch -= 1;
        self.backoff = self.mode.backoff();
        if let Some(idx) = self
            .in_flight_files
            .iter()
            .position(|(name, _)| name == file_name)
        {
            self.in_flight_files.remove(idx);
        }

        // When all files in the current batch complete successfully, clear in-flight tracking.
        if self.splits_on_fetch == 0 {
            tracing::info!("All files fetched successfully, clearing in_flight_files");
            self.in_flight_files.clear();
        }
    }

    /// Handle fetch error with at-least-once recovery.
    /// Re-queues in-flight files to ensure no file is skipped.
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

/// Indices of special columns that need to be pruned from or padded into the output.
struct ColumnIndices {
    file_path_idx: usize,
    file_pos_idx: usize,
    /// The hidden row id column, present when the source has no primary key. Iceberg files do
    /// not carry it, so it is padded with nulls for the downstream row id generator to fill.
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
/// # Refresh Cycle
///
/// 1. Receives `RefreshStart` mutation - clears state and starts new cycle
/// 2. Receives file chunks from upstream list executor - queues files for processing
/// 3. On each barrier, starts batch reader if files are pending
/// 4. Receives `ListFinish` mutation - marks listing as complete
/// 5. When all files processed, reports load finished
///
/// # Delivery
///
/// On fetch errors, files not yet emitted are re-queued, so no file is skipped or emitted twice.
///
/// A refreshable table keeps its queue in memory and restarts its load after recovery.
/// A bounded load also records each assigned file in the state table, and deletes it in the
/// epoch that emits the file's rows. After recovery it reads only the recorded files, so every
/// file reaches downstream exactly once.
pub struct BatchIcebergFetchExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,

    /// Core component for managing external streaming source state
    stream_source_core: Option<StreamSourceCore<S>>,

    /// Upstream list executor that provides file scan tasks
    upstream: Option<Executor>,

    /// Barrier manager for reporting load finished
    barrier_manager: LocalBarrierManager,

    streaming_config: Arc<StreamingConfig>,

    mode: LoadMode,

    scan_metrics: Option<IcebergScanMetricsLabels>,
    file_scan_metrics: Option<IcebergFileScanMetrics>,
}

impl<S: StateStore> BatchIcebergFetchExecutor<S> {
    pub fn new(
        actor_ctx: ActorContextRef,
        stream_source_core: StreamSourceCore<S>,
        upstream: Executor,
        barrier_manager: LocalBarrierManager,
        streaming_config: Arc<StreamingConfig>,
        associated_table_id: Option<TableId>,
        is_bounded: bool,
    ) -> StreamResult<Self> {
        Ok(Self {
            actor_ctx,
            stream_source_core: Some(stream_source_core),
            upstream: Some(upstream),
            barrier_manager,
            streaming_config,
            mode: LoadMode::new(is_bounded, associated_table_id)?,
            scan_metrics: None,
            file_scan_metrics: None,
        })
    }
}

impl<S: StateStore> BatchIcebergFetchExecutor<S> {
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        // Initialize upstream and wait for first barrier
        let mut upstream = self.upstream.take().unwrap().execute();
        let first_barrier = expect_first_barrier(&mut upstream).await?;
        let mode = self.mode;
        let starts_on_creation = mode.starts_on_creation(&first_barrier, self.actor_ctx.id);
        let pause_on_startup = first_barrier.is_pause_on_startup();
        let first_epoch = first_barrier.epoch;
        yield Message::Barrier(first_barrier);

        // Initialize source description
        let mut core = self.stream_source_core.take().unwrap();
        if mode.is_bounded() {
            core.split_state_store.init_epoch(first_epoch).await?;
        }
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
        let mut state = FetchState::new(mode);
        let mut stream = StreamReaderWithPause::<true, ChunksWithState>::new(
            upstream,
            stream::pending().boxed(),
        );

        if pause_on_startup {
            stream.pause_stream();
        }

        if starts_on_creation {
            state.start_load();
            if mode.is_bounded() {
                let pending = Self::pending_files(&core.split_state_store).await?;
                tracing::info!(
                    actor_id = %self.actor_ctx.id,
                    pending = pending.len(),
                    "restored pending bounded files"
                );
                state.enqueue_files(pending);
            }
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
                        self.actor_ctx.fragment_id.to_string(),
                        mode.associated_table_id()
                            .map(|id| id.to_string())
                            .unwrap_or_default(),
                    ]);

                    scan_metrics.record_fetch_error();

                    let Some(delay) = state.backoff.next() else {
                        tracing::error!(
                            source_id = %core.source_id,
                            table_id = ?mode.associated_table_id(),
                            error = %e.as_report(),
                            "bounded load failed after repeated fetch errors"
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
                            table_id = ?mode.associated_table_id(),
                            in_flight_count = %in_flight_count,
                            "re-queued in-flight files that were not emitted yet for retry"
                        );
                    }

                    tracing::info!(
                        source_id = %core.source_id,
                        table_id = ?mode.associated_table_id(),
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
                        let need_rebuild = Self::handle_barrier_mutations(
                            &barrier,
                            core.source_id,
                            self.actor_ctx.id,
                            &mut state,
                            &mut stream,
                            &mode,
                        );

                        if barrier.is_checkpoint() && state.is_failed() {
                            self.barrier_manager.report_bounded_load_progress(
                                barrier.epoch,
                                self.actor_ctx.id,
                                self.actor_ctx.fragment_id,
                                Progress::LoadFailed(LoadFailed {}),
                            );
                            state.mark_idle();
                        }

                        if barrier.is_checkpoint() && state.should_report_finished() {
                            tracing::info!(
                                ?barrier.epoch,
                                actor_id = %self.actor_ctx.id,
                                source_id = %core.source_id,
                                table_id = ?mode.associated_table_id(),
                                "Reporting load finished"
                            );
                            match mode {
                                LoadMode::FullReload {
                                    associated_table_id,
                                } => {
                                    self.barrier_manager.report_source_load_finished(
                                        barrier.epoch,
                                        self.actor_ctx.id,
                                        associated_table_id,
                                        core.source_id,
                                    );
                                }
                                LoadMode::Bounded { .. } => {
                                    self.barrier_manager.report_bounded_load_progress(
                                        barrier.epoch,
                                        self.actor_ctx.id,
                                        self.actor_ctx.fragment_id,
                                        Progress::ReaderDone(ReaderDone {}),
                                    );
                                }
                            }
                            state.mark_idle();
                        }

                        if mode.is_bounded() {
                            let update_vnode_bitmap =
                                barrier.as_update_vnode_bitmap(self.actor_ctx.id);
                            let post_commit = core
                                .split_state_store
                                .commit_may_update_vnode_bitmap(barrier.epoch)
                                .await?;
                            let is_loading = state.is_loading();
                            yield Message::Barrier(barrier);
                            // Meta blocks rescheduling while a bounded load runs. A load that
                            // still sees its vnodes move fails here and restarts from the state
                            // table after recovery.
                            if post_commit
                                .post_yield_barrier(update_vnode_bitmap)
                                .await?
                                .is_some()
                                && is_loading
                            {
                                bail!("vnodes of a bounded fetch changed while it was loading");
                            }
                        } else {
                            yield Message::Barrier(barrier);
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
                        if mode.is_bounded() {
                            core.split_state_store
                                .set_states_json(files.iter().cloned())
                                .await?;
                            core.split_state_store.try_flush().await?;
                        }
                        state.enqueue_files(files);
                    }

                    Message::Watermark(_) => unreachable!(),
                },

                // ----- Fetched Data from Iceberg Files -----
                Ok(Either::Right(ChunksWithState {
                    chunks,
                    data_file_path,
                    ..
                })) => {
                    state.mark_file_fetched(&data_file_path);
                    // The file's rows are emitted below in this same epoch.
                    if mode.is_bounded() {
                        core.split_state_store.delete(&data_file_path).await?;
                    }

                    scan_metrics.set_inflight_file_count(state.splits_on_fetch);

                    for chunk in &chunks {
                        let pruned = prune_additional_cols(
                            chunk,
                            &column_indices.to_prune(),
                            &source_desc.columns,
                        );
                        yield Message::Chunk(match column_indices.row_id_idx {
                            Some(row_id_idx) => pad_row_id(pruned, row_id_idx),
                            None => pruned,
                        });
                    }
                }
            }
        }
    }

    /// Handle barrier mutations and return whether reader needs to be rebuilt.
    fn handle_barrier_mutations(
        barrier: &Barrier,
        source_id: SourceId,
        actor_id: ActorId,
        state: &mut FetchState,
        stream: &mut StreamReaderWithPause<true, ChunksWithState>,
        mode: &LoadMode,
    ) -> bool {
        let Some(mutation) = barrier.mutation.as_deref() else {
            return false;
        };

        match mutation {
            Mutation::Pause => {
                stream.pause_stream();
                false
            }
            Mutation::Resume => {
                stream.resume_stream();
                false
            }
            Mutation::RefreshStart {
                associated_source_id,
                ..
            } if associated_source_id == &source_id && mode.accepts_refresh_mutations() => {
                tracing::info!(
                    ?barrier.epoch,
                    source_id = %source_id,
                    is_checkpoint = barrier.is_checkpoint(),
                    "RefreshStart: starting new refresh cycle"
                );
                state.start_load();
                true
            }
            Mutation::ListFinish {
                associated_source_id,
            } if associated_source_id == &source_id && mode.accepts_refresh_mutations() => {
                tracing::info!(
                    ?barrier.epoch,
                    source_id = %source_id,
                    is_checkpoint = barrier.is_checkpoint(),
                    "ListFinish: upstream finished listing files"
                );
                state.seal_listing();
                false
            }
            Mutation::BoundedLoad {
                reader_actor_ids,
                action,
                ..
            } if mode.accepts_bounded_mutation(reader_actor_ids, actor_id) => match action {
                BoundedLoadAction::ListSealed(_) => {
                    tracing::info!(
                        ?barrier.epoch,
                        source_id = %source_id,
                        is_checkpoint = barrier.is_checkpoint(),
                        "BoundedLoad: upstream sealed listing"
                    );
                    state.seal_listing();
                    false
                }
            },
            _ => false,
        }
    }

    /// Reads the files recorded as pending in the vnodes this actor owns.
    async fn pending_files(
        state_store: &SourceStateTableHandler<S>,
    ) -> StreamExecutorResult<Vec<FileEntry>> {
        let state_table = state_store.state_table();
        let mut files = vec![];
        for vnode in state_table.vnodes().iter_vnodes() {
            let rows = state_table
                .iter_with_vnode(
                    vnode,
                    &(Bound::<OwnedRow>::Unbounded, Bound::<OwnedRow>::Unbounded),
                    PrefetchOptions::prefetch_for_small_range_scan(),
                )
                .await?;
            pin_mut!(rows);
            while let Some(row) = rows.next().await {
                let row = row?;
                let file_name = row.datum_at(0).unwrap().into_utf8().to_owned();
                let scan_task = row.datum_at(1).unwrap().into_jsonb().to_owned_scalar();
                files.push((file_name, scan_task));
            }
        }
        Ok(files)
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

/// Inserts an all-null row id column at `row_id_idx`.
fn pad_row_id(chunk: StreamChunk, row_id_idx: usize) -> StreamChunk {
    let (chunk, ops) = chunk.into_parts();
    let (mut columns, visibility) = chunk.into_parts();
    let len = visibility.len();
    columns.insert(
        row_id_idx,
        Arc::new(
            SerialArray::from_iter_bitmap(
                itertools::repeat_n(Serial::from(0), len),
                Bitmap::zeros(len),
            )
            .into(),
        ),
    );
    StreamChunk::from_parts(ops, DataChunk::from_parts(columns.into(), visibility))
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

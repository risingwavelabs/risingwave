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

use either::Either;
use futures::stream;
use iceberg::scan::FileScanTask;
use itertools::Itertools;
use risingwave_common::array::Op;
use risingwave_common::catalog::{ICEBERG_FILE_PATH_COLUMN_NAME, ICEBERG_FILE_POS_COLUMN_NAME};
use risingwave_common::config::StreamingConfig;
use risingwave_common::id::{SourceId, TableId};
use risingwave_common::metrics::GLOBAL_ERROR_METRICS;
use risingwave_common::types::{JsonbVal, Scalar, ScalarRef};
use risingwave_connector::source::ConnectorProperties;
use risingwave_connector::source::iceberg::{
    GLOBAL_ICEBERG_SCAN_METRICS, IcebergFileScanMetrics, IcebergScanMetricsLabels, IcebergScanOpts,
    PersistedFileScanTask, scan_task_to_chunk_with_deletes,
};
use risingwave_connector::source::reader::desc::SourceDesc;
use risingwave_pb::stream_plan::bounded_load_mutation::Action as BoundedLoadAction;
use risingwave_pb::stream_service::barrier_complete_response::bounded_load_progress::{
    Progress, ReaderDone,
};
use thiserror_ext::AsReport;

use super::load_mode::LoadMode;
use crate::executor::prelude::*;
use crate::executor::source::{ChunksWithState, StreamSourceCore, prune_additional_cols};
use crate::executor::stream_reader::StreamReaderWithPause;
use crate::task::LocalBarrierManager;

/// Type alias for file entries in the queue: (`file_name`, `scan_task_json`)
type FileEntry = (String, JsonbVal);

#[derive(PartialEq, Eq)]
enum FetchPhase {
    Idle,
    /// More file assignments may arrive from upstream.
    Loading,
    /// Listing is sealed; finish the remaining files.
    Draining,
}

struct FetchState {
    phase: FetchPhase,

    /// Number of files currently being fetched in the active batch reader
    splits_on_fetch: usize,

    /// Queue of files waiting to be processed
    file_queue: VecDeque<FileEntry>,

    /// Files currently being fetched by the batch reader.
    /// Used for at-least-once recovery: on error, these files are re-queued.
    in_flight_files: Vec<FileEntry>,
}

impl FetchState {
    fn new() -> Self {
        Self {
            phase: FetchPhase::Idle,
            splits_on_fetch: 0,
            file_queue: VecDeque::new(),
            in_flight_files: Vec::new(),
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

    /// Mark the load as complete after reporting completion.
    fn mark_complete(&mut self) {
        self.phase = FetchPhase::Idle;
    }

    /// Check if we should start a new batch reader.
    fn should_start_batch_reader(&self, need_rebuild: bool) -> bool {
        self.phase != FetchPhase::Idle
            && (need_rebuild || (self.splits_on_fetch == 0 && !self.file_queue.is_empty()))
    }

    /// Mark one file as successfully fetched.
    fn mark_file_fetched(&mut self) {
        self.splits_on_fetch -= 1;

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

/// Indices of special columns that need to be pruned from output.
struct ColumnIndices {
    file_path_idx: usize,
    file_pos_idx: usize,
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
        Self {
            file_path_idx,
            file_pos_idx,
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
/// # At-Least-Once Semantics
///
/// On fetch errors, in-flight files are re-queued to ensure no file is skipped.
/// This may cause duplicate reads, but guarantees data completeness.
pub struct BatchIcebergFetchExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,

    /// Core component for managing external streaming source state
    stream_source_core: Option<StreamSourceCore<S>>,

    /// Upstream list executor that provides file scan tasks
    upstream: Option<Executor>,

    /// Barrier manager for reporting load finished
    barrier_manager: LocalBarrierManager,

    streaming_config: Arc<StreamingConfig>,

    associated_table_id: TableId,
    is_bounded: bool,

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
    ) -> Self {
        assert!(associated_table_id.is_some());
        Self {
            actor_ctx,
            stream_source_core: Some(stream_source_core),
            upstream: Some(upstream),
            barrier_manager,
            streaming_config,
            associated_table_id: associated_table_id.unwrap(),
            is_bounded,
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
        let mode = if self.is_bounded {
            LoadMode::Bounded
        } else {
            LoadMode::FullReload
        };
        let starts_on_creation = mode.starts_on_creation(&first_barrier, self.actor_ctx.id);
        let pause_on_startup = first_barrier.is_pause_on_startup();
        yield Message::Barrier(first_barrier);

        // Initialize source description
        let mut core = self.stream_source_core.take().unwrap();
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
        let mut state = FetchState::new();
        let mut stream = StreamReaderWithPause::<true, ChunksWithState>::new(
            upstream,
            stream::pending().boxed(),
        );

        if pause_on_startup {
            stream.pause_stream();
        }

        if starts_on_creation {
            state.start_load();
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
                        self.associated_table_id.to_string(),
                    ]);

                    scan_metrics.record_fetch_error();

                    let in_flight_count = state.in_flight_files.len();
                    state.handle_error_recovery();

                    if in_flight_count > 0 {
                        tracing::info!(
                            source_id = %core.source_id,
                            table_id = %self.associated_table_id,
                            in_flight_count = %in_flight_count,
                            "re-queued in-flight files for retry to ensure at-least-once semantics"
                        );
                    }

                    stream.replace_data_stream(stream::pending().boxed());

                    tracing::info!(
                        source_id = %core.source_id,
                        table_id = %self.associated_table_id,
                        remaining_files = %state.file_queue.len(),
                        "attempting to recover from fetch error, will retry on next barrier"
                    );

                    continue;
                }

                // ----- Upstream Messages (barriers, file assignments) -----
                Ok(Either::Left(msg)) => match msg {
                    Message::Barrier(barrier) => {
                        let need_rebuild = Self::handle_barrier_mutations(
                            &barrier,
                            core.source_id,
                            &mut state,
                            &mut stream,
                            &mode,
                        );

                        if barrier.is_checkpoint() && state.should_report_finished() {
                            tracing::info!(
                                ?barrier.epoch,
                                actor_id = %self.actor_ctx.id,
                                source_id = %core.source_id,
                                table_id = %self.associated_table_id,
                                "Reporting load finished"
                            );
                            match mode {
                                LoadMode::FullReload => {
                                    self.barrier_manager.report_source_load_finished(
                                        barrier.epoch,
                                        self.actor_ctx.id,
                                        self.associated_table_id,
                                        core.source_id,
                                    );
                                }
                                LoadMode::Bounded => {
                                    self.barrier_manager.report_bounded_load_progress(
                                        barrier.epoch,
                                        self.actor_ctx.id,
                                        self.actor_ctx.fragment_id,
                                        Progress::ReaderDone(ReaderDone {}),
                                    );
                                }
                            }
                            state.mark_complete();
                        }

                        yield Message::Barrier(barrier);

                        if state.should_start_batch_reader(need_rebuild) {
                            Self::start_batch_reader(
                                &mut state,
                                &mut stream,
                                source_desc.clone(),
                                &self.streaming_config,
                                file_scan_metrics.clone(),
                            )?;

                            scan_metrics.set_inflight_file_count(state.splits_on_fetch);
                        }
                    }

                    Message::Chunk(chunk) => {
                        let files = Self::parse_file_assignments(&chunk);
                        tracing::debug!("Received {} file assignments from upstream", files.len());
                        state.enqueue_files(files);
                    }

                    Message::Watermark(_) => unreachable!(),
                },

                // ----- Fetched Data from Iceberg Files -----
                Ok(Either::Right(ChunksWithState { chunks, .. })) => {
                    state.mark_file_fetched();

                    scan_metrics.set_inflight_file_count(state.splits_on_fetch);

                    for chunk in &chunks {
                        let pruned = prune_additional_cols(
                            chunk,
                            &column_indices.to_prune(),
                            &source_desc.columns,
                        );
                        yield Message::Chunk(pruned);
                    }
                }
            }
        }
    }

    /// Handle barrier mutations and return whether reader needs to be rebuilt.
    fn handle_barrier_mutations(
        barrier: &Barrier,
        source_id: SourceId,
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
                source_id: target_source_id,
                action,
            } if target_source_id == &source_id && mode.accepts_bounded_mutations() => match action
            {
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
                streaming_config.developer.chunk_size,
                file_scan_metrics,
            );
            stream.replace_data_stream(batch_reader.boxed());
        }

        Ok(())
    }

    /// Build a stream reader that reads multiple Iceberg files in sequence.
    #[try_stream(ok = ChunksWithState, error = StreamExecutorError)]
    async fn build_batched_stream_reader(
        source_desc: SourceDesc,
        tasks: Vec<FileScanTask>,
        chunk_size: usize,
        file_scan_metrics: IcebergFileScanMetrics,
    ) {
        let properties = match source_desc.source.config.clone() {
            ConnectorProperties::Iceberg(props) => props,
            _ => unreachable!("Expected Iceberg connector properties"),
        };
        let table = properties.load_table().await?;

        for task in tasks {
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
                data_file_path: String::new(), // Not needed for refreshable iceberg fetch
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

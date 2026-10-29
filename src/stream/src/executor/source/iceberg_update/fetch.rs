// Copyright 2026 RisingWave Labs
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

//! Fetch reads assigned Iceberg update tasks and keeps a resumable cursor per task.

use std::ops::Bound;
use std::time::Instant;

use anyhow::Context;
use either::Either;
use futures::stream;
use risingwave_common::array::Op;
use risingwave_common::catalog::ColumnCatalog;
use risingwave_common::config::StreamingConfig;
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::{bail, ensure};
use risingwave_common_estimate_size::EstimateSize;
use risingwave_connector::sink::iceberg::PositionDeleteReader;
use risingwave_connector::source::iceberg::update_reader::read_file_updates;
use risingwave_connector::source::iceberg::update_state::{
    IcebergUpdateAssignment, IcebergUpdateFetchState,
};
use risingwave_connector::source::iceberg::{
    GLOBAL_ICEBERG_SCAN_METRICS, IcebergFileScanMetrics, IcebergProperties,
    IcebergScanMetricsLabels,
};
use risingwave_storage::store::PrefetchOptions;
use thiserror_ext::AsReport;

use super::{decode, encode, properties, scan_metrics};
use crate::executor::prelude::*;
use crate::executor::source::{SourceStateTableHandler, StreamSourceCore};
use crate::executor::stream_reader::StreamReaderWithPause;

pub struct IcebergUpdateFetchExecutor<S: StateStore> {
    actor: ActorContextRef,
    core: StreamSourceCore<S>,
    upstream: Executor,
    streaming_config: Arc<StreamingConfig>,
    output_columns: Vec<ColumnCatalog>,
    key_indices: Vec<usize>,
}

impl<S: StateStore> IcebergUpdateFetchExecutor<S> {
    pub fn new(
        actor: ActorContextRef,
        core: StreamSourceCore<S>,
        upstream: Executor,
        streaming_config: Arc<StreamingConfig>,
        output_columns: Vec<ColumnCatalog>,
        key_indices: Vec<usize>,
    ) -> Self {
        Self {
            actor,
            core,
            upstream,
            streaming_config,
            output_columns,
            key_indices,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let developer = &self.streaming_config.developer;
        let batch_size = developer.iceberg_fetch_batch_size as usize;
        let chunk_size = developer.chunk_size;
        ensure!(batch_size > 0, "Iceberg fetch batch size must be positive");
        let properties = properties(&mut self.core)?;
        let output_types = self
            .output_columns
            .iter()
            .map(|column| column.data_type().clone())
            .collect::<Vec<_>>();
        let ctx = FetchContext {
            job_id: self
                .core
                .split_state_store
                .state_table()
                .table_id()
                .as_raw_id(),
            batch_size,
            chunk_size,
            output_columns: self.output_columns,
            key_indices: self.key_indices,
            scan_metrics: scan_metrics(&self.core, &properties),
            file_scan_metrics: IcebergFileScanMetrics::new(
                &GLOBAL_ICEBERG_SCAN_METRICS,
                properties.table.table_name(),
            ),
            properties,
        };

        let mut state_table = self.core.split_state_store;
        let mut upstream = self.upstream.execute();
        let first = expect_first_barrier(&mut upstream).await?;
        let first_epoch = first.epoch;
        let paused = first.is_pause_on_startup();
        yield Message::Barrier(first);
        state_table.init_epoch(first_epoch).await?;
        let mut stream = FetchStream::new(upstream, stream::pending());
        if paused {
            stream.pause_stream();
        }
        let mut inflight = ctx.install_next_batch(&state_table, &mut stream).await?;
        // Stays set until the installed batch reports `BatchFinished` or fails.
        let mut busy = inflight > 0;
        while let Some(message) = stream.next().await {
            match message? {
                Either::Left(Message::Barrier(barrier)) => {
                    match barrier.mutation.as_deref() {
                        Some(Mutation::Pause) => stream.pause_stream(),
                        Some(Mutation::Resume) => stream.resume_stream(),
                        _ => {}
                    }
                    // Rescale restarts the actor and restores its assigned vnodes at startup.
                    barrier.assume_no_update_vnode_bitmap(self.actor.id)?;
                    state_table.commit(barrier.epoch).await?;
                    yield Message::Barrier(barrier);
                    if !busy {
                        inflight = ctx.install_next_batch(&state_table, &mut stream).await?;
                        busy = inflight > 0;
                    }
                }
                Either::Left(Message::Chunk(chunk)) => {
                    ctx.accept_assignments(&mut state_table, &chunk).await?;
                    if !busy {
                        inflight = ctx.install_next_batch(&state_table, &mut stream).await?;
                        busy = inflight > 0;
                    }
                }
                Either::Left(_) => bail!("unexpected Iceberg Fetch watermark"),
                Either::Right(Err(e)) => {
                    tracing::error!(
                        error = %e.as_report(),
                        "Iceberg update Fetch errored, retrying"
                    );
                    ctx.scan_metrics.record_fetch_error();
                    // Every emitted batch already saved its cursor, so the next barrier
                    // resumes the unfinished tasks from local state.
                    inflight = 0;
                    ctx.scan_metrics.set_inflight_file_count(0);
                    busy = false;
                    stream.replace_data_stream(stream::pending());
                }
                Either::Right(Ok(FetchResult::Batch { state, chunk })) => {
                    ensure!(
                        chunk
                            .columns()
                            .iter()
                            .map(|column| column.data_type())
                            .collect::<Vec<_>>()
                            == output_types,
                        "Iceberg update output schema differs from its plan"
                    );
                    let key = state.assignment.key()?;
                    state_table.set(&key, encode(&state)?).await?;
                    if chunk.cardinality() != 0 {
                        yield Message::Chunk(chunk);
                    }
                }
                Either::Right(Ok(FetchResult::Finished(state))) => {
                    // Retirement and the last output share the same checkpoint. Recovery
                    // restores either the task cursor or its completed downstream effects.
                    let key = state.assignment.key()?;
                    state_table.delete(&key).await?;
                    inflight = inflight.saturating_sub(1);
                    ctx.scan_metrics.set_inflight_file_count(inflight);
                }
                Either::Right(Ok(FetchResult::BatchFinished)) => {
                    inflight = ctx.install_next_batch(&state_table, &mut stream).await?;
                    busy = inflight > 0;
                }
            }
        }
    }
}

impl<S: StateStore> Execute for IcebergUpdateFetchExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }
}

impl<S: StateStore> Debug for IcebergUpdateFetchExecutor<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergUpdateFetchExecutor")
            .finish_non_exhaustive()
    }
}

type FetchStream = StreamReaderWithPause<true, StreamExecutorResult<FetchResult>>;

/// Inputs that stay fixed for the lifetime of a Fetch executor.
struct FetchContext {
    properties: IcebergProperties,
    job_id: u32,
    batch_size: usize,
    chunk_size: usize,
    output_columns: Vec<ColumnCatalog>,
    key_indices: Vec<usize>,
    scan_metrics: IcebergScanMetricsLabels,
    file_scan_metrics: IcebergFileScanMetrics,
}

impl FetchContext {
    /// Persists the assignments sent by List. A replayed assignment must match the stored one.
    async fn accept_assignments<S: StateStore>(
        &self,
        state_table: &mut SourceStateTableHandler<S>,
        chunk: &StreamChunk,
    ) -> StreamExecutorResult<()> {
        for (op, row) in chunk.rows() {
            let key = row
                .datum_at(0)
                .context("missing assignment key")?
                .into_utf8();
            let assignment: IcebergUpdateAssignment = decode(row)?;
            ensure!(
                assignment.job_id == self.job_id && assignment.key()? == key,
                "Iceberg assignment belongs to another job/generation"
            );
            assignment
                .task
                .validate_key(&self.output_columns, &self.key_indices)?;
            match (op, state_table.get(key).await?) {
                (Op::Insert, None) => {
                    state_table
                        .set(key, encode(&IcebergUpdateFetchState::new(assignment))?)
                        .await?
                }
                (Op::Insert, Some(old)) => {
                    ensure!(
                        decode::<IcebergUpdateFetchState>(&old)?.assignment == assignment,
                        "conflicting duplicate Iceberg assignment"
                    );
                }
                _ => bail!("invalid Iceberg assignment operation"),
            }
        }
        state_table.try_flush().await
    }

    /// Installs the next batch of unfinished tasks and returns its size. An empty batch
    /// installs nothing, so an idle Fetch scans its state once per barrier instead of
    /// again on `BatchFinished`.
    async fn install_next_batch<S: StateStore>(
        &self,
        state_table: &SourceStateTableHandler<S>,
        reader: &mut FetchStream,
    ) -> StreamExecutorResult<usize> {
        let tasks = pending_tasks(
            state_table,
            self.job_id,
            self.batch_size,
            &self.output_columns,
            &self.key_indices,
        )
        .await?;
        let count = tasks.len();
        if tasks.is_empty() {
            reader.replace_data_stream(stream::pending());
        } else {
            reader.replace_data_stream(
                fetch_work(
                    self.properties.clone(),
                    tasks,
                    self.chunk_size,
                    self.file_scan_metrics.clone(),
                )
                .map(Ok),
            );
        }
        self.scan_metrics.set_inflight_file_count(count);
        Ok(count)
    }
}

enum FetchResult {
    Batch {
        state: IcebergUpdateFetchState,
        chunk: StreamChunk,
    },
    Finished(IcebergUpdateFetchState),
    BatchFinished,
}

/// Reads a nonempty batch of tasks in order, then reports `BatchFinished`.
#[try_stream(boxed, ok = FetchResult, error = StreamExecutorError)]
async fn fetch_work(
    properties: IcebergProperties,
    tasks: Vec<IcebergUpdateFetchState>,
    chunk_size: usize,
    metrics: IcebergFileScanMetrics,
) {
    let table = properties.load_table().await?;
    let mut reader = PositionDeleteReader::new(table.file_io());
    for mut state in tasks {
        let started = Instant::now();
        let mut rows = 0;
        let (scan, mode) = state.assignment.task.prepare_read(&table)?;
        let mut batches = read_file_updates(
            table.clone(),
            &mut reader,
            scan,
            mode,
            chunk_size,
            state.next_position,
        );
        while let Some(batch) = batches.next().await {
            let batch = batch?;
            state.advance(batch.next_position, false)?;
            rows += batch.chunk.cardinality() as u64;
            metrics.record_read_bytes(batch.chunk.estimated_heap_size() as u64);
            yield FetchResult::Batch {
                state: state.clone(),
                chunk: batch.chunk,
            };
        }
        metrics.record_file_read_duration(started.elapsed().as_secs_f64());
        if rows > 0 {
            metrics.record_rows_read(rows);
        }
        metrics.record_file_read();
        state.advance(state.assignment.task.record_count(), true)?;
        yield FetchResult::Finished(state);
    }
    yield FetchResult::BatchFinished;
}

pub(super) async fn pending_tasks<S: StateStore>(
    state: &SourceStateTableHandler<S>,
    job_id: u32,
    limit: usize,
    columns: &[ColumnCatalog],
    key_indices: &[usize],
) -> StreamExecutorResult<Vec<IcebergUpdateFetchState>> {
    ensure!(limit > 0, "Iceberg fetch batch size must be positive");
    let table = state.state_table();
    let mut tasks = vec![];
    'vnodes: for vnode in table.vnodes().iter_vnodes() {
        let iter = table
            .iter_with_vnode(
                vnode,
                &(Bound::<OwnedRow>::Unbounded, Bound::<OwnedRow>::Unbounded),
                PrefetchOptions::prefetch_for_small_range_scan(),
            )
            .await?;
        pin_mut!(iter);
        while let Some(row) = iter.next().await {
            let task: IcebergUpdateFetchState = decode(&row?)?;
            ensure!(
                task.assignment.job_id == job_id,
                "Iceberg task belongs to another ingestion job"
            );
            task.assignment.task.validate_key(columns, key_indices)?;
            ensure!(
                !task.finished && task.next_position <= task.assignment.task.record_count(),
                "invalid persisted Iceberg task cursor"
            );
            tasks.push(task);
            if tasks.len() == limit {
                break 'vnodes;
            }
        }
    }
    Ok(tasks)
}

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

use std::future::Future;
use std::pin::Pin;

use either::Either;
use futures::stream;
use futures::stream::select_with_strategy;
use itertools::Itertools;
use risingwave_common::array::DataChunk;
use risingwave_common::bail;
use risingwave_common::catalog::ColumnDesc;
use risingwave_common::row::RowExt;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_connector::parser::{
    ByteStreamSourceParser, DebeziumParser, DebeziumProps, EncodingProperties, JsonProperties,
    ProtocolProperties, SourceStreamChunkBuilder, SpecificParserConfig,
};
use risingwave_connector::source::cdc::external::{CdcOffset, ExternalTableReaderImpl};
use risingwave_connector::source::{SourceColumnDesc, SourceContext};
use rw_futures_util::pausable;
use thiserror_ext::AsReport;
use tracing::Instrument;

use crate::executor::backfill::cdc::state::CdcBackfillState;
use crate::executor::backfill::cdc::upstream_table::external::ExternalStorageTable;
use crate::executor::backfill::cdc::upstream_table::snapshot::{
    SnapshotReadArgs, UpstreamTableRead, UpstreamTableReader,
};
use crate::executor::backfill::utils::{
    get_cdc_chunk_last_offset, get_new_pos, mapping_chunk, mapping_message, mark_cdc_chunk,
};
use crate::executor::backfill::CdcScanOptions;
use crate::executor::monitor::CdcBackfillMetrics;
use crate::executor::prelude::*;
use crate::executor::source::get_infinite_backoff_strategy;
use crate::executor::UpdateMutation;
use crate::task::CreateMviewProgressReporter;

/// `split_id`, `is_finished`, `row_count`, `cdc_offset` all occupy 1 column each.
const METADATA_STATE_LEN: usize = 4;

pub struct CdcBackfillExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,

    /// The external table to be backfilled
    external_table: ExternalStorageTable,

    /// Upstream changelog stream which may contain metadata columns, e.g. `_rw_offset`
    upstream: Executor,

    /// The column indices need to be forwarded to the downstream from the upstream and table scan.
    output_indices: Vec<usize>,

    /// The schema of output chunk, including additional columns if any
    output_columns: Vec<ColumnDesc>,

    /// State table of the `CdcBackfill` executor
    state_impl: CdcBackfillState<S>,

    // TODO: introduce a CdcBackfillProgress to report finish to Meta
    // This object is just a stub right now
    progress: Option<CreateMviewProgressReporter>,

    metrics: CdcBackfillMetrics,

    /// Rate limit in rows/s.
    rate_limit_rps: Option<u32>,

    options: CdcScanOptions,
}

impl<S: StateStore> CdcBackfillExecutor<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        actor_ctx: ActorContextRef,
        external_table: ExternalStorageTable,
        upstream: Executor,
        output_indices: Vec<usize>,
        output_columns: Vec<ColumnDesc>,
        progress: Option<CreateMviewProgressReporter>,
        metrics: Arc<StreamingMetrics>,
        state_table: StateTable<S>,
        rate_limit_rps: Option<u32>,
        options: CdcScanOptions,
    ) -> Self {
        let pk_indices = external_table.pk_indices();
        let upstream_table_id = external_table.table_id().table_id;
        let state_impl = CdcBackfillState::new(
            upstream_table_id,
            state_table,
            pk_indices.len() + METADATA_STATE_LEN,
        );

        let metrics = metrics.new_cdc_backfill_metrics(external_table.table_id(), actor_ctx.id);

        Self {
            actor_ctx,
            external_table,
            upstream,
            output_indices,
            output_columns,
            state_impl,
            progress,
            metrics,
            rate_limit_rps,
            options,
        }
    }

    fn report_metrics(
        metrics: &CdcBackfillMetrics,
        snapshot_processed_row_count: u64,
        upstream_processed_row_count: u64,
    ) {
        metrics
            .cdc_backfill_snapshot_read_row_count
            .inc_by(snapshot_processed_row_count);

        metrics
            .cdc_backfill_upstream_output_row_count
            .inc_by(upstream_processed_row_count);
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        // The indices to primary key columns
        let pk_indices = self.external_table.pk_indices().to_vec();
        let pk_order = self.external_table.pk_order_types().to_vec();

        let table_id = self.external_table.table_id().table_id;
        let upstream_table_name = self.external_table.qualified_table_name();
        let schema_table_name = self.external_table.schema_table_name().clone();
        let external_database_name = self.external_table.database_name().to_owned();

        let additional_columns = self
            .output_columns
            .iter()
            .filter(|col| col.additional_column.column_type.is_some())
            .cloned()
            .collect_vec();

        let mut upstream = self.upstream.execute();

        // Current position of the upstream_table storage primary key.
        // `None` means it starts from the beginning.
        let mut current_pk_pos: Option<OwnedRow>;

        // Poll the upstream to get the first barrier.
        let first_barrier = expect_first_barrier(&mut upstream).await?;

        let mut is_snapshot_paused = first_barrier.is_pause_on_startup();
        let first_barrier_epoch = first_barrier.epoch;
        // The first barrier message should be propagated.
        yield Message::Barrier(first_barrier);
        let mut rate_limit_to_zero = self.rate_limit_rps.is_some_and(|val| val == 0);

        // Check whether this parallelism has been assigned splits,
        // if not, we should bypass the backfill directly.
        let mut state_impl = self.state_impl;

        state_impl.init_epoch(first_barrier_epoch).await?;

        // restore backfill state
        let state = state_impl.restore_state().await?;
        current_pk_pos = state.current_pk_pos.clone();

        let need_backfill = !self.options.disable_backfill && !state.is_finished;

        // Keep track of rows from the snapshot.
        let mut total_snapshot_row_count = state.row_count as u64;

        // After init the state table and forward the initial barrier to downstream,
        // we now try to create the table reader with retry.
        // If backfill hasn't finished, we can ignore upstream cdc events before we create the table reader;
        // If backfill is finished, we should forward the upstream cdc events to downstream.
        let mut table_reader: Option<ExternalTableReaderImpl> = None;
        let external_table = self.external_table.clone();
        let mut future = Box::pin(async move {
            let backoff = get_infinite_backoff_strategy();
            tokio_retry::Retry::spawn(backoff, || async {
                match external_table.create_table_reader().await {
                    Ok(reader) => Ok(reader),
                    Err(e) => {
                        tracing::warn!(error = %e.as_report(), "failed to create cdc table reader, retrying...");
                        Err(e)
                    }
                }
            })
            .instrument(tracing::info_span!("create_cdc_table_reader_with_retry"))
            .await
            .expect("Retry create cdc table reader until success.")
        });

        // Make sure to use mapping_message after transform_upstream.
        let mut upstream = transform_upstream(upstream, self.output_columns.clone()).boxed();
        loop {
            if let Some(msg) =
                build_reader_and_poll_upstream(&mut upstream, &mut table_reader, &mut future)
                    .await?
            {
                if let Some(msg) = mapping_message(msg, &self.output_indices) {
                    match msg {
                        Message::Barrier(barrier) => {
                            // commit state to bump the epoch of state table
                            state_impl.commit_state(barrier.epoch).await?;
                            yield Message::Barrier(barrier);
                        }
                        Message::Chunk(chunk) => {
                            if need_backfill {
                                // ignore chunk if we need backfill, since we can read the data from the snapshot
                            } else {
                                // forward the chunk to downstream
                                yield Message::Chunk(chunk);
                            }
                        }
                        Message::Watermark(_) => {
                            // ignore watermark
                        }
                    }
                }
            } else {
                assert!(table_reader.is_some(), "table reader must created");
                tracing::info!(
                    table_id,
                    upstream_table_name,
                    "table reader created successfully"
                );
                break;
            }
        }

        let upstream_table_reader = UpstreamTableReader::new(
            self.external_table.clone(),
            table_reader.expect("table reader must created"),
        );

        let mut upstream = upstream.peekable();
        let mut last_binlog_offset: Option<CdcOffset> = state
            .last_cdc_offset
            .map_or(upstream_table_reader.current_cdc_offset().await?, Some);

        let offset_parse_func = upstream_table_reader.reader.get_cdc_offset_parser();
        let mut consumed_binlog_offset: Option<CdcOffset> = None;

        tracing::info!(
            table_id,
            upstream_table_name,
            initial_binlog_offset = ?last_binlog_offset,
            ?current_pk_pos,
            is_finished = state.is_finished,
            is_snapshot_paused,
            snapshot_row_count = total_snapshot_row_count,
            rate_limit = self.rate_limit_rps,
            disable_backfill = self.options.disable_backfill,
            snapshot_interval = self.options.snapshot_interval,
            snapshot_batch_size = self.options.snapshot_batch_size,
            "start cdc backfill",
        );

        // CDC Backfill Algorithm:
        //
        // When the first barrier comes from upstream:
        //  - read the current binlog offset as `binlog_low`
        //  - start a snapshot read upon upstream table and iterate over the snapshot read stream
        //  - buffer the changelog event from upstream
        //
        // When a new barrier comes from upstream:
        //  - read the current binlog offset as `binlog_high`
        //  - for each row of the upstream change log, forward it to downstream if it in the range
        //    of [binlog_low, binlog_high] and its pk <= `current_pos`, otherwise ignore it
        //  - reconstruct the whole backfill stream with upstream changelog and a new table snapshot
        //
        // When a chunk comes from snapshot, we forward it to the downstream and raise
        // `current_pos`.
        // When we reach the end of the snapshot read stream, it means backfill has been
        // finished.
        //
        // Once the backfill loop ends, we forward the upstream directly to the downstream.
        if need_backfill {
            // drive the upstream changelog first to ensure we can receive timely changelog event,
            // otherwise the upstream changelog may be blocked by the snapshot read stream
            let _ = Pin::new(&mut upstream).peek().await;

            // wait for a barrier to make sure the backfill starts after upstream source
            #[for_await]
            for msg in upstream.by_ref() {
                match msg? {
                    Message::Barrier(barrier) => {
                        match barrier.mutation.as_deref() {
                            Some(crate::executor::Mutation::Pause) => {
                                is_snapshot_paused = true;
                                tracing::info!(
                                    table_id,
                                    upstream_table_name,
                                    "snapshot is paused by barrier"
                                );
                            }
                            Some(crate::executor::Mutation::Resume) => {
                                is_snapshot_paused = false;
                                tracing::info!(
                                    table_id,
                                    upstream_table_name,
                                    "snapshot is resumed by barrier"
                                );
                            }
                            _ => {
                                // ignore other mutations
                            }
                        }
                        // commit state just to bump the epoch of state table
                        state_impl.commit_state(barrier.epoch).await?;
                        yield Message::Barrier(barrier);
                        break;
                    }
                    Message::Chunk(ref chunk) => {
                        last_binlog_offset = get_cdc_chunk_last_offset(&offset_parse_func, chunk)?;
                    }
                    Message::Watermark(_) => {
                        // Ignore watermark
                    }
                }
            }

            tracing::info!(table_id,
                upstream_table_name,
                initial_binlog_offset = ?last_binlog_offset,
                ?current_pk_pos,
                is_snapshot_paused,
                "start cdc backfill loop");

            // the buffer will be drained when a barrier comes
            let mut upstream_chunk_buffer: Vec<StreamChunk> = vec![];

            'backfill_loop: loop {
                let left_upstream = upstream.by_ref().map(Either::Left);

                let mut snapshot_read_row_cnt: usize = 0;
                let read_args = SnapshotReadArgs::new(
                    current_pk_pos.clone(),
                    self.rate_limit_rps,
                    pk_indices.clone(),
                    additional_columns.clone(),
                    schema_table_name.clone(),
                    external_database_name.clone(),
                );

                let right_snapshot = pin!(upstream_table_reader
                    .snapshot_read_full_table(read_args, self.options.snapshot_batch_size)
                    .map(Either::Right));

                let (right_snapshot, snapshot_valve) = pausable(right_snapshot);
                if is_snapshot_paused {
                    snapshot_valve.pause();
                }

                // Prefer to select upstream, so we can stop snapshot stream when barrier comes.
                let mut backfill_stream =
                    select_with_strategy(left_upstream, right_snapshot, |_: &mut ()| {
                        stream::PollNext::Left
                    });

                let mut cur_barrier_snapshot_processed_rows: u64 = 0;
                let mut cur_barrier_upstream_processed_rows: u64 = 0;
                let mut barrier_count: u32 = 0;
                let mut pending_barrier = None;

                #[for_await]
                for either in &mut backfill_stream {
                    match either {
                        // Upstream
                        Either::Left(msg) => {
                            match msg? {
                                Message::Barrier(barrier) => {
                                    // increase the barrier count and check whether need to start a new snapshot
                                    barrier_count += 1;
                                    let can_start_new_snapshot =
                                        barrier_count == self.options.snapshot_interval;

                                    if let Some(mutation) = barrier.mutation.as_deref() {
                                        use crate::executor::Mutation;
                                        match mutation {
                                            Mutation::Pause => {
                                                is_snapshot_paused = true;
                                                snapshot_valve.pause();
                                            }
                                            Mutation::Resume => {
                                                is_snapshot_paused = false;
                                                snapshot_valve.resume();
                                            }
                                            Mutation::Throttle(some) => {
                                                if let Some(new_rate_limit) =
                                                    some.get(&self.actor_ctx.id)
                                                    && *new_rate_limit != self.rate_limit_rps
                                                {
                                                    self.rate_limit_rps = *new_rate_limit;
                                                    rate_limit_to_zero = self
                                                        .rate_limit_rps
                                                        .is_some_and(|val| val == 0);

                                                    // update and persist current backfill progress without draining the buffered upstream chunks
                                                    state_impl
                                                        .mutate_state(
                                                            current_pk_pos.clone(),
                                                            last_binlog_offset.clone(),
                                                            total_snapshot_row_count,
                                                            false,
                                                        )
                                                        .await?;
                                                    state_impl.commit_state(barrier.epoch).await?;
                                                    yield Message::Barrier(barrier);

                                                    // rebuild the snapshot stream with new rate limit
                                                    continue 'backfill_loop;
                                                }
                                            }
                                            Mutation::Update(UpdateMutation {
                                                dropped_actors,
                                                ..
                                            }) => {
                                                if dropped_actors.contains(&self.actor_ctx.id) {
                                                    // the actor has been dropped, exit the backfill loop
                                                    tracing::info!(
                                                        table_id,
                                                        upstream_table_name,
                                                        "CdcBackfill has been dropped due to config change"
                                                    );
                                                    yield Message::Barrier(barrier);
                                                    break 'backfill_loop;
                                                }
                                            }
                                            _ => (),
                                        }
                                    }

                                    Self::report_metrics(
                                        &self.metrics,
                                        cur_barrier_snapshot_processed_rows,
                                        cur_barrier_upstream_processed_rows,
                                    );

                                    // when processing a barrier, check whether can start a new snapshot
                                    // if the number of barriers reaches the snapshot interval
                                    if can_start_new_snapshot {
                                        // staging the barrier
                                        pending_barrier = Some(barrier);
                                        tracing::debug!(
                                            table_id,
                                            ?current_pk_pos,
                                            ?snapshot_read_row_cnt,
                                            "Prepare to start a new snapshot"
                                        );
                                        // Break the loop for consuming snapshot and prepare to start a new snapshot
                                        break;
                                    } else {
                                        // update and persist current backfill progress
                                        state_impl
                                            .mutate_state(
                                                current_pk_pos.clone(),
                                                last_binlog_offset.clone(),
                                                total_snapshot_row_count,
                                                false,
                                            )
                                            .await?;

                                        state_impl.commit_state(barrier.epoch).await?;

                                        // emit barrier and continue consume the backfill stream
                                        yield Message::Barrier(barrier);
                                    }
                                }
                                Message::Chunk(chunk) => {
                                    // skip empty upstream chunk
                                    if chunk.cardinality() == 0 {
                                        continue;
                                    }

                                    let chunk_binlog_offset =
                                        get_cdc_chunk_last_offset(&offset_parse_func, &chunk)?;

                                    tracing::trace!(
                                        "recv changelog chunk: chunk_offset {:?}, capactiy {}",
                                        chunk_binlog_offset,
                                        chunk.capacity()
                                    );

                                    // Since we don't need changelog before the
                                    // `last_binlog_offset`, skip the chunk that *only* contains
                                    // events before `last_binlog_offset`.
                                    if let Some(last_binlog_offset) = last_binlog_offset.as_ref() {
                                        if let Some(chunk_offset) = chunk_binlog_offset
                                            && chunk_offset < *last_binlog_offset
                                        {
                                            tracing::trace!(
                                                "skip changelog chunk: chunk_offset {:?}, capacity {}",
                                                chunk_offset,
                                                chunk.capacity()
                                            );
                                            continue;
                                        }
                                    }
                                    // Buffer the upstream chunk.
                                    upstream_chunk_buffer.push(chunk.compact());
                                }
                                Message::Watermark(_) => {
                                    // Ignore watermark during backfill.
                                }
                            }
                        }
                        // Snapshot read
                        Either::Right(msg) => {
                            match msg? {
                                None => {
                                    tracing::info!(
                                        table_id,
                                        ?last_binlog_offset,
                                        ?current_pk_pos,
                                        "snapshot read stream ends"
                                    );
                                    // If the snapshot read stream ends, it means all historical
                                    // data has been loaded.
                                    // We should not mark the chunk anymore,
                                    // otherwise, we will ignore some rows in the buffer.
                                    for chunk in upstream_chunk_buffer.drain(..) {
                                        yield Message::Chunk(mapping_chunk(
                                            chunk,
                                            &self.output_indices,
                                        ));
                                    }

                                    // backfill has finished, exit the backfill loop and persist the state when we recv a barrier
                                    break 'backfill_loop;
                                }
                                Some(chunk) => {
                                    // Raise the current position.
                                    // As snapshot read streams are ordered by pk, so we can
                                    // just use the last row to update `current_pos`.
                                    current_pk_pos = Some(get_new_pos(&chunk, &pk_indices));

                                    tracing::trace!(
                                        "got a snapshot chunk: len {}, current_pk_pos {:?}",
                                        chunk.cardinality(),
                                        current_pk_pos
                                    );
                                    let chunk_cardinality = chunk.cardinality() as u64;
                                    cur_barrier_snapshot_processed_rows += chunk_cardinality;
                                    total_snapshot_row_count += chunk_cardinality;
                                    yield Message::Chunk(mapping_chunk(
                                        chunk,
                                        &self.output_indices,
                                    ));
                                }
                            }
                        }
                    }
                }

                assert!(pending_barrier.is_some(), "pending_barrier must exist");
                let pending_barrier = pending_barrier.unwrap();

                // Here we have to ensure the snapshot stream is consumed at least once,
                // since the barrier event can kick in anytime.
                // Otherwise, the result set of the new snapshot stream may become empty.
                // It maybe a cancellation bug of the mysql driver.
                let (_, mut snapshot_stream) = backfill_stream.into_inner();

                // skip consume the snapshot stream if it is paused or rate limit to 0
                if !is_snapshot_paused
                    && !rate_limit_to_zero
                    && let Some(msg) = snapshot_stream
                        .next()
                        .instrument_await("consume_snapshot_stream_once")
                        .await
                {
                    let Either::Right(msg) = msg else {
                        bail!("BUG: snapshot_read contains upstream messages");
                    };
                    match msg? {
                        None => {
                            tracing::info!(
                                table_id,
                                ?last_binlog_offset,
                                ?current_pk_pos,
                                "snapshot read stream ends in the force emit branch"
                            );
                            // End of the snapshot read stream.
                            // Consume the buffered upstream chunk without filtering by `binlog_low`.
                            for chunk in upstream_chunk_buffer.drain(..) {
                                yield Message::Chunk(mapping_chunk(chunk, &self.output_indices));
                            }

                            // mark backfill has finished
                            state_impl
                                .mutate_state(
                                    current_pk_pos.clone(),
                                    last_binlog_offset.clone(),
                                    total_snapshot_row_count,
                                    true,
                                )
                                .await?;

                            // commit state because we have received a barrier message
                            state_impl.commit_state(pending_barrier.epoch).await?;
                            yield Message::Barrier(pending_barrier);
                            // end of backfill loop, since backfill has finished
                            break 'backfill_loop;
                        }
                        Some(chunk) => {
                            // Raise the current pk position.
                            current_pk_pos = Some(get_new_pos(&chunk, &pk_indices));

                            let row_count = chunk.cardinality() as u64;
                            cur_barrier_snapshot_processed_rows += row_count;
                            total_snapshot_row_count += row_count;
                            snapshot_read_row_cnt += row_count as usize;

                            tracing::debug!(
                                table_id,
                                ?current_pk_pos,
                                ?snapshot_read_row_cnt,
                                "force emit a snapshot chunk"
                            );
                            yield Message::Chunk(mapping_chunk(chunk, &self.output_indices));
                        }
                    }
                }

                // If the number of barriers reaches the snapshot interval,
                // consume the buffered upstream chunks.
                if let Some(current_pos) = &current_pk_pos {
                    for chunk in upstream_chunk_buffer.drain(..) {
                        cur_barrier_upstream_processed_rows += chunk.cardinality() as u64;

                        // record the consumed binlog offset that will be
                        // persisted later
                        consumed_binlog_offset =
                            get_cdc_chunk_last_offset(&offset_parse_func, &chunk)?;

                        yield Message::Chunk(mapping_chunk(
                            mark_cdc_chunk(
                                &offset_parse_func,
                                chunk,
                                current_pos,
                                &pk_indices,
                                &pk_order,
                                last_binlog_offset.clone(),
                            )?,
                            &self.output_indices,
                        ));
                    }
                } else {
                    // If no current_pos, means we did not process any snapshot yet.
                    // we can just ignore the upstream buffer chunk in that case.
                    upstream_chunk_buffer.clear();
                }

                // Update last seen binlog offset
                if consumed_binlog_offset.is_some() {
                    last_binlog_offset.clone_from(&consumed_binlog_offset);
                }

                Self::report_metrics(
                    &self.metrics,
                    cur_barrier_snapshot_processed_rows,
                    cur_barrier_upstream_processed_rows,
                );

                // update and persist current backfill progress
                state_impl
                    .mutate_state(
                        current_pk_pos.clone(),
                        last_binlog_offset.clone(),
                        total_snapshot_row_count,
                        false,
                    )
                    .await?;

                state_impl.commit_state(pending_barrier.epoch).await?;
                yield Message::Barrier(pending_barrier);
            }
        } else if self.options.disable_backfill {
            // If backfill is disabled, we just mark the backfill as finished
            tracing::info!(
                table_id,
                upstream_table_name,
                "CdcBackfill has been disabled"
            );
            state_impl
                .mutate_state(
                    current_pk_pos.clone(),
                    last_binlog_offset.clone(),
                    total_snapshot_row_count,
                    true,
                )
                .await?;
        }

        // drop reader to release db connection
        drop(upstream_table_reader);

        tracing::info!(
            table_id,
            upstream_table_name,
            "CdcBackfill has already finished and will forward messages directly to the downstream"
        );

        // Wait for first barrier to come after backfill is finished.
        // So we can update our progress + persist the status.
        while let Some(Ok(msg)) = upstream.next().await {
            if let Some(msg) = mapping_message(msg, &self.output_indices) {
                // If not finished then we need to update state, otherwise no need.
                if let Message::Barrier(barrier) = &msg {
                    // finalized the backfill state
                    // TODO: unify `mutate_state` and `commit_state` into one method
                    state_impl
                        .mutate_state(
                            current_pk_pos.clone(),
                            last_binlog_offset.clone(),
                            total_snapshot_row_count,
                            true,
                        )
                        .await?;
                    state_impl.commit_state(barrier.epoch).await?;

                    // mark progress as finished
                    if let Some(progress) = self.progress.as_mut() {
                        progress.finish(barrier.epoch, total_snapshot_row_count);
                    }
                    yield msg;
                    // break after the state have been saved
                    break;
                }
                yield msg;
            }
        }

        // After backfill progress finished
        // we can forward messages directly to the downstream,
        // as backfill is finished.
        #[for_await]
        for msg in upstream {
            // upstream offsets will be removed from the message before forwarding to
            // downstream
            if let Some(msg) = mapping_message(msg?, &self.output_indices) {
                if let Message::Barrier(barrier) = &msg {
                    // commit state just to bump the epoch of state table
                    state_impl.commit_state(barrier.epoch).await?;
                }
                yield msg;
            }
        }
    }
}

async fn build_reader_and_poll_upstream(
    upstream: &mut BoxedMessageStream,
    table_reader: &mut Option<ExternalTableReaderImpl>,
    future: &mut Pin<Box<impl Future<Output = ExternalTableReaderImpl>>>,
) -> StreamExecutorResult<Option<Message>> {
    if table_reader.is_some() {
        return Ok(None);
    }
    tokio::select! {
        biased;
        reader = &mut *future => {
            *table_reader = Some(reader);
            Ok(None)
        }
        msg = upstream.next() => {
            msg.transpose()
        }
    }
}

#[try_stream(ok = Message, error = StreamExecutorError)]
pub async fn transform_upstream(upstream: BoxedMessageStream, output_columns: Vec<ColumnDesc>) {
    let props = SpecificParserConfig {
        encoding_config: EncodingProperties::Json(JsonProperties {
            use_schema_registry: false,
            timestamptz_handling: None,
        }),
        // the cdc message is generated internally so the key must exist.
        protocol_config: ProtocolProperties::Debezium(DebeziumProps::default()),
    };

    // convert to source column desc to feed into parser
    let columns_with_meta = output_columns
        .iter()
        .map(SourceColumnDesc::from)
        .collect_vec();

    let mut parser = DebeziumParser::new(
        props,
        columns_with_meta.clone(),
        Arc::new(SourceContext::dummy()),
    )
    .await
    .map_err(StreamExecutorError::connector_error)?;

    pin_mut!(upstream);
    #[for_await]
    for msg in upstream {
        let mut msg = msg?;
        if let Message::Chunk(chunk) = &mut msg {
            let parsed_chunk = parse_debezium_chunk(&mut parser, chunk).await?;
            let _ = std::mem::replace(chunk, parsed_chunk);
        }
        yield msg;
    }
}

async fn parse_debezium_chunk(
    parser: &mut DebeziumParser,
    chunk: &StreamChunk,
) -> StreamExecutorResult<StreamChunk> {
    // here we transform the input chunk in (payload varchar, _rw_offset varchar, _rw_table_name varchar) schema
    // to chunk with downstream table schema `info.schema` of MergeNode contains the schema of the
    // table job with `_rw_offset` in the end
    // see `gen_create_table_plan_for_cdc_source` for details
    let mut builder =
        SourceStreamChunkBuilder::with_capacity(parser.columns().to_vec(), chunk.capacity());

    // The schema of input chunk (payload varchar, _rw_offset varchar, _rw_table_name varchar, _row_id)
    // We should use the debezium parser to parse the first column,
    // then chain the parsed row with `_rw_offset` row to get a new row.
    let payloads = chunk.data_chunk().project(vec![0].as_slice());
    let offset_columns = chunk.data_chunk().project(vec![1].as_slice());

    // TODO: preserve the transaction semantics
    for payload in payloads.rows() {
        let ScalarRefImpl::Jsonb(jsonb_ref) = payload.datum_at(0).expect("payload must exist")
        else {
            unreachable!("payload must be jsonb");
        };

        parser
            .parse_inner(
                None,
                Some(jsonb_ref.to_string().as_bytes().to_vec()),
                builder.row_writer(),
            )
            .await
            .unwrap();
    }

    let parsed_chunk = builder.finish();
    let (data_chunk, ops) = parsed_chunk.into_parts();

    // concat the rows in the parsed chunk with the _rw_offset column, we should also retain the Op column
    let mut new_rows = Vec::with_capacity(chunk.capacity());
    let offset_columns = offset_columns.compact();
    for (data_row, offset_row) in data_chunk
        .rows_with_holes()
        .zip_eq_fast(offset_columns.rows_with_holes())
    {
        let combined = data_row.chain(offset_row);
        new_rows.push(combined);
    }

    let data_types = parser
        .columns()
        .iter()
        .map(|col| col.data_type.clone())
        .chain(std::iter::once(DataType::Varchar)) // _rw_offset column
        .collect_vec();

    Ok(StreamChunk::from_parts(
        ops,
        DataChunk::from_rows(new_rows.as_slice(), data_types.as_slice()),
    ))
}

impl<S: StateStore> Execute for CdcBackfillExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use futures::{pin_mut, StreamExt};
    use risingwave_common::array::{Array, DataChunk, Op, StreamChunk};
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema};
    use risingwave_common::types::{DataType, Datum, JsonbVal};
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_common::util::iter_util::ZipEqFast;
    use risingwave_storage::memory::MemoryStateStore;

    use crate::executor::backfill::cdc::cdc_backfill::transform_upstream;
    use crate::executor::monitor::StreamingMetrics;
    use crate::executor::prelude::StateTable;
    use crate::executor::source::default_source_internal_table;
    use crate::executor::test_utils::MockSource;
    use crate::executor::{
        ActorContext, Barrier, CdcBackfillExecutor, CdcScanOptions, ExternalStorageTable, Message,
    };

    #[tokio::test]
    async fn test_transform_upstream_chunk() {
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Jsonb),   // debezium json payload
            Field::unnamed(DataType::Varchar), // _rw_offset
            Field::unnamed(DataType::Varchar), // _rw_table_name
        ]);
        let pk_indices = vec![1];
        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(schema.clone(), pk_indices.clone());
        // let payload = r#"{"before": null,"after":{"O_ORDERKEY": 5, "O_CUSTKEY": 44485, "O_ORDERSTATUS": "F", "O_TOTALPRICE": "144659.20", "O_ORDERDATE": "1994-07-30" },"source":{"version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002", "ts_ms": 1695277757000, "snapshot": "last", "db": "mydb", "sequence": null, "table": "orders_new", "server_id": 0, "gtid": null, "file": "binlog.000008", "pos": 3693, "row": 0, "thread": null, "query": null},"op":"r","ts_ms":1695277757017,"transaction":null}"#.to_string();
        let payload = r#"{ "payload": { "before": null, "after": { "O_ORDERKEY": 5, "O_CUSTKEY": 44485, "O_ORDERSTATUS": "F", "O_TOTALPRICE": "144659.20", "O_ORDERDATE": "1994-07-30" }, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002", "ts_ms": 1695277757000, "snapshot": "last", "db": "mydb", "sequence": null, "table": "orders_new", "server_id": 0, "gtid": null, "file": "binlog.000008", "pos": 3693, "row": 0, "thread": null, "query": null }, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#;

        let datums: Vec<Datum> = vec![
            Some(JsonbVal::from_str(payload).unwrap().into()),
            Some("file: 1.binlog, pos: 100".to_owned().into()),
            Some("mydb.orders".to_owned().into()),
        ];

        println!("datums: {:?}", datums[1]);

        let mut builders = schema.create_array_builders(8);
        for (builder, datum) in builders.iter_mut().zip_eq_fast(datums.iter()) {
            builder.append(datum.clone());
        }
        let columns = builders
            .into_iter()
            .map(|builder| builder.finish().into())
            .collect();

        // one row chunk
        let chunk = StreamChunk::from_parts(vec![Op::Insert], DataChunk::new(columns, 1));

        tx.push_chunk(chunk);
        let upstream = Box::new(source).execute();

        // schema to the debezium parser
        let columns = vec![
            ColumnDesc::named("O_ORDERKEY", ColumnId::new(1), DataType::Int64),
            ColumnDesc::named("O_CUSTKEY", ColumnId::new(2), DataType::Int64),
            ColumnDesc::named("O_ORDERSTATUS", ColumnId::new(3), DataType::Varchar),
            ColumnDesc::named("O_TOTALPRICE", ColumnId::new(4), DataType::Decimal),
            ColumnDesc::named("O_ORDERDATE", ColumnId::new(5), DataType::Date),
            ColumnDesc::named("commit_ts", ColumnId::new(6), DataType::Timestamptz),
        ];

        let parsed_stream = transform_upstream(upstream, columns);
        pin_mut!(parsed_stream);
        // the output chunk must contain the offset column
        if let Some(message) = parsed_stream.next().await {
            println!("chunk: {:#?}", message.unwrap());
        }
    }

    #[tokio::test]
    async fn test_build_reader_and_poll_upstream() {
        let actor_context = ActorContext::for_test(1);
        let external_storage_table = ExternalStorageTable::for_test_undefined();
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Jsonb),   // debezium json payload
            Field::unnamed(DataType::Varchar), // _rw_offset
            Field::unnamed(DataType::Varchar), // _rw_table_name
        ]);
        let pk_indices = vec![1];
        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(schema.clone(), pk_indices.clone());
        let output_indices = vec![1, 0, 4]; // reorder
        let output_columns = vec![
            ColumnDesc::named("O_ORDERKEY", ColumnId::new(1), DataType::Int64),
            ColumnDesc::named("O_CUSTKEY", ColumnId::new(2), DataType::Int64),
            ColumnDesc::named("O_ORDERSTATUS", ColumnId::new(3), DataType::Varchar),
            ColumnDesc::named("O_TOTALPRICE", ColumnId::new(4), DataType::Decimal),
            ColumnDesc::named("O_DUMMY", ColumnId::new(5), DataType::Int64),
            ColumnDesc::named("commit_ts", ColumnId::new(6), DataType::Timestamptz),
        ];
        let store = MemoryStateStore::new();
        let state_table =
            StateTable::from_table_catalog(&default_source_internal_table(0x2333), store, None)
                .await;
        let cdc = CdcBackfillExecutor::new(
            actor_context,
            external_storage_table,
            source,
            output_indices,
            output_columns,
            None,
            StreamingMetrics::unused().into(),
            state_table,
            None,
            CdcScanOptions {
                // We want to mark backfill as finished. However it's not straightforward to do so.
                // Here we disable_backfill instead.
                disable_backfill: true,
                ..CdcScanOptions::default()
            },
        );
        // cdc.state_impl.init_epoch(EpochPair::new(test_epoch(4), test_epoch(3))).await.unwrap();
        // cdc.state_impl.mutate_state(None, None, 0, true).await.unwrap();
        // cdc.state_impl.commit_state(EpochPair::new(test_epoch(5), test_epoch(4))).await.unwrap();
        let s = cdc.execute_inner();
        pin_mut!(s);

        // send first barrier
        tx.send_barrier(Barrier::new_test_barrier(test_epoch(8)));
        // send chunk
        {
            let payload = r#"{ "payload": { "before": null, "after": { "O_ORDERKEY": 5, "O_CUSTKEY": 44485, "O_ORDERSTATUS": "F", "O_TOTALPRICE": "144659.20", "O_DUMMY": 100 }, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002", "ts_ms": 1695277757000, "snapshot": "last", "db": "mydb", "sequence": null, "table": "orders_new", "server_id": 0, "gtid": null, "file": "binlog.000008", "pos": 3693, "row": 0, "thread": null, "query": null }, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#;
            let datums: Vec<Datum> = vec![
                Some(JsonbVal::from_str(payload).unwrap().into()),
                Some("file: 1.binlog, pos: 100".to_owned().into()),
                Some("mydb.orders".to_owned().into()),
            ];
            let mut builders = schema.create_array_builders(8);
            for (builder, datum) in builders.iter_mut().zip_eq_fast(datums.iter()) {
                builder.append(datum.clone());
            }
            let columns = builders
                .into_iter()
                .map(|builder| builder.finish().into())
                .collect();
            // one row chunk
            let chunk = StreamChunk::from_parts(vec![Op::Insert], DataChunk::new(columns, 1));

            tx.push_chunk(chunk);
        }
        let _first_barrier = s.next().await.unwrap();
        let upstream_change_log = s.next().await.unwrap().unwrap();
        let Message::Chunk(chunk) = upstream_change_log else {
            panic!("expect chunk");
        };
        assert_eq!(chunk.columns().len(), 3);
        assert_eq!(chunk.rows().count(), 1);
        assert_eq!(
            chunk.columns()[0].as_int64().iter().collect::<Vec<_>>(),
            vec![Some(44485)]
        );
        assert_eq!(
            chunk.columns()[1].as_int64().iter().collect::<Vec<_>>(),
            vec![Some(5)]
        );
        assert_eq!(
            chunk.columns()[2].as_int64().iter().collect::<Vec<_>>(),
            vec![Some(100)]
        );
    }
}

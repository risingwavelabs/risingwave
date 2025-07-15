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

use either::Either;
use futures::stream;
use futures::stream::select_with_strategy;
use itertools::Itertools;
use risingwave_common::bitmap::BitmapBuilder;
use risingwave_common::catalog::{ColumnDesc, Field};
use risingwave_common::row::RowDeserializer;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::sort_util::{OrderType, cmp_datum};
use risingwave_connector::source::cdc::CdcScanOptions;
use risingwave_connector::source::cdc::external::{CdcOffset, ExternalTableReaderImpl};
use risingwave_connector::source::{CdcTableSnapshotSplit, CdcTableSnapshotSplitRaw};
use rw_futures_util::pausable;
use thiserror_ext::AsReport;
use tracing::Instrument;

use crate::executor::UpdateMutation;
use crate::executor::backfill::cdc::cdc_backfill::{
    build_reader_and_poll_upstream, transform_upstream,
};
use crate::executor::backfill::cdc::state_v2::ParallelizedCdcBackfillState;
use crate::executor::backfill::cdc::upstream_table::external::ExternalStorageTable;
use crate::executor::backfill::cdc::upstream_table::snapshot::{
    SplitSnapshotReadArgs, UpstreamTableRead, UpstreamTableReader,
};
use crate::executor::backfill::utils::{get_cdc_chunk_last_offset, mapping_chunk, mapping_message};
use crate::executor::prelude::*;
use crate::executor::source::get_infinite_backoff_strategy;

/// `split_id`, `is_finished`, `row_count` all occupy 1 column each.
const METADATA_STATE_LEN: usize = 3;

pub struct ParallelizedCdcBackfillExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,

    /// The external table to be backfilled
    external_table: ExternalStorageTable,

    /// Upstream changelog stream which may contain metadata columns, e.g. `_rw_offset`
    upstream: Executor,

    /// The column indices need to be forwarded to the downstream from the upstream and table scan.
    output_indices: Vec<usize>,

    /// The schema of output chunk, including additional columns if any
    output_columns: Vec<ColumnDesc>,

    /// Rate limit in rows/s.
    rate_limit_rps: Option<u32>,

    options: CdcScanOptions,

    state_table: StateTable<S>,
}

impl<S: StateStore> ParallelizedCdcBackfillExecutor<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        actor_ctx: ActorContextRef,
        external_table: ExternalStorageTable,
        upstream: Executor,
        output_indices: Vec<usize>,
        output_columns: Vec<ColumnDesc>,
        _metrics: Arc<StreamingMetrics>,
        state_table: StateTable<S>,
        rate_limit_rps: Option<u32>,
        options: CdcScanOptions,
    ) -> Self {
        Self {
            actor_ctx,
            external_table,
            upstream,
            output_indices,
            output_columns,
            rate_limit_rps,
            options,
            state_table,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        assert!(!self.options.disable_backfill);
        // The indices to primary key columns
        let pk_indices = self.external_table.pk_indices().to_vec();
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
        // Currently we hard code the split column to be the first column of primary keys.
        // TODO(zw): feat: configurable split columns
        let snapshot_split_column_index = pk_indices[0];
        let cdc_table_snapshot_split_column =
            vec![self.external_table.schema().fields[snapshot_split_column_index].clone()];

        let mut upstream = self.upstream.execute();
        // Poll the upstream to get the first barrier.
        let first_barrier = expect_first_barrier(&mut upstream).await?;
        // Make sure to use mapping_message after transform_upstream.
        let mut upstream = transform_upstream(upstream, self.output_columns.clone()).boxed();
        let mut next_reset_barrier = Some(first_barrier);
        let mut is_reset = false;
        let mut state_impl =
            ParallelizedCdcBackfillState::new(self.state_table, METADATA_STATE_LEN);
        // Need reset on CDC table snapshot splits reschedule.
        'with_cdc_table_snapshot_splits: loop {
            let reset_barrier = next_reset_barrier.take().unwrap();
            let all_snapshot_splits = match reset_barrier.mutation.as_deref() {
                Some(Mutation::Add(add)) => &add.actor_cdc_table_snapshot_splits,
                Some(Mutation::Update(update)) => &update.actor_cdc_table_snapshot_splits,
                _ => {
                    return Err(anyhow::anyhow!("ParallelizedCdcBackfillExecutor expects either Mutation::Add or Mutation::Update to initialize CDC table snapshot splits.").into());
                }
            };
            let mut actor_snapshot_splits = vec![];
            // TODO(zw): optimization: remove consumed splits to reduce barrier size for downstream.
            if let Some(splits) = all_snapshot_splits.get(&self.actor_ctx.id) {
                actor_snapshot_splits = splits
                    .iter()
                    .map(|s: &CdcTableSnapshotSplitRaw| {
                        let de = RowDeserializer::new(
                            cdc_table_snapshot_split_column
                                .iter()
                                .map(Field::data_type)
                                .collect_vec(),
                        );
                        let left_bound_inclusive =
                            de.deserialize(s.left_bound_inclusive.as_ref()).unwrap();
                        let right_bound_exclusive =
                            de.deserialize(s.right_bound_exclusive.as_ref()).unwrap();
                        CdcTableSnapshotSplit {
                            split_id: s.split_id,
                            left_bound_inclusive,
                            right_bound_exclusive,
                        }
                    })
                    .collect();
            }
            tracing::debug!(?actor_snapshot_splits, "actor splits");

            let mut is_snapshot_paused = reset_barrier.is_pause_on_startup();
            let barrier_epoch = reset_barrier.epoch;
            yield Message::Barrier(reset_barrier);
            if !is_reset {
                state_impl.init_epoch(barrier_epoch).await?;
                is_reset = true;
                tracing::info!("Initialize executor.");
            } else {
                tracing::info!("Reset executor.");
            }

            let mut current_actor_bounds = None;
            // Find next split that need backfill.
            let mut next_split_idx = actor_snapshot_splits.len();
            for (idx, split) in actor_snapshot_splits.iter().enumerate() {
                let state = state_impl.restore_state(split.split_id).await?;
                if !state.is_finished {
                    next_split_idx = idx;
                    break;
                }
                extends_current_actor_bound(&mut current_actor_bounds, split);
            }
            for split in actor_snapshot_splits.iter().skip(next_split_idx) {
                // Initialize state so that overall progress can be measured.
                state_impl.mutate_state(split.split_id, false, 0).await?;
            }

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
                                if is_reset_barrier(&barrier, self.actor_ctx.id) {
                                    next_reset_barrier = Some(barrier);
                                    continue 'with_cdc_table_snapshot_splits;
                                }
                                yield Message::Barrier(barrier);
                            }
                            Message::Chunk(chunk) => {
                                if chunk.cardinality() == 0 {
                                    continue;
                                }
                                if let Some(filtered_chunk) = filter_stream_chunk(
                                    chunk,
                                    &current_actor_bounds,
                                    snapshot_split_column_index,
                                ) && filtered_chunk.cardinality() > 0
                                {
                                    yield Message::Chunk(filtered_chunk);
                                }
                            }
                            msg @ Message::Watermark(_) => yield msg,
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
            let offset_parse_func = upstream_table_reader.reader.get_cdc_offset_parser();

            'split_backfill: for split in actor_snapshot_splits.iter().skip(next_split_idx) {
                // let state = state_impl.restore_state(split.split_id).await?;
                // Keep track of rows from the snapshot.
                tracing::info!(
                    table_id,
                    upstream_table_name,
                    ?split,
                    is_snapshot_paused,
                    "start cdc backfill split"
                );
                let mut upstream_chunk_buffer: Vec<StreamChunk> = vec![];
                let left_upstream = upstream.by_ref().map(Either::Left);
                let read_args = SplitSnapshotReadArgs::new(
                    split.left_bound_inclusive.clone(),
                    split.right_bound_exclusive.clone(),
                    cdc_table_snapshot_split_column.clone(),
                    self.rate_limit_rps,
                    additional_columns.clone(),
                    schema_table_name.clone(),
                    external_database_name.clone(),
                );
                let split_low_log_offset: Option<CdcOffset> =
                    upstream_table_reader.current_cdc_offset().await?;
                let right_snapshot = pin!(
                    upstream_table_reader
                        .snapshot_read_table_split(read_args)
                        .map(Either::Right)
                );
                let (right_snapshot, snapshot_valve) = pausable(right_snapshot);
                if is_snapshot_paused {
                    snapshot_valve.pause();
                }
                let mut backfill_stream =
                    select_with_strategy(left_upstream, right_snapshot, |_: &mut ()| {
                        stream::PollNext::Left
                    });
                let mut row_count = 0;
                #[for_await]
                for either in &mut backfill_stream {
                    match either {
                        // Upstream
                        Either::Left(msg) => {
                            match msg? {
                                Message::Barrier(barrier) => {
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
                                                // TODO(zw): optimization: improve throttle.
                                                // 1. Handle rate limit 0. Currently, to resume the process, the actor must be rebuilt.
                                                // 2. Apply new rate limit immediately.
                                                if let Some(new_rate_limit) =
                                                    some.get(&self.actor_ctx.id)
                                                    && *new_rate_limit != self.rate_limit_rps
                                                {
                                                    // The new rate limit will take effect since next split.
                                                    self.rate_limit_rps = *new_rate_limit;
                                                }
                                            }
                                            Mutation::Update(UpdateMutation {
                                                dropped_actors,
                                                ..
                                            }) => {
                                                if dropped_actors.contains(&self.actor_ctx.id) {
                                                    tracing::info!(
                                                        table_id,
                                                        upstream_table_name,
                                                        "CdcBackfill has been dropped due to config change"
                                                    );
                                                    yield Message::Barrier(barrier);
                                                    break 'split_backfill;
                                                }
                                            }
                                            _ => (),
                                        }
                                    }

                                    // update and persist current backfill progress
                                    state_impl
                                        .mutate_state(split.split_id, false, row_count)
                                        .await?;

                                    state_impl.commit_state(barrier.epoch).await?;

                                    if is_reset_barrier(&barrier, self.actor_ctx.id) {
                                        next_reset_barrier = Some(barrier);
                                        continue 'with_cdc_table_snapshot_splits;
                                    }
                                    // emit barrier and continue to consume the backfill stream
                                    yield Message::Barrier(barrier);
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
                                    // `split_low_log_offset`, skip the chunk that *only* contains
                                    // events before `split_low_log_offset`.
                                    if let Some(split_low_log_offset) =
                                        split_low_log_offset.as_ref()
                                        && let Some(chunk_offset) = chunk_binlog_offset
                                        && chunk_offset < *split_low_log_offset
                                    {
                                        tracing::trace!(
                                            "skip changelog chunk: chunk_offset {:?}, capacity {}",
                                            chunk_offset,
                                            chunk.capacity()
                                        );
                                        continue;
                                    }

                                    if let Some(filtered_chunk) = filter_stream_chunk(
                                        chunk,
                                        &current_actor_bounds,
                                        snapshot_split_column_index,
                                    ) && filtered_chunk.cardinality() > 0
                                    {
                                        // Buffer the upstream chunk.
                                        upstream_chunk_buffer.push(filtered_chunk.compact());
                                    }
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
                                        split_id = split.split_id,
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
                                    // Next split.
                                    break;
                                }
                                Some(chunk) => {
                                    let chunk_cardinality = chunk.cardinality() as u64;
                                    row_count = row_count.saturating_add(chunk_cardinality);
                                    yield Message::Chunk(mapping_chunk(
                                        chunk,
                                        &self.output_indices,
                                    ));
                                }
                            }
                        }
                    }
                }

                // TODO(zw): review: do we still need this workaround?
                // // Here we have to ensure the snapshot stream is consumed at least once,
                // // since the barrier event can kick in anytime.
                // // Otherwise, the result set of the new snapshot stream may become empty.
                // // It maybe a cancellation bug of the mysql driver.

                extends_current_actor_bound(&mut current_actor_bounds, split);
                // update and persist current backfill progress
                // Wait for first barrier to come after backfill is finished.
                // So we can update our progress + persist the status.
                while let Some(Ok(msg)) = upstream.next().await {
                    let Some(msg) = mapping_message(msg, &self.output_indices) else {
                        continue;
                    };
                    match msg {
                        Message::Barrier(barrier) => {
                            // finalized the backfill state
                            state_impl
                                .mutate_state(split.split_id, true, row_count)
                                .await?;
                            state_impl.commit_state(barrier.epoch).await?;
                            if is_reset_barrier(&barrier, self.actor_ctx.id) {
                                next_reset_barrier = Some(barrier);
                                continue 'with_cdc_table_snapshot_splits;
                            }
                            yield Message::Barrier(barrier);
                            // break after the state have been saved
                            break;
                        }
                        Message::Chunk(chunk) => {
                            if chunk.cardinality() == 0 {
                                continue;
                            }
                            if let Some(filtered_chunk) = filter_stream_chunk(
                                chunk,
                                &current_actor_bounds,
                                snapshot_split_column_index,
                            ) && filtered_chunk.cardinality() > 0
                            {
                                yield Message::Chunk(filtered_chunk);
                            }
                        }
                        msg @ Message::Watermark(_) => {
                            yield msg;
                        }
                    }
                }
            }

            upstream_table_reader.disconnect().await?;
            tracing::info!(
                table_id,
                upstream_table_name,
                "CdcBackfill has already finished and will forward messages directly to the downstream"
            );

            // After backfill progress finished
            // we can forward messages directly to the downstream,
            // as backfill is finished.
            #[for_await]
            for msg in &mut upstream {
                // upstream offsets will be removed from the message before forwarding to
                // downstream
                let Some(msg) = mapping_message(msg?, &self.output_indices) else {
                    continue;
                };
                match msg {
                    Message::Barrier(barrier) => {
                        // commit state just to bump the epoch of state table
                        state_impl.commit_state(barrier.epoch).await?;
                        if is_reset_barrier(&barrier, self.actor_ctx.id) {
                            next_reset_barrier = Some(barrier);
                            continue 'with_cdc_table_snapshot_splits;
                        }
                        yield Message::Barrier(barrier);
                    }
                    Message::Chunk(chunk) => {
                        if chunk.cardinality() == 0 {
                            continue;
                        }
                        if let Some(filtered_chunk) = filter_stream_chunk(
                            chunk,
                            &current_actor_bounds,
                            snapshot_split_column_index,
                        ) && filtered_chunk.cardinality() > 0
                        {
                            yield Message::Chunk(filtered_chunk);
                        }
                    }
                    msg @ Message::Watermark(_) => {
                        yield msg;
                    }
                }
            }
        }
    }
}

fn filter_stream_chunk(
    chunk: StreamChunk,
    bound: &Option<(OwnedRow, OwnedRow)>,
    snapshot_split_column_index: usize,
) -> Option<StreamChunk> {
    let Some((left, right)) = bound else {
        return None;
    };
    assert_eq!(left.len(), 1, "multiple split columns is not supported yet");
    assert_eq!(
        right.len(),
        1,
        "multiple split columns is not supported yet"
    );
    let left_split_key = left.datum_at(0);
    let right_split_key = right.datum_at(0);
    let is_leftmost_bound = is_leftmost_bound(left);
    let is_rightmost_bound = is_rightmost_bound(right);
    if is_leftmost_bound && is_rightmost_bound {
        return Some(chunk);
    }
    let mut new_bitmap = BitmapBuilder::with_capacity(chunk.capacity());
    let (ops, columns, visibility) = chunk.into_inner();
    for (row_split_key, v) in columns[snapshot_split_column_index]
        .iter()
        .zip_eq_fast(visibility.iter())
    {
        if !v {
            new_bitmap.append(false);
            continue;
        }
        let mut is_in_range = true;
        if !is_leftmost_bound {
            is_in_range = cmp_datum(
                row_split_key,
                left_split_key,
                OrderType::ascending_nulls_first(),
            )
            .is_ge();
        }
        if is_in_range && !is_rightmost_bound {
            is_in_range = cmp_datum(
                row_split_key,
                right_split_key,
                OrderType::ascending_nulls_last(),
            )
            .is_lt();
        }
        if !is_in_range {
            tracing::trace!(?row_split_key, ?left_split_key, ?right_split_key, snapshot_split_column_index, data_type = ?columns[snapshot_split_column_index].data_type(), "filter out row")
        }
        new_bitmap.append(is_in_range);
    }
    Some(StreamChunk::with_visibility(
        ops,
        columns,
        new_bitmap.finish(),
    ))
}

fn is_leftmost_bound(row: &OwnedRow) -> bool {
    row.iter().all(|d| d.is_none())
}

fn is_rightmost_bound(row: &OwnedRow) -> bool {
    row.iter().all(|d| d.is_none())
}

impl<S: StateStore> Execute for ParallelizedCdcBackfillExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

fn extends_current_actor_bound(
    current: &mut Option<(OwnedRow, OwnedRow)>,
    split: &CdcTableSnapshotSplit,
) {
    if current.is_none() {
        *current = Some((
            split.left_bound_inclusive.clone(),
            split.right_bound_exclusive.clone(),
        ));
    } else {
        current.as_mut().unwrap().1 = split.right_bound_exclusive.clone();
    }
}

fn is_reset_barrier(barrier: &Barrier, actor_id: ActorId) -> bool {
    match barrier.mutation.as_deref() {
        Some(Mutation::Update(update)) => update
            .actor_cdc_table_snapshot_splits
            .contains_key(&actor_id),
        _ => false,
    }
}

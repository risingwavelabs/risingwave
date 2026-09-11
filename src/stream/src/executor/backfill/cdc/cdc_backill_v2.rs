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

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

use either::Either;
use futures::stream::select_with_strategy;
use futures::{Stream, stream};
use itertools::Itertools;
use risingwave_common::bitmap::BitmapBuilder;
use risingwave_common::catalog::{ColumnDesc, Field};
use risingwave_common::row::RowDeserializer;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::sort_util::{OrderType, cmp_datum};
use risingwave_connector::source::cdc::CdcScanOptions;
use risingwave_connector::source::cdc::external::{
    CdcOffset, ExternalCdcTableType, ExternalTableReaderImpl,
};
use risingwave_connector::source::{CdcTableSnapshotSplit, CdcTableSnapshotSplitRaw};
use rw_futures_util::pausable;
use thiserror_ext::AsReport;
use tracing::Instrument;

use crate::executor::backfill::cdc::cdc_backfill::{
    get_cdc_json_parse_handling_from_properties, transform_upstream,
};
use crate::executor::backfill::cdc::state_v2::{CdcStateRecord, ParallelizedCdcBackfillState};
use crate::executor::backfill::cdc::upstream_table::external::ExternalStorageTable;
use crate::executor::backfill::cdc::upstream_table::snapshot::{
    SplitSnapshotReadArgs, UpstreamTableRead, UpstreamTableReader,
};
use crate::executor::backfill::utils::{get_cdc_chunk_last_offset, mapping_chunk, mapping_message};
use crate::executor::prelude::*;
use crate::executor::source::get_infinite_backoff_strategy;
use crate::task::cdc_progress::CdcProgressReporter;
use crate::task::{ActorId, FragmentId};

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

    properties: BTreeMap<String, String>,

    progress: Option<CdcProgressReporter>,
}

enum SnapshotAttemptState {
    Reading,
    Failed,
    Finished(CdcOffset),
}

impl<S: StateStore> ParallelizedCdcBackfillExecutor<S> {
    #[expect(clippy::too_many_arguments)]
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
        properties: BTreeMap<String, String>,
        progress: Option<CdcProgressReporter>,
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
            properties,
            progress,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        assert!(!self.options.disable_backfill);
        // The indices to primary key columns
        let pk_indices = self.external_table.pk_indices().to_vec();
        let table_id = self.external_table.table_id();
        let upstream_table_name = self.external_table.qualified_table_name();
        let schema_table_name = self.external_table.schema_table_name().clone();
        let external_database_name = self.external_table.database_name().to_owned();
        let additional_columns = self
            .output_columns
            .iter()
            .filter(|col| col.additional_column.column_type.is_some())
            .cloned()
            .collect_vec();
        assert!(
            (self.options.backfill_split_pk_column_index as usize) < pk_indices.len(),
            "split pk column index {} out of bound",
            self.options.backfill_split_pk_column_index
        );
        let snapshot_split_column_index =
            pk_indices[self.options.backfill_split_pk_column_index as usize];
        let snapshot_split_column_in_output_index = self
            .output_indices
            .iter()
            .position(|&idx| idx == snapshot_split_column_index)
            .expect("snapshot split column must be present in CDC backfill output");
        let cdc_table_snapshot_split_column =
            vec![self.external_table.schema().fields[snapshot_split_column_index].clone()];

        let mut upstream = self.upstream.execute();
        // Poll the upstream to get the first barrier.
        let first_barrier = expect_first_barrier(&mut upstream).await?;
        // Make sure to use mapping_message after transform_upstream.

        // If user sets debezium.time.precision.mode to "connect", it means the user can guarantee
        // that the upstream data precision is MilliSecond. In this case, we don't use GuessNumberUnit
        // mode to guess precision, but use Milli mode directly, which can handle extreme timestamps.
        let (timestamp_handling, timestamptz_handling, time_handling, bigint_unsigned_handling) =
            get_cdc_json_parse_handling_from_properties(&self.properties);
        // Only postgres-cdc connector may trigger TOAST.
        let handle_toast_columns: bool =
            self.external_table.table_type() == &ExternalCdcTableType::Postgres;
        let mut upstream = transform_upstream(
            upstream,
            self.output_columns.clone(),
            timestamp_handling,
            timestamptz_handling,
            time_handling,
            bigint_unsigned_handling,
            handle_toast_columns,
        )
        .boxed();
        let mut next_reset_barrier = Some(first_barrier);
        let mut is_reset = false;
        let mut state_impl = ParallelizedCdcBackfillState::new(self.state_table);
        // The buffered chunks have already been mapped.
        let mut upstream_chunk_buffer: Vec<StreamChunk> = vec![];

        // Need reset on CDC table snapshot splits reschedule.
        'with_cdc_table_snapshot_splits: loop {
            assert!(upstream_chunk_buffer.is_empty());
            let reset_barrier = next_reset_barrier.take().unwrap();
            let all_snapshot_splits = match reset_barrier.mutation.as_deref() {
                Some(Mutation::Add(add)) => &add.actor_cdc_table_snapshot_splits.splits,

                Some(Mutation::Update(update)) => &update.actor_cdc_table_snapshot_splits.splits,
                _ => {
                    return Err(anyhow::anyhow!("ParallelizedCdcBackfillExecutor expects either Mutation::Add or Mutation::Update to initialize CDC table snapshot splits.").into());
                }
            };
            let mut actor_snapshot_splits = vec![];
            let mut generation = None;
            // TODO(zw): optimization: remove consumed splits to reduce barrier size for downstream.
            if let Some((splits, snapshot_generation)) = all_snapshot_splits.get(&self.actor_ctx.id)
            {
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
                generation = Some(*snapshot_generation);
            }
            tracing::debug!(?actor_snapshot_splits, ?generation, "actor splits");
            assert_consecutive_splits(&actor_snapshot_splits);

            let mut is_snapshot_paused = reset_barrier.is_pause_on_startup();
            let barrier_epoch = reset_barrier.epoch;
            yield Message::Barrier(reset_barrier);
            if !is_reset {
                state_impl.init_epoch(barrier_epoch).await?;
                is_reset = true;
                tracing::info!(%table_id, "Initialize executor.");
            } else {
                tracing::info!(%table_id, "Reset executor.");
            }

            let mut current_actor_bounds = None;
            let mut actor_cdc_offset_high: Option<CdcOffset> = None;
            let mut actor_cdc_offset_low: Option<CdcOffset> = None;
            let mut split_states = Vec::with_capacity(actor_snapshot_splits.len());

            for split in &actor_snapshot_splits {
                let state = state_impl.restore_state(split.split_id).await?;
                extends_current_actor_bound(&mut current_actor_bounds, split);

                if state.is_finished {
                    if let Some(cdc_offset) = state.cdc_offset_low.as_ref()
                        && actor_cdc_offset_low
                            .as_ref()
                            .is_none_or(|current| current > cdc_offset)
                    {
                        actor_cdc_offset_low = Some(cdc_offset.clone());
                    }

                    if let Some(cdc_offset) = state.cdc_offset_high.as_ref()
                        && actor_cdc_offset_high
                            .as_ref()
                            .is_none_or(|current| current < cdc_offset)
                    {
                        actor_cdc_offset_high = Some(cdc_offset.clone());
                    }
                }

                split_states.push(state);
            }

            let next_split_idx = split_states.iter().position(|state| !state.is_finished);
            let finished_prefix_len = next_split_idx.unwrap_or(actor_snapshot_splits.len());
            let mut should_report_actor_backfill_progress = (finished_prefix_len > 0).then(|| {
                (
                    actor_snapshot_splits[0].split_id,
                    actor_snapshot_splits[finished_prefix_len - 1].split_id,
                )
            });

            for split in &actor_snapshot_splits {
                // Initialize state so that overall progress can be measured.
                state_impl.init_state_if_absent(split.split_id).await?;
            }

            let offset_parse_func = self.external_table.table_type().get_cdc_offset_parser()?;

            // A reader is only needed while at least one assigned snapshot split is unfinished.
            // Once all splits are complete, the executor only forwards the table-filtered CDC
            // stream and must not depend on the upstream snapshot table still existing.
            if let Some(next_split_idx) = next_split_idx {
                let external_table = self.external_table.clone();
                let actor_id = self.actor_ctx.id;
                let fragment_id = self.actor_ctx.fragment_id;
                let mut future = Box::pin(create_table_reader_with_retry(
                    external_table,
                    actor_id,
                    fragment_id,
                ));

                let next_split = &actor_snapshot_splits[next_split_idx];
                let table_reader = loop {
                    match build_reader_and_poll_upstream(&mut upstream, &mut future).await? {
                        Either::Left(msg) => match msg {
                            Message::Barrier(barrier) => {
                                for chunk in &upstream_chunk_buffer {
                                    yield Message::Chunk(chunk.clone());
                                }

                                let next_split_state = &split_states[next_split_idx];
                                state_impl
                                    .mutate_state(
                                        next_split.split_id,
                                        false,
                                        next_split_state.row_count as u64,
                                        next_split_state.cdc_offset_low.clone(),
                                        None,
                                    )
                                    .await?;
                                state_impl.commit_state(barrier.epoch).await?;

                                if is_reset_barrier(&barrier, self.actor_ctx.id) {
                                    upstream_chunk_buffer.clear();
                                    next_reset_barrier = Some(barrier);
                                    continue 'with_cdc_table_snapshot_splits;
                                }
                                yield Message::Barrier(barrier);
                            }
                            Message::Chunk(chunk) => {
                                if !chunk.has_visible_rows() {
                                    continue;
                                }

                                let chunk = mapping_chunk(chunk, &self.output_indices);
                                let (forwarded_chunk, buffered_chunk) = route_cdc_chunk(
                                    chunk,
                                    &actor_snapshot_splits,
                                    &split_states,
                                    next_split_idx,
                                    snapshot_split_column_in_output_index,
                                );

                                if let Some(forwarded_chunk) = forwarded_chunk {
                                    yield Message::Chunk(forwarded_chunk);
                                }

                                if let Some(buffered_chunk) = buffered_chunk {
                                    upstream_chunk_buffer.push(buffered_chunk);
                                }
                            }
                            Message::Watermark(_) => {
                                // Ignore watermark, like the `CdcBackfillExecutor`.
                            }
                        },
                        Either::Right(table_reader) => break table_reader,
                    }
                };
                tracing::info!(
                    %table_id,
                    upstream_table_name,
                    "table reader created successfully"
                );

                let mut upstream_table_reader =
                    UpstreamTableReader::new(self.external_table.clone(), table_reader);

                // Backfill snapshot splits sequentially.
                for (split_idx, (split, restored_state)) in actor_snapshot_splits
                    .iter()
                    .zip_eq_fast(split_states.iter())
                    .enumerate()
                    .skip(next_split_idx)
                {
                    if restored_state.is_finished {
                        extend_backfill_progress(
                            &mut should_report_actor_backfill_progress,
                            split.split_id,
                        );

                        continue;
                    }
                    tracing::info!(
                        %table_id,
                        upstream_table_name,
                        ?split,
                        is_snapshot_paused,
                        "start cdc backfill split"
                    );
                    let mut durable_row_count = restored_state.row_count as u64;
                    let mut split_cdc_offset_low = restored_state.cdc_offset_low.clone();

                    let split_cdc_offset_high = 'backfill_loop: loop {
                        // A rebuilt reader restarts this split from its lower bound, so count each
                        // snapshot attempt independently instead of accumulating duplicate reads.
                        let mut attempt_row_count = 0_u64;

                        if split_cdc_offset_low.is_none() {
                            static CDC_CONN_SEMAPHORE: tokio::sync::Semaphore =
                                tokio::sync::Semaphore::const_new(10);
                            let _permit = CDC_CONN_SEMAPHORE.acquire().await.unwrap();
                            split_cdc_offset_low =
                                upstream_table_reader.current_cdc_offset().await?;
                        }

                        if let Some(ref cdc_offset) = split_cdc_offset_low
                            && actor_cdc_offset_low
                                .as_ref()
                                .is_none_or(|cur| cur > cdc_offset)
                        {
                            actor_cdc_offset_low = split_cdc_offset_low.clone();
                        }

                        let attempt_state = {
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
                            let right_snapshot = pin!(
                                upstream_table_reader
                                    .snapshot_read_table_split(read_args)
                                    .map(Either::Right)
                            );
                            let (right_snapshot, snapshot_valve) = pausable(right_snapshot);

                            if is_snapshot_paused {
                                snapshot_valve.pause();
                            }

                            let mut backfill_stream = select_with_strategy(
                                left_upstream,
                                right_snapshot,
                                |_: &mut ()| stream::PollNext::Left,
                            );
                            let mut attempt_state = SnapshotAttemptState::Reading;

                            #[for_await]
                            'backfill_stream: for either in &mut backfill_stream {
                                match either {
                                    Either::Left(upstream_message) => match upstream_message? {
                                        Message::Barrier(barrier) => {
                                            // The source offset advances with this barrier, so make
                                            // every buffered change durable first. Keep the buffer
                                            // because a later snapshot row may still overwrite it.
                                            for chunk in &upstream_chunk_buffer {
                                                yield Message::Chunk(chunk.clone());
                                            }

                                            durable_row_count =
                                                durable_row_count.max(attempt_row_count);

                                            state_impl
                                                .mutate_state(
                                                    split.split_id,
                                                    false,
                                                    durable_row_count,
                                                    split_cdc_offset_low.clone(),
                                                    None,
                                                )
                                                .await?;
                                            state_impl.commit_state(barrier.epoch).await?;

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
                                                    Mutation::Throttle(_) => {
                                                        // TODO(zw): optimization: improve throttle.
                                                        // 1. Handle rate limit 0. Currently, to resume the process, the actor must be rebuilt.
                                                        // 2. Apply new rate limit immediately.
                                                        if let Some(entry) = mutation
                                                            .backfill_throttle_config(
                                                                self.actor_ctx.fragment_id,
                                                            )
                                                        {
                                                            // The new rate limit will take effect since next split.
                                                            self.rate_limit_rps = entry.rate_limit;
                                                        }
                                                    }
                                                    mutation
                                                        if mutation.is_stop(self.actor_ctx.id) =>
                                                    {
                                                        tracing::info!(
                                                            %table_id,
                                                            upstream_table_name,
                                                            "CdcBackfill has been dropped due to config change"
                                                        );

                                                        yield Message::Barrier(barrier);

                                                        let () = futures::future::pending().await;
                                                        unreachable!();
                                                    }
                                                    _ => (),
                                                }
                                            }

                                            if is_reset_barrier(&barrier, self.actor_ctx.id) {
                                                // the cdc events belonging to this split no longer matters,
                                                // since a new snapshot will cover the latest state
                                                upstream_chunk_buffer.clear();
                                                next_reset_barrier = Some(barrier);

                                                // restart to apply new split state
                                                continue 'with_cdc_table_snapshot_splits;
                                            }

                                            if let (Some(split_range), Some(progress)) = (
                                                should_report_actor_backfill_progress.take(),
                                                self.progress.as_ref(),
                                            ) {
                                                progress.update(
                                                    self.actor_ctx.fragment_id,
                                                    self.actor_ctx.id,
                                                    barrier.epoch,
                                                    generation.expect("should have set generation when having progress to report"),
                                                    split_range,
                                                );
                                            }

                                            yield Message::Barrier(barrier);

                                            if matches!(attempt_state, SnapshotAttemptState::Failed)
                                            {
                                                break 'backfill_stream;
                                            }
                                        }
                                        Message::Chunk(chunk) => {
                                            if !chunk.has_visible_rows() {
                                                continue 'backfill_stream;
                                            }

                                            // TODO(zw): re-enable
                                            // let chunk_cdc_offset =
                                            //     get_cdc_chunk_last_offset(&offset_parse_func, &chunk)?;
                                            // if *self.external_table.table_type()
                                            //     == ExternalCdcTableType::Postgres
                                            //     && let Some(cur) = actor_cdc_offset_low.as_ref()
                                            //     && let Some(chunk_offset) = chunk_cdc_offset
                                            //     && chunk_offset < *cur
                                            // {
                                            //     continue;
                                            // }

                                            // emit chunks belonging to past splits which are processed
                                            let chunk = mapping_chunk(chunk, &self.output_indices);
                                            let (forwarded_chunk, buffered_chunk) = route_cdc_chunk(
                                                chunk,
                                                &actor_snapshot_splits,
                                                &split_states,
                                                split_idx,
                                                snapshot_split_column_in_output_index,
                                            );

                                            if let Some(forwarded_chunk) = forwarded_chunk {
                                                yield Message::Chunk(forwarded_chunk);
                                            }

                                            if let Some(buffered_chunk) = buffered_chunk {
                                                upstream_chunk_buffer.push(buffered_chunk);
                                            }
                                        }
                                        Message::Watermark(_) => {
                                            // ignore watermark during backfill
                                        }
                                    },
                                    Either::Right(snapshot) => {
                                        // if snapshot already failed, continue polling upstream until barrier arrives to reconstruct reader
                                        if matches!(attempt_state, SnapshotAttemptState::Failed) {
                                            continue 'backfill_stream;
                                        }

                                        match snapshot {
                                            Ok(None) => {
                                                tracing::info!(
                                                    %table_id,
                                                    split_id = split.split_id,
                                                    "snapshot read stream ends"
                                                );

                                                for chunk in upstream_chunk_buffer.drain(..) {
                                                    yield Message::Chunk(chunk);
                                                }

                                                // This attempt scanned the whole split, so its count
                                                // is exact even if earlier attempts read duplicate rows.
                                                durable_row_count = attempt_row_count;

                                                // Limit concurrent CDC connections globally to 10 using a semaphore.
                                                static CDC_CONN_SEMAPHORE: tokio::sync::Semaphore =
                                                    tokio::sync::Semaphore::const_new(10);
                                                let _permit =
                                                    CDC_CONN_SEMAPHORE.acquire().await.unwrap();
                                                let high = upstream_table_reader
                                                .current_cdc_offset()
                                                .await?
                                                .expect(
                                                    "CDC offset must be available after snapshot completion",
                                                );
                                                attempt_state =
                                                    SnapshotAttemptState::Finished(high);

                                                break 'backfill_stream;
                                            }
                                            Ok(Some(chunk)) => {
                                                attempt_row_count = attempt_row_count
                                                    .saturating_add(chunk.cardinality() as u64);

                                                yield Message::Chunk(mapping_chunk(
                                                    chunk,
                                                    &self.output_indices,
                                                ));
                                            }
                                            Err(error) => {
                                                attempt_state = SnapshotAttemptState::Failed;
                                                tracing::warn!(
                                                    error = %error.as_report(),
                                                    %table_id,
                                                    upstream_table_name,
                                                    "failed to read CDC snapshot; rebuilding reader after a barrier"
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                            attempt_state
                        };

                        match attempt_state {
                            SnapshotAttemptState::Finished(split_cdc_offset_high) => {
                                break 'backfill_loop split_cdc_offset_high;
                            }
                            SnapshotAttemptState::Failed => {
                                if let Err(error) = upstream_table_reader.disconnect().await {
                                    tracing::warn!(
                                        error = %error.as_report(),
                                        %table_id,
                                        upstream_table_name,
                                        "failed to disconnect CDC snapshot reader; continuing with rebuild"
                                    );
                                }

                                let mut future = Box::pin(create_table_reader_with_retry(
                                    self.external_table.clone(),
                                    self.actor_ctx.id,
                                    self.actor_ctx.fragment_id,
                                ));
                                let table_reader = loop {
                                    match build_reader_and_poll_upstream(&mut upstream, &mut future)
                                        .await?
                                    {
                                        Either::Left(msg) => match msg {
                                            Message::Barrier(barrier) => {
                                                for chunk in &upstream_chunk_buffer {
                                                    yield Message::Chunk(chunk.clone());
                                                }

                                                state_impl
                                                    .mutate_state(
                                                        split.split_id,
                                                        false,
                                                        durable_row_count,
                                                        split_cdc_offset_low.clone(),
                                                        None,
                                                    )
                                                    .await?;
                                                state_impl.commit_state(barrier.epoch).await?;

                                                if let Some(mutation) = barrier.mutation.as_deref()
                                                {
                                                    use crate::executor::Mutation;
                                                    match mutation {
                                                        Mutation::Pause => {
                                                            is_snapshot_paused = true;
                                                        }
                                                        Mutation::Resume => {
                                                            is_snapshot_paused = false;
                                                        }
                                                        Mutation::Throttle(_) => {
                                                            if let Some(entry) = mutation
                                                                .backfill_throttle_config(
                                                                    self.actor_ctx.fragment_id,
                                                                )
                                                            {
                                                                self.rate_limit_rps =
                                                                    entry.rate_limit;
                                                            }
                                                        }
                                                        mutation
                                                            if mutation
                                                                .is_stop(self.actor_ctx.id) =>
                                                        {
                                                            yield Message::Barrier(barrier);
                                                            let () =
                                                                futures::future::pending().await;
                                                            unreachable!();
                                                        }
                                                        _ => (),
                                                    }
                                                }

                                                if is_reset_barrier(&barrier, self.actor_ctx.id) {
                                                    upstream_chunk_buffer.clear();
                                                    next_reset_barrier = Some(barrier);

                                                    continue 'with_cdc_table_snapshot_splits;
                                                }

                                                yield Message::Barrier(barrier);
                                            }
                                            Message::Chunk(chunk) => {
                                                let chunk =
                                                    mapping_chunk(chunk, &self.output_indices);
                                                let (forwarded_chunk, buffered_chunk) =
                                                    route_cdc_chunk(
                                                        chunk,
                                                        &actor_snapshot_splits,
                                                        &split_states,
                                                        split_idx,
                                                        snapshot_split_column_in_output_index,
                                                    );

                                                if let Some(forwarded_chunk) = forwarded_chunk {
                                                    yield Message::Chunk(forwarded_chunk);
                                                }

                                                if let Some(buffered_chunk) = buffered_chunk {
                                                    upstream_chunk_buffer.push(buffered_chunk);
                                                }
                                            }
                                            Message::Watermark(_) => {}
                                        },
                                        Either::Right(table_reader) => {
                                            break table_reader;
                                        }
                                    }
                                };

                                upstream_table_reader = UpstreamTableReader::new(
                                    self.external_table.clone(),
                                    table_reader,
                                );

                                tracing::info!(
                                    %table_id,
                                    upstream_table_name,
                                    "CDC table reader rebuilt successfully"
                                );

                                continue 'backfill_loop;
                            }
                            SnapshotAttemptState::Reading => {
                                unreachable!(
                                    "backfill stream must not end while the snapshot attempt is still reading"
                                );
                            }
                        }
                    };

                    if actor_cdc_offset_high
                        .as_ref()
                        .is_none_or(|cur| cur < &split_cdc_offset_high)
                    {
                        actor_cdc_offset_high = Some(split_cdc_offset_high.clone());
                    }

                    // Mark current split backfill as finished. The state will be persisted by next barrier.
                    state_impl
                        .mutate_state(
                            split.split_id,
                            true,
                            durable_row_count,
                            split_cdc_offset_low.clone(),
                            Some(split_cdc_offset_high),
                        )
                        .await?;

                    extend_backfill_progress(
                        &mut should_report_actor_backfill_progress,
                        split.split_id,
                    );
                }

                upstream_table_reader.disconnect().await?;
            }
            tracing::info!(
                %table_id,
                upstream_table_name,
                "CdcBackfill has already finished and will forward messages directly to the downstream"
            );

            let mut should_report_actor_backfill_done = false;
            // After backfill progress finished
            // we can forward messages directly to the downstream,
            // as backfill is finished.
            #[for_await]
            for msg in &mut upstream {
                let msg = msg?;
                match msg {
                    Message::Barrier(barrier) => {
                        state_impl.commit_state(barrier.epoch).await?;
                        if is_reset_barrier(&barrier, self.actor_ctx.id) {
                            next_reset_barrier = Some(barrier);
                            continue 'with_cdc_table_snapshot_splits;
                        }
                        if let Some(split_range) = should_report_actor_backfill_progress.take()
                            && let Some(ref progress) = self.progress
                        {
                            progress.update(
                                self.actor_ctx.fragment_id,
                                self.actor_ctx.id,
                                barrier.epoch,
                                generation.expect(
                                    "should have set generation when having progress to report",
                                ),
                                split_range,
                            );
                        }
                        if should_report_actor_backfill_done {
                            should_report_actor_backfill_done = false;
                            assert!(!actor_snapshot_splits.is_empty());
                            if let Some(ref progress) = self.progress {
                                progress.finish(
                                    self.actor_ctx.fragment_id,
                                    self.actor_ctx.id,
                                    barrier.epoch,
                                    generation.expect(
                                        "should have set generation when having progress to report",
                                    ),
                                    (
                                        actor_snapshot_splits[0].split_id,
                                        actor_snapshot_splits[actor_snapshot_splits.len() - 1]
                                            .split_id,
                                    ),
                                );
                            }
                        }
                        yield Message::Barrier(barrier);
                    }
                    Message::Chunk(chunk) => {
                        if actor_snapshot_splits.is_empty() || !chunk.has_visible_rows() {
                            continue;
                        }

                        let chunk_cdc_offset =
                            get_cdc_chunk_last_offset(&offset_parse_func, &chunk)?;
                        // // TODO(zw): re-enable
                        // if *self.external_table.table_type() == ExternalCdcTableType::Postgres
                        //     && let Some(cur) = actor_cdc_offset_low.as_ref()
                        //     && let Some(ref chunk_offset) = chunk_cdc_offset
                        //     && *chunk_offset < *cur
                        // {
                        //     continue;
                        // }

                        // should_report_actor_backfill_done is set to true at most once.
                        if let Some(high) = actor_cdc_offset_high.as_ref() {
                            if state_impl.is_legacy_state() {
                                // Since the legacy state does not track CDC offsets, report backfill completion immediately.
                                actor_cdc_offset_high = None;
                                should_report_actor_backfill_done = true;
                            } else if let Some(ref chunk_offset) = chunk_cdc_offset
                                && *chunk_offset >= *high
                            {
                                // Report backfill completion once the latest CDC offset exceeds the highest offset tracked during the backfill.
                                actor_cdc_offset_high = None;
                                should_report_actor_backfill_done = true;
                            }
                        }
                        let chunk = mapping_chunk(chunk, &self.output_indices);
                        if let Some(filtered_chunk) = filter_stream_chunk(
                            chunk,
                            &current_actor_bounds,
                            snapshot_split_column_in_output_index,
                        ) {
                            yield Message::Chunk(filtered_chunk);
                        }
                    }
                    msg @ Message::Watermark(_) => {
                        if let Some(msg) = mapping_message(msg, &self.output_indices) {
                            yield msg;
                        }
                    }
                }
            }
        }
    }
}

/// Splits CDC rows into chunks to forward immediately or buffer for replay.
/// Returns `None` for either result when it contains no rows.
///
/// Rows in the active split are always buffered because later snapshot rows may overwrite them.
///
/// Rows in inactive splits are routed according to their durable snapshot progress:
/// - not started: discard, because a future snapshot will observe the latest state;
/// - finished: forward, because no future snapshot will cover the change;
/// - partially written: forward, because some rows may already exist downstream and, without a
///   cursor, we cannot determine which side of the durable snapshot progress contains this row.
fn route_cdc_chunk(
    chunk: StreamChunk,
    splits: &[CdcTableSnapshotSplit],
    states: &[CdcStateRecord],
    current_split_idx: usize,
    snapshot_split_column_index: usize,
) -> (Option<StreamChunk>, Option<StreamChunk>) {
    let mut forwarded = BitmapBuilder::zeroed(chunk.capacity());
    let mut buffered = BitmapBuilder::zeroed(chunk.capacity());

    for (_, row) in chunk.rows() {
        let split_key = row.datum_at(snapshot_split_column_index);

        let Ok(split_idx) = splits.binary_search_by(|split| {
            let right_bound_le_key = !is_rightmost_bound(&split.right_bound_exclusive)
                && cmp_datum(
                    split.right_bound_exclusive.datum_at(0),
                    split_key,
                    OrderType::ascending_nulls_first(),
                )
                .is_le();

            let left_bound_gt_key = !is_leftmost_bound(&split.left_bound_inclusive)
                && cmp_datum(
                    split.left_bound_inclusive.datum_at(0),
                    split_key,
                    OrderType::ascending_nulls_first(),
                )
                .is_gt();

            if right_bound_le_key {
                // The split is entirely before the key, so search to the right.
                std::cmp::Ordering::Less
            } else if left_bound_gt_key {
                // The split is entirely after the key, so search to the left.
                std::cmp::Ordering::Greater
            } else {
                // The split contains the key: left <= key < right.
                std::cmp::Ordering::Equal
            }
        }) else {
            // The key is outside the assigned split ranges or lies in a gap.
            continue;
        };

        // Buffer rows in the active split for replay after any later snapshot output.
        if split_idx == current_split_idx {
            buffered.set(row.index(), true);
            continue;
        }

        let state = &states[split_idx];
        // A split before the active split is finished by construction. Splits after it may also
        // be finished or partially written after scaling, for example
        // [finished, active, finished, partially written, not started].
        let is_completed = split_idx < current_split_idx || state.is_finished;
        let is_partially_completed = state.row_count > 0;
        if is_completed || is_partially_completed {
            forwarded.set(row.index(), true);
        }

        // Otherwise leave both bits unset. The split has no durable snapshot output, so its future
        // full snapshot will cover the change.
    }

    let forwarded = forwarded.finish();
    let forwarded_chunk = forwarded
        .any()
        .then(|| chunk.clone_with_vis(forwarded).compact_vis());

    let buffered = buffered.finish();
    let buffered_chunk = buffered
        .any()
        .then(|| chunk.clone_with_vis(buffered).compact_vis());

    (forwarded_chunk, buffered_chunk)
}

fn extend_backfill_progress(progress: &mut Option<(i64, i64)>, split_id: i64) {
    if let Some((_, right)) = progress {
        assert!(*right < split_id);
        *right = split_id;
    } else {
        *progress = Some((split_id, split_id));
    }
}

/// Keep rows whose snapshot split-column value falls within `bound`'s half-open range
/// `[left, right)`, preserving their operations and relative order through a visibility bitmap.
/// Returns `None` when no bounds are supplied or no visible rows fall within the range.
///
/// For example, filtering split keys `[50, 100, 150, 200]` with bounds `[100, 200)` keeps
/// `[100, 150]`: the left bound is inclusive and the right bound is exclusive.
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
        return chunk.has_visible_rows().then_some(chunk);
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
                OrderType::ascending_nulls_first(),
            )
            .is_lt();
        }
        if !is_in_range {
            tracing::trace!(?row_split_key, ?left_split_key, ?right_split_key, snapshot_split_column_index, data_type = ?columns[snapshot_split_column_index].data_type(), "filter out row")
        }
        new_bitmap.append(is_in_range);
    }

    let visibility = new_bitmap.finish();

    visibility
        .any()
        .then_some(StreamChunk::with_visibility(ops, columns, visibility))
}

// has no left bound, e.g. [-inf, N)
fn is_leftmost_bound(row: &OwnedRow) -> bool {
    row.iter().all(|d| d.is_none())
}

// has no right bound, e.g. [N, inf)
fn is_rightmost_bound(row: &OwnedRow) -> bool {
    row.iter().all(|d| d.is_none())
}

async fn build_reader_and_poll_upstream(
    upstream: &mut (impl Stream<Item = StreamExecutorResult<Message>> + Unpin),
    future: &mut Pin<Box<impl Future<Output = ExternalTableReaderImpl>>>,
) -> StreamExecutorResult<Either<Message, ExternalTableReaderImpl>> {
    tokio::select! {
        biased;
        reader = &mut *future => Ok(Either::Right(reader)),
        msg = upstream.next() => {
            msg.transpose()?
                .map(Either::Left)
                .ok_or_else(|| anyhow::anyhow!(
                    "upstream closed while creating CDC table reader"
                ).into())
        }
    }
}

async fn create_table_reader_with_retry(
    external_table: ExternalStorageTable,
    actor_id: ActorId,
    fragment_id: FragmentId,
) -> ExternalTableReaderImpl {
    let backoff = get_infinite_backoff_strategy();

    tokio_retry::Retry::spawn(backoff, || async {
        match external_table.create_table_reader().await {
            Ok(reader) => Ok(reader),
            Err(error) => {
                tracing::warn!(
                    error = %error.as_report(),
                    actor_id = %actor_id,
                    fragment_id = %fragment_id,
                    "failed to create CDC table reader; retrying"
                );
                Err(error)
            }
        }
    })
    .instrument(tracing::info_span!("create_cdc_table_reader_with_retry"))
    .await
    .expect("retry creating CDC table reader until success")
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
            .splits
            .contains_key(&actor_id),
        _ => false,
    }
}

fn assert_consecutive_splits(actor_snapshot_splits: &[CdcTableSnapshotSplit]) {
    for i in 1..actor_snapshot_splits.len() {
        assert_eq!(
            actor_snapshot_splits[i].split_id,
            actor_snapshot_splits[i - 1].split_id + 1,
            "{:?}",
            actor_snapshot_splits
        );
        assert!(
            cmp_datum(
                actor_snapshot_splits[i - 1]
                    .right_bound_exclusive
                    .datum_at(0),
                actor_snapshot_splits[i].right_bound_exclusive.datum_at(0),
                OrderType::ascending_nulls_last(),
            )
            .is_lt()
        );
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    use futures::StreamExt;
    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
    use risingwave_common::row::{OwnedRow, Row};
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_common::util::epoch::{EpochExt, test_epoch};
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_connector::source::CdcTableSnapshotSplitRaw;
    use risingwave_connector::source::cdc::external::{
        ExternalCdcTableType, ExternalTableConfig, SchemaTableName,
    };
    use risingwave_connector::source::cdc::{
        CdcScanOptions, CdcTableSnapshotSplitAssignmentWithGeneration,
    };
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::common::table::state_table::StateTable;
    use crate::common::table::test_utils::gen_pbtable;
    use crate::executor::monitor::StreamingMetrics;
    use crate::executor::test_utils::MockSource;
    use crate::executor::{AddMutation, Execute, Mutation};

    async fn create_parallel_cdc_state_table(
        store: MemoryStateStore,
    ) -> StateTable<MemoryStateStore> {
        let state_schema = Schema::new(vec![
            Field::with_name(DataType::Int64, "split_id"),
            Field::with_name(DataType::Boolean, "backfill_finished"),
            Field::with_name(DataType::Int64, "row_count"),
            Field::with_name(DataType::Jsonb, "cdc_offset_low"),
            Field::with_name(DataType::Jsonb, "cdc_offset_high"),
        ]);
        let column_descs = state_schema
            .fields
            .iter()
            .enumerate()
            .map(|(idx, field)| {
                ColumnDesc::unnamed(ColumnId::new(idx as i32), field.data_type.clone())
            })
            .collect();

        StateTable::from_table_catalog(
            &gen_pbtable(
                TableId::new(0x42),
                column_descs,
                vec![OrderType::ascending()],
                vec![0],
                0,
            ),
            store,
            None,
        )
        .await
    }

    #[tokio::test(start_paused = true)]
    async fn test_rebuilds_reader_after_snapshot_error() {
        let (mut tx, source) = MockSource::channel();

        let source = source.into_executor(
            Schema::new(vec![
                Field::unnamed(DataType::Jsonb),
                Field::unnamed(DataType::Varchar),
            ]),
            vec![0],
        );
        let external_table = ExternalStorageTable::new(
            TableId::new(1234),
            SchemaTableName {
                schema_name: "public".to_owned(),
                table_name: "mock_table".to_owned(),
            },
            "mydb".to_owned(),
            ExternalTableConfig::default(),
            ExternalCdcTableType::Mock,
            Schema::new(vec![
                Field::with_name(DataType::Int64, "id"),
                Field::with_name(DataType::Float64, "price"),
            ]),
            vec![OrderType::ascending()],
            vec![0],
        )
        .with_mock_snapshot_errors([1, 0]);
        let external_table_for_assertion = external_table.clone();

        let state_table = create_parallel_cdc_state_table(MemoryStateStore::new()).await;

        let actor_id = 0x1a.into();
        let mut executor = ParallelizedCdcBackfillExecutor::new(
            ActorContext::for_test(actor_id),
            external_table,
            source,
            vec![0, 1],
            vec![
                ColumnDesc::named("id", ColumnId::new(1), DataType::Int64),
                ColumnDesc::named("price", ColumnId::new(2), DataType::Float64),
            ],
            Arc::new(StreamingMetrics::unused()),
            state_table,
            Some(4),
            CdcScanOptions {
                backfill_parallelism: 1,
                backfill_num_rows_per_split: 100,
                ..Default::default()
            },
            BTreeMap::new(),
            None,
        )
        .boxed()
        .execute();

        let mut curr_epoch = test_epoch(1);
        let snapshot_splits = [(
            actor_id,
            (
                vec![CdcTableSnapshotSplitRaw {
                    split_id: 1,
                    left_bound_inclusive: OwnedRow::new(vec![Some(ScalarImpl::Int64(1))])
                        .value_serialize(),
                    right_bound_exclusive: OwnedRow::new(vec![Some(ScalarImpl::Int64(10))])
                        .value_serialize(),
                }],
                10,
            ),
        )]
        .into_iter()
        .collect();

        // Assign the split to this actor on the initial barrier.
        tx.send_barrier(
            Barrier::new_test_barrier(curr_epoch).with_mutation(Mutation::Add(AddMutation {
                actor_cdc_table_snapshot_splits: CdcTableSnapshotSplitAssignmentWithGeneration {
                    splits: snapshot_splits,
                },
                ..Default::default()
            })),
        );

        assert!(matches!(
            executor.next().await.unwrap().unwrap(),
            Message::Barrier(Barrier { epoch, .. }) if epoch.curr == curr_epoch
        ));

        // This poll creates reader 1 and starts the snapshot, consuming its injected error. The
        // executor then keeps polling the CDC upstream until a barrier provides a safe recovery
        // boundary.
        assert!(
            tokio::time::timeout(Duration::from_millis(50), executor.next())
                .await
                .is_err()
        );
        assert_eq!(external_table_for_assertion.mock_reader_create_count(), 1);

        curr_epoch.inc_epoch();
        tx.push_barrier(curr_epoch, false);
        assert!(matches!(
            executor.next().await.unwrap().unwrap(),
            Message::Barrier(Barrier { epoch, .. }) if epoch.curr == curr_epoch
        ));

        // Resuming after the barrier creates reader 2 and produces a snapshot chunk.
        assert!(matches!(
            executor.next().await.unwrap().unwrap(),
            Message::Chunk(_)
        ));

        assert_eq!(external_table_for_assertion.mock_reader_create_count(), 2);
    }

    #[test]
    fn test_filter_stream_chunk() {
        use risingwave_common::array::StreamChunkTestExt;
        let chunk = StreamChunk::from_pretty(
            "  I I
             + 1 6
             - 2 .
            U- 3 7
            U+ 4 .",
        );
        let bound = None;
        let c = filter_stream_chunk(chunk.clone(), &bound, 0);
        assert!(c.is_none());

        let bound = Some((OwnedRow::new(vec![None]), OwnedRow::new(vec![None])));
        let c = filter_stream_chunk(chunk.clone(), &bound, 0);
        assert_eq!(c.unwrap().compact_vis(), chunk);

        let bound = Some((
            OwnedRow::new(vec![None]),
            OwnedRow::new(vec![Some(ScalarImpl::Int64(3))]),
        ));
        let c = filter_stream_chunk(chunk.clone(), &bound, 0);
        assert_eq!(
            c.unwrap().compact_vis(),
            StreamChunk::from_pretty(
                "  I I
             + 1 6
             - 2 .",
            )
        );

        let bound = Some((
            OwnedRow::new(vec![Some(ScalarImpl::Int64(3))]),
            OwnedRow::new(vec![None]),
        ));
        let c = filter_stream_chunk(chunk.clone(), &bound, 0);
        assert_eq!(
            c.unwrap().compact_vis(),
            StreamChunk::from_pretty(
                "  I I
            U- 3 7
            U+ 4 .",
            )
        );

        let bound = Some((
            OwnedRow::new(vec![Some(ScalarImpl::Int64(2))]),
            OwnedRow::new(vec![Some(ScalarImpl::Int64(4))]),
        ));
        let c = filter_stream_chunk(chunk.clone(), &bound, 0);
        assert_eq!(
            c.unwrap().compact_vis(),
            StreamChunk::from_pretty(
                "  I I
             - 2 .
            U- 3 7",
            )
        );

        // Test NULL value.
        let bound = None;
        let c = filter_stream_chunk(chunk.clone(), &bound, 1);
        assert!(c.is_none());

        let bound = Some((OwnedRow::new(vec![None]), OwnedRow::new(vec![None])));
        let c = filter_stream_chunk(chunk.clone(), &bound, 1);
        assert_eq!(c.unwrap().compact_vis(), chunk);

        let bound = Some((
            OwnedRow::new(vec![None]),
            OwnedRow::new(vec![Some(ScalarImpl::Int64(7))]),
        ));
        let c = filter_stream_chunk(chunk.clone(), &bound, 1);
        assert_eq!(
            c.unwrap().compact_vis(),
            StreamChunk::from_pretty(
                "  I I
             + 1 6
             - 2 .
            U+ 4 .",
            )
        );

        let bound = Some((
            OwnedRow::new(vec![Some(ScalarImpl::Int64(7))]),
            OwnedRow::new(vec![None]),
        ));
        let c = filter_stream_chunk(chunk, &bound, 1);
        assert_eq!(
            c.unwrap().compact_vis(),
            StreamChunk::from_pretty(
                "  I I
            U- 3 7",
            )
        );
    }

    #[test]
    fn test_route_cdc_chunk() {
        use risingwave_common::array::StreamChunkTestExt;

        let bound = |value: i64| OwnedRow::new(vec![Some(value.into())]);
        let splits = [(0, 100), (100, 200), (200, 300), (300, 400), (400, 500)]
            .into_iter()
            .enumerate()
            .map(|(idx, (left, right))| CdcTableSnapshotSplit {
                split_id: idx as i64,
                left_bound_inclusive: bound(left),
                right_bound_exclusive: bound(right),
            })
            .collect_vec();

        // [finished, active, finished, partially written, not started]
        let states = [
            CdcStateRecord {
                is_finished: true,
                ..Default::default()
            },
            CdcStateRecord {
                row_count: 1,
                ..Default::default()
            },
            CdcStateRecord {
                is_finished: true,
                ..Default::default()
            },
            CdcStateRecord {
                row_count: 1,
                ..Default::default()
            },
            CdcStateRecord::default(),
        ];

        // splits: [0, 100), [100, 200), [200, 300), [300, 400), [400, 500)
        // status: finished, active,       finished,     partial,      not started
        //
        // chunk       expected     split         status
        // + -1  1     dropped      outside       N/A
        // + 50  5     forwarded    [0, 100)      finished
        // + 150 15    buffered     [100, 200)    active
        // - 151 16    buffered     [100, 200)    active
        // - 250 25    forwarded    [200, 300)    finished
        // - 350 35    forwarded    [300, 400)    partially written
        // + 450 45    dropped      [400, 500)    not started
        // + 500 50    dropped      outside       N/A
        let chunk = StreamChunk::from_pretty(
            "  I I
             + -1 1
             + 50 5
             + 150 15
             - 151 16
             - 250 25
             - 350 35
             + 450 45
             + 500 50",
        );
        let (forwarded_chunk, buffered_chunk) = route_cdc_chunk(chunk, &splits, &states, 1, 0);

        assert_eq!(
            forwarded_chunk.unwrap(),
            StreamChunk::from_pretty(
                "  I I
                 + 50 5
                 - 250 25
                 - 350 35",
            )
        );

        assert_eq!(
            buffered_chunk.unwrap(),
            StreamChunk::from_pretty(
                "  I I
                 + 150 15
                 - 151 16",
            )
        );
    }
}

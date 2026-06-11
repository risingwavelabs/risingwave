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

use std::collections::HashMap;
use std::ops::Bound;

use futures::{StreamExt, pin_mut};
use risingwave_common::array::Op;
use risingwave_common::gap_fill::{
    FillStrategy, apply_interpolation_step, calculate_interpolation_step,
};
use risingwave_common::metrics::LabelGuardedIntCounter;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::{CheckedAdd, Datum, Interval, ScalarImpl, ToOwnedDatum};
use risingwave_common::util::epoch::EpochPair;
use risingwave_expr::expr::NonStrictExpression;
use risingwave_storage::StateStore;
use risingwave_storage::store::PrefetchOptions;
use tracing::warn;

use crate::common::table::state_table::{StateTable, StateTablePostCommit};
use crate::executor::prelude::*;

pub struct GapFillExecutorArgs<S: StateStore> {
    pub ctx: ActorContextRef,
    pub input: Executor,
    pub schema: Schema,
    pub chunk_size: usize,
    pub time_column_index: usize,
    pub fill_columns: HashMap<usize, FillStrategy>,
    pub gap_interval: NonStrictExpression,
    pub state_table: StateTable<S>,
    pub partition_by_indices: Vec<usize>,
    pub pointer_key_indices: Vec<usize>,
}

/// Only original (anchor) rows are persisted. Filled rows are computed on the fly.
///
/// State rows have the same layout as output rows. Neighbor lookups use the state table PK prefix:
/// `(partition_cols..., time_col, upstream stream key columns excluding partition/time)`.
pub struct ManagedGapFillState<S: StateStore> {
    state_table: StateTable<S>,
    partition_by_indices: Vec<usize>,
    pointer_key_indices: Vec<usize>,
}

impl<S: StateStore> ManagedGapFillState<S> {
    pub fn new(
        state_table: StateTable<S>,
        _schema: &Schema,
        partition_by_indices: Vec<usize>,
        pointer_key_indices: Vec<usize>,
    ) -> Self {
        assert!(
            !pointer_key_indices.is_empty(),
            "gap fill pointer key should not be empty",
        );

        Self {
            state_table,
            partition_by_indices,
            pointer_key_indices,
        }
    }

    pub async fn init_epoch(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.state_table.init_epoch(epoch).await
    }

    pub fn insert(&mut self, value: impl Row) {
        self.state_table.insert(value);
    }

    pub fn delete(&mut self, value: impl Row) {
        self.state_table.delete(value);
    }

    pub async fn flush(
        &mut self,
        epoch: EpochPair,
    ) -> StreamExecutorResult<StateTablePostCommit<'_, S>> {
        self.state_table.commit(epoch).await
    }

    pub async fn try_flush(&mut self) -> StreamExecutorResult<()> {
        self.state_table.try_flush().await
    }

    fn state_row_to_output_row(&self, state_row: impl Row) -> OwnedRow {
        state_row.into_owned_row()
    }

    /// Find the previous neighbor within the same partition by scanning backward.
    /// Uses the partition as prefix and scans rows with pointer key < target pointer key.
    async fn find_prev_in_partition(
        &self,
        partition_key: impl Row,
        target_pointer_key: impl Row,
    ) -> StreamExecutorResult<Option<OwnedRow>> {
        let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) = &(
            Bound::Unbounded,
            Bound::Excluded(target_pointer_key.into_owned_row()),
        );

        let iter = self
            .state_table
            .rev_iter_with_prefix(partition_key, sub_range, PrefetchOptions::default())
            .await?;
        pin_mut!(iter);

        if let Some(item) = iter.next().await {
            let state_row = item?.into_owned_row();
            Ok(Some(state_row))
        } else {
            Ok(None)
        }
    }

    /// Find the next neighbor within the same partition by scanning forward.
    async fn find_next_in_partition(
        &self,
        partition_key: impl Row,
        target_pointer_key: impl Row,
    ) -> StreamExecutorResult<Option<OwnedRow>> {
        let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) = &(
            Bound::Excluded(target_pointer_key.into_owned_row()),
            Bound::Unbounded,
        );

        let iter = self
            .state_table
            .iter_with_prefix(partition_key, sub_range, PrefetchOptions::default())
            .await?;
        pin_mut!(iter);

        if let Some(item) = iter.next().await {
            let state_row = item?.into_owned_row();
            Ok(Some(state_row))
        } else {
            Ok(None)
        }
    }
}

pub struct GapFillExecutor<S: StateStore> {
    ctx: ActorContextRef,
    input: Executor,
    schema: Schema,
    chunk_size: usize,
    time_column_index: usize,
    fill_columns: HashMap<usize, FillStrategy>,
    gap_interval: NonStrictExpression,

    // State management
    managed_state: ManagedGapFillState<S>,

    // Metrics
    metrics: GapFillMetrics,
}

pub struct GapFillMetrics {
    pub gap_fill_generated_rows_count: LabelGuardedIntCounter,
}

impl<S: StateStore> GapFillExecutor<S> {
    async fn find_prev_output(
        managed_state: &ManagedGapFillState<S>,
        partition_key: impl Row,
        pointer_key: impl Row,
    ) -> StreamExecutorResult<Option<OwnedRow>> {
        Ok(managed_state
            .find_prev_in_partition(partition_key, pointer_key)
            .await?
            .map(|sr| managed_state.state_row_to_output_row(sr)))
    }

    async fn find_next_output(
        managed_state: &ManagedGapFillState<S>,
        partition_key: impl Row,
        pointer_key: impl Row,
    ) -> StreamExecutorResult<Option<OwnedRow>> {
        Ok(managed_state
            .find_next_in_partition(partition_key, pointer_key)
            .await?
            .map(|sr| managed_state.state_row_to_output_row(sr)))
    }

    pub fn new(args: GapFillExecutorArgs<S>) -> Self {
        let managed_state = ManagedGapFillState::new(
            args.state_table,
            &args.schema,
            args.partition_by_indices,
            args.pointer_key_indices,
        );

        let metrics = args.ctx.streaming_metrics.clone();
        let actor_id = args.ctx.id.to_string();
        let fragment_id = args.ctx.fragment_id.to_string();
        let gap_fill_metrics = GapFillMetrics {
            gap_fill_generated_rows_count: metrics
                .gap_fill_generated_rows_count
                .with_guarded_label_values(&[&actor_id, &fragment_id]),
        };

        Self {
            ctx: args.ctx,
            input: args.input,
            schema: args.schema,
            chunk_size: args.chunk_size,
            time_column_index: args.time_column_index,
            fill_columns: args.fill_columns,
            gap_interval: args.gap_interval,
            managed_state,
            metrics: gap_fill_metrics,
        }
    }

    /// Generates interpolated rows between two time points (`prev_row` and `curr_row`) using a static interval.
    ///
    /// # Parameters
    /// - `prev_row`: Reference to the previous row (start of the gap).
    /// - `curr_row`: Reference to the current row (end of the gap).
    /// - `interval`: The interval to use for generating each filled row (typically a time interval).
    /// - `time_column_index`: The index of the time column in the row, used to increment time values.
    /// - `fill_columns`: A `HashMap` mapping column indices to their respective `FillStrategy`.
    /// - `metrics`: Metrics for tracking the number of generated rows.
    ///
    /// # Fill Strategy Application
    /// For each filled row, the function applies the specified `FillStrategy` for each column:
    /// - `FillStrategy::Locf`: Carries the previous row's value forward.
    /// - `FillStrategy::Interpolate`: Interpolates linearly between the previous and current row values.
    /// - `FillStrategy::Null`: Leaves the column null.
    ///
    /// Returns a vector of `OwnedRow` representing the filled rows between `prev_row` and `curr_row`.
    fn generate_filled_rows_between_static(
        prev_row: &OwnedRow,
        curr_row: &OwnedRow,
        interval: &risingwave_common::types::Interval,
        time_column_index: usize,
        partition_by_indices: &[usize],
        fill_columns: &HashMap<usize, FillStrategy>,
        metrics: &GapFillMetrics,
    ) -> StreamExecutorResult<Vec<OwnedRow>> {
        let mut filled_rows = Vec::new();

        let (Some(prev_time_scalar), Some(curr_time_scalar)) = (
            prev_row.datum_at(time_column_index),
            curr_row.datum_at(time_column_index),
        ) else {
            return Ok(filled_rows);
        };

        let prev_time = match prev_time_scalar {
            ScalarRefImpl::Timestamp(ts) => ts,
            ScalarRefImpl::Timestamptz(ts) => {
                match risingwave_common::types::Timestamp::with_micros(ts.timestamp_micros()) {
                    Ok(timestamp) => timestamp,
                    Err(_) => {
                        warn!("Failed to convert timestamptz to timestamp: {:?}", ts);
                        return Ok(filled_rows);
                    }
                }
            }
            _ => {
                warn!("Time column is not timestamp type: {:?}", prev_time_scalar);
                return Ok(filled_rows);
            }
        };

        let curr_time = match curr_time_scalar {
            ScalarRefImpl::Timestamp(ts) => ts,
            ScalarRefImpl::Timestamptz(ts) => {
                match risingwave_common::types::Timestamp::with_micros(ts.timestamp_micros()) {
                    Ok(timestamp) => timestamp,
                    Err(_) => {
                        warn!("Failed to convert timestamptz to timestamp: {:?}", ts);
                        return Ok(filled_rows);
                    }
                }
            }
            _ => {
                warn!("Time column is not timestamp type: {:?}", curr_time_scalar);
                return Ok(filled_rows);
            }
        };

        if prev_time >= curr_time {
            return Ok(filled_rows);
        }

        // Calculate the number of rows to be generated and validate
        let mut fill_time = match prev_time.checked_add(*interval) {
            Some(t) => t,
            None => {
                // If the interval is so large that adding it to prev_time causes overflow,
                // it means we shouldn't do gap fill at all.
                warn!(
                    "Gap fill interval is too large, causing timestamp overflow. \
                     No gap filling will be performed between {:?} and {:?}.",
                    prev_time, curr_time
                );
                return Ok(filled_rows);
            }
        };

        // Check if fill_time is already >= curr_time, which means no gap to fill
        if fill_time >= curr_time {
            return Ok(filled_rows);
        }

        // Count the number of rows to generate
        let mut row_count = 0;
        let mut temp_time = fill_time;
        while temp_time < curr_time {
            row_count += 1;
            temp_time = match temp_time.checked_add(*interval) {
                Some(t) => t,
                None => break,
            };
        }

        // Pre-compute interpolation steps for each column that requires interpolation
        let mut interpolation_steps: Vec<Option<ScalarImpl>> = Vec::new();
        let mut interpolation_states: Vec<Datum> = Vec::new();

        for i in 0..prev_row.len() {
            if let Some(strategy) = fill_columns.get(&i) {
                if matches!(strategy, FillStrategy::Interpolate) {
                    let step = calculate_interpolation_step(
                        prev_row.datum_at(i),
                        curr_row.datum_at(i),
                        row_count + 1,
                    );
                    interpolation_steps.push(step.clone());
                    interpolation_states.push(prev_row.datum_at(i).to_owned_datum());
                } else {
                    interpolation_steps.push(None);
                    interpolation_states.push(None);
                }
            } else {
                interpolation_steps.push(None);
                interpolation_states.push(None);
            }
        }

        // Generate filled rows, applying the appropriate strategy for each column
        while fill_time < curr_time {
            let mut new_row_data = Vec::with_capacity(prev_row.len());

            for col_idx in 0..prev_row.len() {
                let datum = if col_idx == time_column_index {
                    // Time column: use the incremented timestamp
                    let fill_time_scalar = match prev_time_scalar {
                        ScalarRefImpl::Timestamp(_) => ScalarImpl::Timestamp(fill_time),
                        ScalarRefImpl::Timestamptz(_) => {
                            let micros = fill_time.0.and_utc().timestamp_micros();
                            ScalarImpl::Timestamptz(
                                risingwave_common::types::Timestamptz::from_micros(micros),
                            )
                        }
                        _ => unreachable!("Time column should be Timestamp or Timestamptz"),
                    };
                    Some(fill_time_scalar)
                } else if partition_by_indices.contains(&col_idx) {
                    // Gap-filled rows must stay in the same partition as the surrounding anchors.
                    prev_row.datum_at(col_idx).to_owned_datum()
                } else if let Some(strategy) = fill_columns.get(&col_idx) {
                    // Apply the fill strategy for this column
                    match strategy {
                        FillStrategy::Locf => prev_row.datum_at(col_idx).to_owned_datum(),
                        FillStrategy::Null => None,
                        FillStrategy::Interpolate => {
                            // Apply interpolation step and update cumulative value
                            if let Some(step) = &interpolation_steps[col_idx] {
                                apply_interpolation_step(&mut interpolation_states[col_idx], step);
                                interpolation_states[col_idx].clone()
                            } else {
                                // If interpolation step is None, fill with NULL
                                None
                            }
                        }
                    }
                } else {
                    // No strategy specified, default to NULL. This can include upstream stream-key
                    // columns (for example hidden `_row_id` on no-PK inputs). The generated row is
                    // still distinct from both anchor rows because the time column is always part
                    // of the gap-fill output key and `fill_time` is strictly between them.
                    None
                };
                new_row_data.push(datum);
            }

            let filled_row = OwnedRow::new(new_row_data);
            debug_assert_ne!(
                filled_row.datum_at(time_column_index),
                prev_row.datum_at(time_column_index)
            );
            debug_assert_ne!(
                filled_row.datum_at(time_column_index),
                curr_row.datum_at(time_column_index)
            );
            filled_rows.push(filled_row);

            fill_time = match fill_time.checked_add(*interval) {
                Some(t) => t,
                None => {
                    // Time overflow during iteration, stop filling
                    warn!(
                        "Gap fill stopped due to timestamp overflow after generating {} rows.",
                        filled_rows.len()
                    );
                    break;
                }
            };
        }

        // Update metrics with the number of generated rows
        metrics
            .gap_fill_generated_rows_count
            .inc_by(filled_rows.len() as u64);

        Ok(filled_rows)
    }
}

impl<S: StateStore> Execute for GapFillExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

impl<S: StateStore> GapFillExecutor<S> {
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self: Box<Self>) {
        let Self {
            mut managed_state,
            schema,
            chunk_size,
            time_column_index,
            fill_columns,
            gap_interval,
            ctx,
            input,
            metrics,
        } = *self;

        let mut input = input.execute();

        let barrier = expect_first_barrier(&mut input).await?;
        let first_epoch = barrier.epoch;
        yield Message::Barrier(barrier);
        managed_state.init_epoch(first_epoch).await?;

        // Calculate and validate gap interval once at initialization
        let dummy_row = OwnedRow::new(vec![]);
        let interval_datum = gap_interval.eval_row_infallible(&dummy_row).await;
        let interval = interval_datum
            .ok_or_else(|| anyhow::anyhow!("Gap interval expression returned null"))?
            .into_interval();

        if interval <= Interval::from_month_day_usec(0, 0, 0) {
            Err(anyhow::anyhow!("Gap interval must be positive"))?;
        }

        let partition_by_indices = managed_state.partition_by_indices.clone();
        let pointer_key_indices = managed_state.pointer_key_indices.clone();

        #[for_await]
        for msg in input {
            match msg? {
                Message::Chunk(chunk) => {
                    let chunk = chunk.compact_vis();
                    let mut chunk_builder =
                        StreamChunkBuilder::new(chunk_size, schema.data_types());

                    // Fill rows interleave between an update's U-/U+, so the pair can't stay
                    // adjacent; normalize anchor ops to Insert/Delete to avoid a dangling Update.
                    for (op, row_ref) in chunk.rows() {
                        let row = row_ref.to_owned_row();
                        if row.datum_at(time_column_index).is_none() {
                            if let Some(chunk) =
                                chunk_builder.append_row(op.normalize_update(), &row)
                            {
                                yield Message::Chunk(chunk);
                            }
                            continue;
                        }
                        let partition_key = (&row).project(&partition_by_indices);
                        let pointer_key = (&row).project(&pointer_key_indices);

                        match op {
                            Op::Insert | Op::UpdateInsert => {
                                // 1. Find prev and next neighbors in the same partition from the
                                //    authoritative state table.
                                let prev_output = Self::find_prev_output(
                                    &managed_state,
                                    &partition_key,
                                    &pointer_key,
                                )
                                .await?;

                                let next_output = Self::find_next_output(
                                    &managed_state,
                                    &partition_key,
                                    &pointer_key,
                                )
                                .await?;

                                // 2. If both neighbors existed, delete old filled rows between them.
                                if let (Some(prev_out_ref), Some(next_out_ref)) =
                                    (&prev_output, &next_output)
                                {
                                    let old_fills = Self::generate_filled_rows_between_static(
                                        prev_out_ref,
                                        next_out_ref,
                                        &interval,
                                        time_column_index,
                                        &managed_state.partition_by_indices,
                                        &fill_columns,
                                        &metrics,
                                    )?;
                                    for filled_row in &old_fills {
                                        if let Some(chunk) =
                                            chunk_builder.append_row(Op::Delete, filled_row)
                                        {
                                            yield Message::Chunk(chunk);
                                        }
                                    }
                                }

                                // 3. Insert the new anchor row into state.
                                managed_state.insert(&row);
                                // 4. Emit the inserted row.
                                if let Some(chunk) =
                                    chunk_builder.append_row(op.normalize_update(), &row)
                                {
                                    yield Message::Chunk(chunk);
                                }

                                // 5. Emit new filled rows between prev and new row.
                                if let Some(prev_out) = &prev_output {
                                    let filled_rows = Self::generate_filled_rows_between_static(
                                        prev_out,
                                        &row,
                                        &interval,
                                        time_column_index,
                                        &managed_state.partition_by_indices,
                                        &fill_columns,
                                        &metrics,
                                    )?;
                                    for filled_row in filled_rows {
                                        if let Some(chunk) =
                                            chunk_builder.append_row(Op::Insert, &filled_row)
                                        {
                                            yield Message::Chunk(chunk);
                                        }
                                    }
                                }

                                // 6. Emit new filled rows between new row and next.
                                if let Some(next_out) = &next_output {
                                    let filled_rows = Self::generate_filled_rows_between_static(
                                        &row,
                                        next_out,
                                        &interval,
                                        time_column_index,
                                        &managed_state.partition_by_indices,
                                        &fill_columns,
                                        &metrics,
                                    )?;
                                    for filled_row in filled_rows {
                                        if let Some(chunk) =
                                            chunk_builder.append_row(Op::Insert, &filled_row)
                                        {
                                            yield Message::Chunk(chunk);
                                        }
                                    }
                                }
                            }
                            Op::Delete | Op::UpdateDelete => {
                                // 1. Find prev/next neighbors from the authoritative state table.
                                let prev_output = Self::find_prev_output(
                                    &managed_state,
                                    &partition_key,
                                    &pointer_key,
                                )
                                .await?;

                                let next_output = Self::find_next_output(
                                    &managed_state,
                                    &partition_key,
                                    &pointer_key,
                                )
                                .await?;

                                // 2. Delete old filled rows on both sides.
                                if let Some(prev_out) = &prev_output {
                                    let old_fills = Self::generate_filled_rows_between_static(
                                        prev_out,
                                        &row,
                                        &interval,
                                        time_column_index,
                                        &managed_state.partition_by_indices,
                                        &fill_columns,
                                        &metrics,
                                    )?;
                                    for filled_row in &old_fills {
                                        if let Some(chunk) =
                                            chunk_builder.append_row(Op::Delete, filled_row)
                                        {
                                            yield Message::Chunk(chunk);
                                        }
                                    }
                                }

                                if let Some(next_out) = &next_output {
                                    let old_fills = Self::generate_filled_rows_between_static(
                                        &row,
                                        next_out,
                                        &interval,
                                        time_column_index,
                                        &managed_state.partition_by_indices,
                                        &fill_columns,
                                        &metrics,
                                    )?;
                                    for filled_row in &old_fills {
                                        if let Some(chunk) =
                                            chunk_builder.append_row(Op::Delete, filled_row)
                                        {
                                            yield Message::Chunk(chunk);
                                        }
                                    }
                                }

                                // 3. Delete the anchor row from state.
                                managed_state.delete(&row);

                                // 4. Emit the delete for the original row.
                                if let Some(chunk) =
                                    chunk_builder.append_row(op.normalize_update(), &row)
                                {
                                    yield Message::Chunk(chunk);
                                }

                                // 5. If both neighbors exist, emit new fills between them.
                                if let (Some(prev_out), Some(next_out)) =
                                    (&prev_output, &next_output)
                                {
                                    let filled_rows = Self::generate_filled_rows_between_static(
                                        prev_out,
                                        next_out,
                                        &interval,
                                        time_column_index,
                                        &managed_state.partition_by_indices,
                                        &fill_columns,
                                        &metrics,
                                    )?;
                                    for filled_row in filled_rows {
                                        if let Some(chunk) =
                                            chunk_builder.append_row(Op::Insert, &filled_row)
                                        {
                                            yield Message::Chunk(chunk);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if let Some(chunk) = chunk_builder.take() {
                        yield Message::Chunk(chunk);
                    }

                    managed_state.try_flush().await?;
                }
                Message::Watermark(_) => {
                    // Gap fill back-fills and retracts rows below the latest time, so its output is
                    // not watermark-aligned on any column (see the empty `WatermarkColumns` in
                    // `StreamGapFill`). Drop input watermarks rather than forwarding a promise the
                    // output cannot keep.
                    continue;
                }
                Message::Barrier(barrier) => {
                    let post_commit = managed_state.flush(barrier.epoch).await?;
                    let update_vnode_bitmap = barrier.as_update_vnode_bitmap(ctx.id);
                    yield Message::Barrier(barrier);
                    let _ = post_commit.post_yield_barrier(update_vnode_bitmap).await?;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
    use risingwave_common::types::test_utils::IntervalTestExt;
    use risingwave_common::types::{DataType, Interval, ScalarImpl, Timestamp};
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_expr::expr::LiteralExpression;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::common::table::state_table::StateTable;
    use crate::common::table::test_utils::gen_pbtable_with_dist_key;
    use crate::executor::test_utils::{MessageSender, MockSource};

    async fn create_executor(
        store: MemoryStateStore,
        fill_columns: HashMap<usize, FillStrategy>,
        schema: Schema,
        gap_interval: Interval,
    ) -> (MessageSender, BoxedMessageStream) {
        let (tx, source) = MockSource::channel();
        let source = source.into_executor(schema.clone(), vec![0]);

        let time_column_index = 0;
        let partition_by_indices: Vec<usize> = vec![];
        // Stream key = [0] (time column), so the lookup key within the singleton partition is the
        // time value.
        let pointer_key_indices = vec![0];

        let table_columns: Vec<ColumnDesc> = schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, f)| ColumnDesc::unnamed(ColumnId::new(i as i32), f.data_type.clone()))
            .collect();

        // PK: (partition_cols, time_col, stream_key) with dedup = [0]
        // (no partition, time=0, sk=[0] already covered)
        let table = StateTable::from_table_catalog(
            &gen_pbtable_with_dist_key(
                TableId::new(0),
                table_columns,
                vec![OrderType::ascending()],
                vec![0],
                0,
                vec![],
            ),
            store,
            None,
        )
        .await;

        let executor = GapFillExecutor::new(GapFillExecutorArgs {
            ctx: ActorContext::for_test(123),
            input: source,
            schema: schema.clone(),
            chunk_size: 1024,
            time_column_index,
            fill_columns,
            gap_interval: NonStrictExpression::for_test(LiteralExpression::new(
                DataType::Interval,
                Some(gap_interval.into()),
            )),
            state_table: table,
            partition_by_indices,
            pointer_key_indices,
        });

        (tx, executor.boxed().execute())
    }

    fn test_gap_fill_metrics() -> GapFillMetrics {
        let ctx = ActorContext::for_test(123);
        let actor_id = ctx.id.to_string();
        let fragment_id = ctx.fragment_id.to_string();

        GapFillMetrics {
            gap_fill_generated_rows_count: ctx
                .streaming_metrics
                .gap_fill_generated_rows_count
                .with_guarded_label_values(&[&actor_id, &fragment_id]),
        }
    }

    #[test]
    fn test_generate_filled_rows_preserve_partition_columns() {
        let prev_row = OwnedRow::new(vec![
            Some(ScalarImpl::Int32(7)),
            Some(ScalarImpl::Timestamp(
                "2023-04-01T10:00:00".parse::<Timestamp>().unwrap(),
            )),
            Some(ScalarImpl::Int32(10)),
            Some(ScalarImpl::Int32(99)),
        ]);
        let curr_row = OwnedRow::new(vec![
            Some(ScalarImpl::Int32(7)),
            Some(ScalarImpl::Timestamp(
                "2023-04-01T10:03:00".parse::<Timestamp>().unwrap(),
            )),
            Some(ScalarImpl::Int32(40)),
            Some(ScalarImpl::Int32(88)),
        ]);

        let filled_rows = GapFillExecutor::<MemoryStateStore>::generate_filled_rows_between_static(
            &prev_row,
            &curr_row,
            &Interval::from_minutes(1),
            1,
            &[0],
            &HashMap::from([(2, FillStrategy::Locf)]),
            &test_gap_fill_metrics(),
        )
        .unwrap();

        assert_eq!(
            filled_rows,
            vec![
                OwnedRow::new(vec![
                    Some(ScalarImpl::Int32(7)),
                    Some(ScalarImpl::Timestamp(
                        "2023-04-01T10:01:00".parse::<Timestamp>().unwrap(),
                    )),
                    Some(ScalarImpl::Int32(10)),
                    None,
                ]),
                OwnedRow::new(vec![
                    Some(ScalarImpl::Int32(7)),
                    Some(ScalarImpl::Timestamp(
                        "2023-04-01T10:02:00".parse::<Timestamp>().unwrap(),
                    )),
                    Some(ScalarImpl::Int32(10)),
                    None,
                ]),
            ]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_streaming_gap_fill_locf() {
        let store = MemoryStateStore::new();
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Timestamp),
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Float64),
        ]);
        let fill_columns = HashMap::from([(1, FillStrategy::Locf), (2, FillStrategy::Locf)]);
        let (mut tx, mut executor) =
            create_executor(store, fill_columns, schema, Interval::from_minutes(1)).await;

        // Init with barrier.
        tx.push_barrier(test_epoch(1), false);
        executor.next().await.unwrap().unwrap(); // Barrier

        // 1. Send an initial chunk with a gap to test basic filling.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            + 2022-01-01T00:00:00 1   1.0
            + 2022-01-01T00:03:00 4   4.0",
        ));

        let chunk = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        let expected = StreamChunk::from_pretty(
            " TS                  i   F
            + 2022-01-01T00:00:00 1   1.0
            + 2022-01-01T00:03:00 4   4.0
            + 2022-01-01T00:01:00 1   1.0
            + 2022-01-01T00:02:00 1   1.0",
        );

        // Simple comparison since the test utility assumes Int64 keys.
        assert_eq!(chunk.ops(), expected.ops());
        assert_eq!(chunk.visibility(), expected.visibility());

        // Compare each row individually.
        let chunk_rows: Vec<_> = chunk.rows().collect();
        let expected_rows: Vec<_> = expected.rows().collect();
        assert_eq!(chunk_rows.len(), expected_rows.len());

        for (i, ((op1, row1), (op2, row2))) in
            chunk_rows.iter().zip_eq(expected_rows.iter()).enumerate()
        {
            assert_eq!(op1, op2, "Row {} operation mismatch", i);
            assert_eq!(
                row1.to_owned_row(),
                row2.to_owned_row(),
                "Row {} data mismatch",
                i
            );
        }

        // 2. Send a new chunk that arrives out-of-order, landing in the previously filled gap.
        // This tests if the executor can correctly retract old filled rows and create new ones.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            + 2022-01-01T00:02:00 2   2.0",
        ));

        // Expect a chunk that retracts the old fills, inserts the new row, and adds the new fills.
        let chunk2 = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();

        let expected2 = StreamChunk::from_pretty(
            " TS                  i   F
                - 2022-01-01T00:01:00 1   1.0
                - 2022-01-01T00:02:00 1   1.0
                + 2022-01-01T00:01:00 1   1.0
                + 2022-01-01T00:02:00 2   2.0",
        );

        assert_eq!(chunk2.sort_rows(), expected2.sort_rows());

        // 3. Send a delete chunk to remove an original data point.
        // This should trigger retraction of old fills and generation of new ones.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            - 2022-01-01T00:02:00 2   2.0",
        ));

        let chunk3 = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk3.sort_rows(),
            StreamChunk::from_pretty(
                " TS                  i   F
                - 2022-01-01T00:01:00 1   1.0
                - 2022-01-01T00:02:00 2   2.0
                + 2022-01-01T00:01:00 1   1.0
                + 2022-01-01T00:02:00 1   1.0"
            )
            .sort_rows()
        );

        // 4. Send an update chunk to modify an original data point.
        // This should also trigger retraction and re-generation of fills.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            U- 2022-01-01T00:03:00 4   4.0
            U+ 2022-01-01T00:03:00 5   5.0",
        ));

        let chunk4 = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        // The filled rows' values don't change as they depend on the first row,
        // but they are still retracted and re-inserted due to the general path logic.
        assert_eq!(
            chunk4.sort_rows(),
            StreamChunk::from_pretty(
                " TS                  i   F
                - 2022-01-01T00:01:00 1   1.0
                - 2022-01-01T00:02:00 1   1.0
                - 2022-01-01T00:03:00 4   4.0
                + 2022-01-01T00:01:00 1   1.0
                + 2022-01-01T00:02:00 1   1.0
                + 2022-01-01T00:03:00 5   5.0"
            )
            .sort_rows()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_streaming_gap_fill_null() {
        let store = MemoryStateStore::new();
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Timestamp),
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Float64),
        ]);
        let fill_columns = HashMap::from([(1, FillStrategy::Null), (2, FillStrategy::Null)]);
        let (mut tx, mut executor) =
            create_executor(store, fill_columns, schema, Interval::from_minutes(1)).await;

        // Init with barrier.
        tx.push_barrier(test_epoch(1), false);
        executor.next().await.unwrap().unwrap(); // Barrier

        // 1. Send an initial chunk with a gap to test basic filling.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            + 2022-01-01T00:00:00 1   1.0
            + 2022-01-01T00:03:00 4   4.0",
        ));

        let chunk = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk.sort_rows(),
            StreamChunk::from_pretty(
                " TS                  i   F
                + 2022-01-01T00:00:00 1   1.0
                + 2022-01-01T00:01:00 .   .
                + 2022-01-01T00:02:00 .   .
                + 2022-01-01T00:03:00 4   4.0"
            )
            .sort_rows()
        );

        // 2. Send a new chunk that arrives out-of-order, landing in the previously filled gap.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            + 2022-01-01T00:02:00 2   2.0",
        ));

        let chunk2 = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk2.sort_rows(),
            StreamChunk::from_pretty(
                " TS                  i   F
                - 2022-01-01T00:01:00 .   .
                - 2022-01-01T00:02:00 .   .
                + 2022-01-01T00:01:00 .   .
                + 2022-01-01T00:02:00 2   2.0"
            )
            .sort_rows()
        );

        // 3. Send a delete chunk to remove an original data point.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            - 2022-01-01T00:02:00 2   2.0",
        ));

        let chunk3 = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk3.sort_rows(),
            StreamChunk::from_pretty(
                " TS                  i   F
                - 2022-01-01T00:01:00 .   .
                - 2022-01-01T00:02:00 2   2.0
                + 2022-01-01T00:01:00 .   .
                + 2022-01-01T00:02:00 .   ."
            )
            .sort_rows()
        );

        // 4. Send an update chunk to modify an original data point.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            U- 2022-01-01T00:03:00 4   4.0
            U+ 2022-01-01T00:03:00 5   5.0",
        ));

        let chunk4 = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk4.sort_rows(),
            StreamChunk::from_pretty(
                " TS                  i   F
                - 2022-01-01T00:01:00 .   .
                - 2022-01-01T00:02:00 .   .
                - 2022-01-01T00:03:00 4   4.0
                + 2022-01-01T00:01:00 .   .
                + 2022-01-01T00:02:00 .   .
                + 2022-01-01T00:03:00 5   5.0"
            )
            .sort_rows()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_streaming_gap_fill_interpolate() {
        let store = MemoryStateStore::new();
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Timestamp),
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Float64),
        ]);
        let fill_columns = HashMap::from([
            (1, FillStrategy::Interpolate),
            (2, FillStrategy::Interpolate),
        ]);
        let (mut tx, mut executor) =
            create_executor(store, fill_columns, schema, Interval::from_minutes(1)).await;

        // Init with barrier.
        tx.push_barrier(test_epoch(1), false);
        executor.next().await.unwrap().unwrap(); // Barrier

        // 1. Send an initial chunk with a gap to test basic filling.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            + 2022-01-01T00:00:00 1   1.0
            + 2022-01-01T00:03:00 4   4.0",
        ));

        let chunk = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk.sort_rows(),
            StreamChunk::from_pretty(
                " TS                  i   F
                + 2022-01-01T00:00:00 1   1.0
                + 2022-01-01T00:01:00 2   2.0
                + 2022-01-01T00:02:00 3   3.0
                + 2022-01-01T00:03:00 4   4.0"
            )
            .sort_rows()
        );

        // 2. Send a new chunk that arrives out-of-order, landing in the previously filled gap.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            + 2022-01-01T00:02:00 10  10.0",
        ));

        let chunk2 = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk2.sort_rows(),
            StreamChunk::from_pretty(
                " TS                  i   F
                - 2022-01-01T00:01:00 2   2.0
                - 2022-01-01T00:02:00 3   3.0
                + 2022-01-01T00:01:00 5   5.5
                + 2022-01-01T00:02:00 10  10.0"
            )
            .sort_rows()
        );

        // 3. Send a delete chunk to remove an original data point.
        // This should trigger retraction of old fills and re-calculation of interpolated values.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            - 2022-01-01T00:02:00 10  10.0",
        ));

        let chunk3 = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk3.sort_rows(),
            StreamChunk::from_pretty(
                " TS                  i   F
                - 2022-01-01T00:01:00 5   5.5
                - 2022-01-01T00:02:00 10  10.0
                + 2022-01-01T00:01:00 2   2.0
                + 2022-01-01T00:02:00 3   3.0"
            )
            .sort_rows()
        );

        // 4. Send an update chunk to modify an original data point.
        // This will cause the interpolated values to be re-calculated.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            U- 2022-01-01T00:03:00 4   4.0
            U+ 2022-01-01T00:03:00 10  10.0",
        ));

        let chunk4 = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk4.sort_rows(),
            StreamChunk::from_pretty(
                " TS                  i   F
                - 2022-01-01T00:01:00 2   2.0
                - 2022-01-01T00:02:00 3   3.0
                - 2022-01-01T00:03:00 4   4.0
                + 2022-01-01T00:01:00 4   4.0
                + 2022-01-01T00:02:00 7   7.0
                + 2022-01-01T00:03:00 10  10.0"
            )
            .sort_rows()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_streaming_gap_fill_prefers_nearest_state_neighbor_over_partial_cache() {
        let store = MemoryStateStore::new();
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Timestamp),
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Float64),
        ]);
        let fill_columns = HashMap::from([(1, FillStrategy::Null), (2, FillStrategy::Null)]);

        // --- First run: persist the initial anchors into state.
        let (mut tx, mut executor) = create_executor(
            store.clone(),
            fill_columns.clone(),
            schema.clone(),
            Interval::from_minutes(1),
        )
        .await;

        tx.push_barrier(test_epoch(1), false);
        executor.next().await.unwrap().unwrap(); // Barrier

        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            + 2022-01-01T00:00:00 10  10.0
            + 2022-01-01T00:02:00 12  12.0
            + 2022-01-01T00:05:00 15  15.0",
        ));
        executor.next().await.unwrap().unwrap(); // Initial filled chunk

        tx.push_barrier(test_epoch(2), false);
        executor.next().await.unwrap().unwrap(); // Commit

        // --- Second run: recovered state exists, but cache starts empty.
        let (mut tx2, mut executor2) = create_executor(
            store.clone(),
            fill_columns.clone(),
            schema.clone(),
            Interval::from_minutes(1),
        )
        .await;

        tx2.push_barrier(test_epoch(2), false);
        executor2.next().await.unwrap().unwrap(); // Recovery barrier

        // Insert a later anchor so the cache only knows about 00:08.
        tx2.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            + 2022-01-01T00:08:00 20  20.0",
        ));
        executor2.next().await.unwrap().unwrap(); // Chunk

        tx2.push_barrier(test_epoch(3), false);
        executor2.next().await.unwrap().unwrap(); // Commit

        // Insert into the historical gap. The nearest next neighbor is 00:02 from state, not
        // the cached 00:08 row. We should only retract the old fill at 00:01.
        tx2.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            + 2022-01-01T00:01:00 11  11.0",
        ));

        let chunk = executor2
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();

        assert_eq!(
            chunk.sort_rows(),
            StreamChunk::from_pretty(
                " TS                  i   F
                - 2022-01-01T00:01:00 .   .
                + 2022-01-01T00:01:00 11  11.0"
            )
            .sort_rows()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_streaming_gap_fill_recovery() {
        let store = MemoryStateStore::new();
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Timestamp),
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Float64),
        ]);
        let fill_columns = HashMap::from([(1, FillStrategy::Locf), (2, FillStrategy::Interpolate)]);

        // --- First run ---
        let (mut tx, mut executor) = create_executor(
            store.clone(),
            fill_columns.clone(),
            schema.clone(),
            Interval::from_minutes(1),
        )
        .await;

        // Init with barrier.
        tx.push_barrier(test_epoch(1), false);
        executor.next().await.unwrap().unwrap(); // Barrier

        // Send a chunk and commit.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            + 2022-01-01T00:00:00 1   1.0
            + 2022-01-01T00:03:00 4   4.0",
        ));

        // Consume the initial filled chunk.
        let chunk = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk.sort_rows(),
            StreamChunk::from_pretty(
                " TS                  i   F
                + 2022-01-01T00:00:00 1   1.0
                + 2022-01-01T00:01:00 1   2.0
                + 2022-01-01T00:02:00 1   3.0
                + 2022-01-01T00:03:00 4   4.0"
            )
            .sort_rows()
        );

        tx.push_barrier(test_epoch(2), false);
        executor.next().await.unwrap().unwrap(); // Barrier to commit.

        // --- Second run (after recovery) ---
        let (mut tx2, mut executor2) = create_executor(
            store.clone(),
            fill_columns.clone(),
            schema.clone(),
            Interval::from_minutes(1),
        )
        .await;

        // Init with barrier, which triggers recovery.
        tx2.push_barrier(test_epoch(2), false);
        executor2.next().await.unwrap().unwrap(); // Barrier

        // After recovery, the executor should not output anything for the loaded state.

        // Send a new chunk, which should fill the gap between old and new data.
        tx2.push_chunk(StreamChunk::from_pretty(
            " TS                  i   F
            + 2022-01-01T00:05:00 6   10.0",
        ));

        let chunk2 = executor2
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk2.sort_rows(),
            StreamChunk::from_pretty(
                " TS                  i   F
                + 2022-01-01T00:04:00 4   7.0
                + 2022-01-01T00:05:00 6   10.0"
            )
            .sort_rows()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_streaming_gap_fill_mixed_strategy() {
        let store = MemoryStateStore::new();
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Timestamp),
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int64),
            Field::unnamed(DataType::Float32),
            Field::unnamed(DataType::Float64),
        ]);

        let fill_columns = HashMap::from([
            (1, FillStrategy::Interpolate),
            (2, FillStrategy::Locf),
            (3, FillStrategy::Null),
            (4, FillStrategy::Interpolate),
        ]);
        let gap_interval = Interval::from_days(1);
        let (mut tx, mut executor) =
            create_executor(store, fill_columns, schema, gap_interval).await;

        // Init with barrier.
        tx.push_barrier(test_epoch(1), false);
        executor.next().await.unwrap().unwrap();

        // Send an initial chunk with a gap to test mixed filling strategies.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   I    f     F
            + 2023-04-01T10:00:00 10 100 1.0 100.0
            + 2023-04-05T10:00:00 50 200 5.0 200.0",
        ));

        let chunk = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk.sort_rows(),
            StreamChunk::from_pretty(
                " TS                  i   I    f     F
                + 2023-04-01T10:00:00 10 100 1.0 100.0
                + 2023-04-02T10:00:00 20 100 .    125.0
                + 2023-04-03T10:00:00 30 100 .    150.0
                + 2023-04-04T10:00:00 40 100 .    175.0
                + 2023-04-05T10:00:00 50 200 5.0 200.0"
            )
            .sort_rows()
        );

        // 2. Send a new chunk that arrives out-of-order, landing in the previously filled gap.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   I    f     F
            + 2023-04-03T10:00:00 25 150 3.0 160.0",
        ));

        let chunk2 = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk2.sort_rows(),
            StreamChunk::from_pretty(
                " TS                  i   I    f     F
                - 2023-04-02T10:00:00 20 100 .    125.0
                - 2023-04-03T10:00:00 30 100 .    150.0
                - 2023-04-04T10:00:00 40 100 .    175.0
                + 2023-04-02T10:00:00 17 100 .    130.0
                + 2023-04-03T10:00:00 25 150 3.0 160.0
                + 2023-04-04T10:00:00 37 150 .    180.0"
            )
            .sort_rows()
        );

        // 3. Send a delete chunk to remove an original data point.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   I    f     F
            - 2023-04-03T10:00:00 25 150 3.0 160.0",
        ));
        let chunk3 = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk3.sort_rows(),
            StreamChunk::from_pretty(
                " TS                  i   I    f     F
                - 2023-04-02T10:00:00 17 100 .    130.0
                - 2023-04-03T10:00:00 25 150 3.0 160.0
                - 2023-04-04T10:00:00 37 150 .    180.0
                + 2023-04-02T10:00:00 20 100 .    125.0
                + 2023-04-03T10:00:00 30 100 .    150.0
                + 2023-04-04T10:00:00 40 100 .    175.0"
            )
            .sort_rows()
        );

        // 4. Send an update chunk to modify an original data point.
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   I    f     F
            U- 2023-04-05T10:00:00 50 200 5.0 200.0
            U+ 2023-04-05T10:00:00 50 200 5.0 300.0",
        ));
        let chunk4 = executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        assert_eq!(
            chunk4.sort_rows(),
            StreamChunk::from_pretty(
                " TS                  i   I    f     F
                - 2023-04-02T10:00:00 20 100 .    125.0
                - 2023-04-03T10:00:00 30 100 .    150.0
                - 2023-04-04T10:00:00 40 100 .    175.0
                - 2023-04-05T10:00:00 50 200 5.0 200.0
                + 2023-04-02T10:00:00 20 100 .    150.0
                + 2023-04-03T10:00:00 30 100 .    200.0
                + 2023-04-04T10:00:00 40 100 .    250.0
                + 2023-04-05T10:00:00 50 200 5.0 300.0"
            )
            .sort_rows()
        );
    }
}

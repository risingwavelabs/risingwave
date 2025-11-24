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

use risingwave_common::array::Op;
use risingwave_common::gap_fill::{
    FillStrategy, apply_interpolation_step, calculate_interpolation_step,
};
use risingwave_common::metrics::LabelGuardedIntCounter;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{CheckedAdd, ToOwnedDatum};
use risingwave_expr::ExprError;
use risingwave_expr::expr::NonStrictExpression;
use tracing::warn;

use super::sort_buffer::SortBuffer;
use crate::executor::prelude::*;

pub struct EowcGapFillExecutor<S: StateStore> {
    input: Executor,
    inner: ExecutorInner<S>,
}

pub struct EowcGapFillExecutorArgs<S: StateStore> {
    pub actor_ctx: ActorContextRef,

    pub input: Executor,

    pub schema: Schema,
    pub buffer_table: StateTable<S>,
    pub prev_row_table: StateTable<S>,
    pub chunk_size: usize,
    pub time_column_index: usize,
    pub fill_columns: HashMap<usize, FillStrategy>,
    pub gap_interval: NonStrictExpression,
}

pub struct GapFillMetrics {
    pub gap_fill_generated_rows_count: LabelGuardedIntCounter,
}

struct ExecutorInner<S: StateStore> {
    actor_ctx: ActorContextRef,

    schema: Schema,
    buffer_table: StateTable<S>,
    prev_row_table: StateTable<S>,
    chunk_size: usize,
    time_column_index: usize,
    fill_columns: HashMap<usize, FillStrategy>,
    gap_interval: NonStrictExpression,

    // Metrics
    metrics: GapFillMetrics,
}

struct ExecutionVars<S: StateStore> {
    buffer: SortBuffer<S>,
}

impl<S: StateStore> ExecutorInner<S> {
    fn generate_filled_rows(
        prev_row: &OwnedRow,
        curr_row: &OwnedRow,
        time_column_index: usize,
        fill_columns: &HashMap<usize, FillStrategy>,
        interval: risingwave_common::types::Interval,
        metrics: &GapFillMetrics,
    ) -> Result<Vec<OwnedRow>, ExprError> {
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
                warn!(
                    "Failed to convert time column to timestamp, got {:?}. Skipping gap fill.",
                    prev_time_scalar
                );
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
                warn!(
                    "Failed to convert time column to timestamp, got {:?}. Skipping gap fill.",
                    curr_time_scalar
                );
                return Ok(filled_rows);
            }
        };
        if prev_time >= curr_time {
            return Ok(filled_rows);
        }

        let mut fill_time = match prev_time.checked_add(interval) {
            Some(t) => t,
            None => {
                return Ok(filled_rows);
            }
        };
        if fill_time >= curr_time {
            return Ok(filled_rows);
        }

        // Calculate the number of rows to fill
        let mut row_count = 0;
        let mut temp_time = fill_time;
        while temp_time < curr_time {
            row_count += 1;
            temp_time = match temp_time.checked_add(interval) {
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
                    // No strategy specified, default to NULL
                    None
                };
                new_row_data.push(datum);
            }

            filled_rows.push(OwnedRow::new(new_row_data));

            fill_time = match fill_time.checked_add(interval) {
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

impl<S: StateStore> Execute for EowcGapFillExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

impl<S: StateStore> EowcGapFillExecutor<S> {
    pub fn new(args: EowcGapFillExecutorArgs<S>) -> Self {
        let metrics = args.actor_ctx.streaming_metrics.clone();
        let actor_id = args.actor_ctx.id.to_string();
        let fragment_id = args.actor_ctx.fragment_id.to_string();
        let gap_fill_metrics = GapFillMetrics {
            gap_fill_generated_rows_count: metrics
                .gap_fill_generated_rows_count
                .with_guarded_label_values(&[&actor_id, &fragment_id]),
        };

        Self {
            input: args.input,

            inner: ExecutorInner {
                actor_ctx: args.actor_ctx,
                schema: args.schema,
                buffer_table: args.buffer_table,
                prev_row_table: args.prev_row_table,
                chunk_size: args.chunk_size,
                time_column_index: args.time_column_index,
                fill_columns: args.fill_columns,
                gap_interval: args.gap_interval,
                metrics: gap_fill_metrics,
            },
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let Self {
            input,
            inner: mut this,
        } = self;

        let mut input = input.execute();

        let barrier = expect_first_barrier(&mut input).await?;
        let first_epoch = barrier.epoch;
        yield Message::Barrier(barrier);
        this.buffer_table.init_epoch(first_epoch).await?;
        this.prev_row_table.init_epoch(first_epoch).await?;

        // Calculate and validate gap interval once at initialization
        let dummy_row = OwnedRow::new(vec![]);
        let interval_datum = this.gap_interval.eval_row_infallible(&dummy_row).await;
        let interval = interval_datum
            .ok_or_else(|| anyhow::anyhow!("Gap interval expression returned null"))?
            .into_interval();

        // Validate that gap interval is not zero
        if interval.months() == 0 && interval.days() == 0 && interval.usecs() == 0 {
            Err(anyhow::anyhow!("Gap interval cannot be zero"))?;
        }

        let mut vars = ExecutionVars {
            buffer: SortBuffer::new(this.time_column_index, &this.buffer_table),
        };
        let mut committed_prev_row: Option<OwnedRow> =
            this.prev_row_table.get_from_one_row_table().await?;
        let mut staging_prev_row = committed_prev_row.clone();

        vars.buffer.refill_cache(None, &this.buffer_table).await?;

        #[for_await]
        for msg in input {
            match msg? {
                Message::Watermark(watermark @ Watermark { col_idx, .. })
                    if col_idx == this.time_column_index =>
                {
                    let mut chunk_builder =
                        StreamChunkBuilder::new(this.chunk_size, this.schema.data_types());

                    #[for_await]
                    for row in vars
                        .buffer
                        .consume(watermark.val.clone(), &mut this.buffer_table)
                    {
                        let current_row = row?;
                        if let Some(p_row) = &staging_prev_row {
                            let filled_rows = ExecutorInner::<S>::generate_filled_rows(
                                p_row,
                                &current_row,
                                this.time_column_index,
                                &this.fill_columns,
                                interval,
                                &this.metrics,
                            )?;
                            for filled_row in filled_rows {
                                if let Some(chunk) =
                                    chunk_builder.append_row(Op::Insert, &filled_row)
                                {
                                    yield Message::Chunk(chunk);
                                }
                            }
                        }
                        if let Some(chunk) = chunk_builder.append_row(Op::Insert, &current_row) {
                            yield Message::Chunk(chunk);
                        }
                        staging_prev_row = Some(current_row);
                    }
                    if let Some(chunk) = chunk_builder.take() {
                        yield Message::Chunk(chunk);
                    }

                    yield Message::Watermark(watermark);
                }
                Message::Watermark(_) => continue,
                Message::Chunk(chunk) => {
                    vars.buffer.apply_chunk(chunk, &mut this.buffer_table);
                    this.buffer_table.try_flush().await?;
                }
                Message::Barrier(barrier) => {
                    if committed_prev_row != staging_prev_row {
                        if let Some(old_row) = &committed_prev_row {
                            this.prev_row_table.delete(old_row);
                        }
                        if let Some(new_row) = &staging_prev_row {
                            this.prev_row_table.insert(new_row);
                        }
                    }

                    let post_commit = this.buffer_table.commit(barrier.epoch).await?;
                    this.prev_row_table
                        .commit_assert_no_update_vnode_bitmap(barrier.epoch)
                        .await?;

                    committed_prev_row.clone_from(&staging_prev_row);

                    let update_vnode_bitmap = barrier.as_update_vnode_bitmap(this.actor_ctx.id);
                    yield Message::Barrier(barrier);

                    if let Some((_, cache_may_stale)) =
                        post_commit.post_yield_barrier(update_vnode_bitmap).await?
                        && cache_may_stale
                    {
                        vars.buffer.refill_cache(None, &this.buffer_table).await?;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, TableId};
    use risingwave_common::types::Interval;
    use risingwave_common::types::test_utils::IntervalTestExt;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_expr::expr::LiteralExpression;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::common::table::test_utils::gen_pbtable_with_dist_key;
    use crate::executor::test_utils::{MessageSender, MockSource, StreamExecutorTestExt};

    async fn create_executor<S: StateStore>(
        time_column_index: usize,
        fill_columns: HashMap<usize, FillStrategy>,
        gap_interval: NonStrictExpression,
        store: S,
    ) -> (MessageSender, BoxedMessageStream) {
        let input_schema = Schema::new(vec![
            Field::unnamed(DataType::Timestamp),
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int64),
            Field::unnamed(DataType::Float32),
            Field::unnamed(DataType::Float64),
        ]);
        let input_stream_key = vec![time_column_index];

        let table_columns = vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Timestamp),
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Int32),
            ColumnDesc::unnamed(ColumnId::new(2), DataType::Int64),
            ColumnDesc::unnamed(ColumnId::new(3), DataType::Float32),
            ColumnDesc::unnamed(ColumnId::new(4), DataType::Float64),
        ];

        let table_pk_indices = vec![time_column_index];
        let table_order_types = vec![OrderType::ascending()];
        let buffer_table = StateTable::from_table_catalog(
            &gen_pbtable_with_dist_key(
                TableId::new(0),
                table_columns.clone(),
                table_order_types,
                table_pk_indices,
                0,
                vec![],
            ),
            store.clone(),
            None,
        )
        .await;

        let prev_row_pk_indices = vec![0];
        let prev_row_order_types = vec![OrderType::ascending()];
        let prev_row_table = StateTable::from_table_catalog(
            &gen_pbtable_with_dist_key(
                TableId::new(1),
                table_columns,
                prev_row_order_types,
                prev_row_pk_indices,
                0,
                vec![],
            ),
            store,
            None,
        )
        .await;

        let (tx, source) = MockSource::channel();
        let source = source.into_executor(input_schema, input_stream_key);
        let gap_fill_executor = EowcGapFillExecutor::new(EowcGapFillExecutorArgs {
            actor_ctx: ActorContext::for_test(123),
            schema: source.schema().clone(),
            input: source,
            buffer_table,
            prev_row_table,
            chunk_size: 1024,
            time_column_index,
            fill_columns,
            gap_interval,
        });

        (tx, gap_fill_executor.boxed().execute())
    }

    #[tokio::test]
    async fn test_gap_fill_interpolate() {
        let time_column_index = 0;
        let gap_interval = Interval::from_days(1);
        let fill_columns = HashMap::from([
            (1, FillStrategy::Interpolate),
            (2, FillStrategy::Interpolate),
            (3, FillStrategy::Interpolate),
            (4, FillStrategy::Interpolate),
        ]);
        let store = MemoryStateStore::new();
        let (mut tx, mut gap_fill_executor) = create_executor(
            time_column_index,
            fill_columns,
            NonStrictExpression::for_test(LiteralExpression::new(
                DataType::Interval,
                Some(gap_interval.into()),
            )),
            store.clone(),
        )
        .await;

        tx.push_barrier(test_epoch(1), false);
        gap_fill_executor.expect_barrier().await;

        tx.push_int64_watermark(1, 0_i64);
        tx.push_watermark(
            0,
            DataType::Timestamp,
            "2023-03-06 18:27:03"
                .parse::<risingwave_common::types::Timestamp>()
                .unwrap()
                .into(),
        );
        gap_fill_executor.expect_watermark().await;

        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   I    f     F
            + 2023-04-01T10:00:00 10 100 1.0 100.0
            + 2023-04-05T10:00:00 50 200 5.0 200.0",
        ));

        tx.push_int64_watermark(1, 0_i64);
        tx.push_watermark(
            0,
            DataType::Timestamp,
            "2023-04-05 18:27:03"
                .parse::<risingwave_common::types::Timestamp>()
                .unwrap()
                .into(),
        );

        let chunk = gap_fill_executor.expect_chunk().await;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " TS                  i   I    f     F
                + 2023-04-01T10:00:00 10 100 1.0 100.0
                + 2023-04-02T10:00:00 20 125 2.0 125.0
                + 2023-04-03T10:00:00 30 150 3.0 150.0
                + 2023-04-04T10:00:00 40 175 4.0 175.0
                + 2023-04-05T10:00:00 50 200 5.0 200.0",
            )
        );
        gap_fill_executor.expect_watermark().await;
    }

    #[tokio::test]
    async fn test_gap_fill_locf() {
        let time_column_index = 0;
        let gap_interval = Interval::from_days(1);
        let fill_columns = HashMap::from([
            (1, FillStrategy::Locf),
            (2, FillStrategy::Locf),
            (3, FillStrategy::Locf),
            (4, FillStrategy::Locf),
        ]);
        let store = MemoryStateStore::new();
        let (mut tx, mut gap_fill_executor) = create_executor(
            time_column_index,
            fill_columns,
            NonStrictExpression::for_test(LiteralExpression::new(
                DataType::Interval,
                Some(gap_interval.into()),
            )),
            store.clone(),
        )
        .await;

        tx.push_barrier(test_epoch(1), false);
        gap_fill_executor.expect_barrier().await;

        tx.push_int64_watermark(1, 0_i64);
        tx.push_watermark(
            0,
            DataType::Timestamp,
            "2023-03-06 18:27:03"
                .parse::<risingwave_common::types::Timestamp>()
                .unwrap()
                .into(),
        );
        gap_fill_executor.expect_watermark().await;

        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   I    f     F
            + 2023-04-01T10:00:00 10 100 1.0 100.0
            + 2023-04-05T10:00:00 50 200 5.0 200.0",
        ));

        tx.push_int64_watermark(1, 0_i64);
        tx.push_watermark(
            0,
            DataType::Timestamp,
            "2023-04-05 18:27:03"
                .parse::<risingwave_common::types::Timestamp>()
                .unwrap()
                .into(),
        );

        let chunk = gap_fill_executor.expect_chunk().await;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " TS                  i   I    f     F
                + 2023-04-01T10:00:00 10 100 1.0 100.0
                + 2023-04-02T10:00:00 10 100 1.0 100.0
                + 2023-04-03T10:00:00 10 100 1.0 100.0
                + 2023-04-04T10:00:00 10 100 1.0 100.0
                + 2023-04-05T10:00:00 50 200 5.0 200.0",
            )
        );
        gap_fill_executor.expect_watermark().await;
    }

    #[tokio::test]
    async fn test_gap_fill_null() {
        let time_column_index = 0;
        let gap_interval = Interval::from_days(1);
        let fill_columns = HashMap::from([
            (1, FillStrategy::Null),
            (2, FillStrategy::Null),
            (3, FillStrategy::Null),
            (4, FillStrategy::Null),
        ]);
        let store = MemoryStateStore::new();
        let (mut tx, mut gap_fill_executor) = create_executor(
            time_column_index,
            fill_columns,
            NonStrictExpression::for_test(LiteralExpression::new(
                DataType::Interval,
                Some(gap_interval.into()),
            )),
            store.clone(),
        )
        .await;

        tx.push_barrier(test_epoch(1), false);
        gap_fill_executor.expect_barrier().await;

        tx.push_int64_watermark(1, 0_i64);
        tx.push_watermark(
            0,
            DataType::Timestamp,
            "2023-03-06 18:27:03"
                .parse::<risingwave_common::types::Timestamp>()
                .unwrap()
                .into(),
        );
        gap_fill_executor.expect_watermark().await;

        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   I    f     F
            + 2023-04-01T10:00:00 10 100 1.0 100.0
            + 2023-04-05T10:00:00 50 200 5.0 200.0",
        ));

        tx.push_int64_watermark(1, 0_i64);
        tx.push_watermark(
            0,
            DataType::Timestamp,
            "2023-04-05 18:27:03"
                .parse::<risingwave_common::types::Timestamp>()
                .unwrap()
                .into(),
        );

        let chunk = gap_fill_executor.expect_chunk().await;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " TS                  i   I    f     F
                + 2023-04-01T10:00:00 10 100 1.0 100.0
                + 2023-04-02T10:00:00 .  .    .    .
                + 2023-04-03T10:00:00 .  .    .    .
                + 2023-04-04T10:00:00 .  .    .    .
                + 2023-04-05T10:00:00 50 200 5.0 200.0",
            )
        );
        gap_fill_executor.expect_watermark().await;
    }

    #[tokio::test]
    async fn test_gap_fill_mixed_strategy() {
        let time_column_index = 0;
        let gap_interval = Interval::from_days(1);
        let fill_columns = HashMap::from([
            (1, FillStrategy::Interpolate),
            (2, FillStrategy::Locf),
            (3, FillStrategy::Null),
            (4, FillStrategy::Interpolate),
        ]);
        let store = MemoryStateStore::new();
        let (mut tx, mut gap_fill_executor) = create_executor(
            time_column_index,
            fill_columns,
            NonStrictExpression::for_test(LiteralExpression::new(
                DataType::Interval,
                Some(gap_interval.into()),
            )),
            store.clone(),
        )
        .await;

        tx.push_barrier(test_epoch(1), false);
        gap_fill_executor.expect_barrier().await;

        tx.push_int64_watermark(1, 0_i64);
        tx.push_watermark(
            0,
            DataType::Timestamp,
            "2023-03-06 18:27:03"
                .parse::<risingwave_common::types::Timestamp>()
                .unwrap()
                .into(),
        );
        gap_fill_executor.expect_watermark().await;

        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   I    f     F
            + 2023-04-01T10:00:00 10 100 1.0 100.0
            + 2023-04-05T10:00:00 50 200 5.0 200.0",
        ));

        tx.push_int64_watermark(1, 0_i64);
        tx.push_watermark(
            0,
            DataType::Timestamp,
            "2023-04-05 18:27:03"
                .parse::<risingwave_common::types::Timestamp>()
                .unwrap()
                .into(),
        );

        let chunk = gap_fill_executor.expect_chunk().await;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " TS                  i   I    f     F
                + 2023-04-01T10:00:00 10 100 1.0 100.0
                + 2023-04-02T10:00:00 20 100 .    125.0
                + 2023-04-03T10:00:00 30 100 .    150.0
                + 2023-04-04T10:00:00 40 100 .    175.0
                + 2023-04-05T10:00:00 50 200 5.0 200.0",
            )
        );
        gap_fill_executor.expect_watermark().await;
    }

    #[tokio::test]
    async fn test_gap_fill_fail_over() {
        let time_column_index = 0;
        let gap_interval = Interval::from_days(1);
        let fill_columns = HashMap::from([
            (1, FillStrategy::Locf),
            (2, FillStrategy::Interpolate),
            (3, FillStrategy::Locf),
            (4, FillStrategy::Locf),
        ]);
        let store = MemoryStateStore::new();
        let (mut tx, mut gap_fill_executor) = create_executor(
            time_column_index,
            fill_columns.clone(),
            NonStrictExpression::for_test(LiteralExpression::new(
                DataType::Interval,
                Some(gap_interval.into()),
            )),
            store.clone(),
        )
        .await;

        tx.push_barrier(test_epoch(1), false);
        gap_fill_executor.expect_barrier().await;

        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   I    f     F
            + 2023-04-01T10:00:00 10 100 1.0 100.0
            + 2023-04-05T10:00:00 50 200 5.0 200.0",
        ));

        tx.push_barrier(test_epoch(2), false);
        gap_fill_executor.expect_barrier().await;

        let (mut recovered_tx, mut recovered_gap_fill_executor) = create_executor(
            time_column_index,
            fill_columns.clone(),
            NonStrictExpression::for_test(LiteralExpression::new(
                DataType::Interval,
                Some(gap_interval.into()),
            )),
            store.clone(),
        )
        .await;

        recovered_tx.push_barrier(test_epoch(2), false);
        recovered_gap_fill_executor.expect_barrier().await;

        recovered_tx.push_watermark(
            0,
            DataType::Timestamp,
            "2023-04-06T10:00:00"
                .parse::<risingwave_common::types::Timestamp>()
                .unwrap()
                .into(),
        );

        let chunk = recovered_gap_fill_executor.expect_chunk().await;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " TS                  i   I    f     F
                + 2023-04-01T10:00:00 10 100 1.0 100.0
                + 2023-04-02T10:00:00 10 125 1.0 100.0
                + 2023-04-03T10:00:00 10 150 1.0 100.0
                + 2023-04-04T10:00:00 10 175 1.0 100.0
                + 2023-04-05T10:00:00 50 200 5.0 200.0"
            )
        );

        recovered_gap_fill_executor.expect_watermark().await;

        recovered_tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   I    f     F
            + 2023-04-08T10:00:00 80 500 8.0 500.0",
        ));

        recovered_tx.push_barrier(test_epoch(3), false);
        recovered_gap_fill_executor.expect_barrier().await;

        let (mut final_recovered_tx, mut final_recovered_gap_fill_executor) = create_executor(
            time_column_index,
            fill_columns,
            NonStrictExpression::for_test(LiteralExpression::new(
                DataType::Interval,
                Some(gap_interval.into()),
            )),
            store,
        )
        .await;

        final_recovered_tx.push_barrier(test_epoch(3), false);
        final_recovered_gap_fill_executor.expect_barrier().await;

        final_recovered_tx.push_watermark(
            0,
            DataType::Timestamp,
            "2023-04-09T10:00:00"
                .parse::<risingwave_common::types::Timestamp>()
                .unwrap()
                .into(),
        );

        let chunk = final_recovered_gap_fill_executor.expect_chunk().await;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " TS                  i   I    f     F
                + 2023-04-06T10:00:00 50 300 5.0 200.0
                + 2023-04-07T10:00:00 50 400 5.0 200.0
                + 2023-04-08T10:00:00 80 500 8.0 500.0"
            )
        );

        final_recovered_gap_fill_executor.expect_watermark().await;
    }
}

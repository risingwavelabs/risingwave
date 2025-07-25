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

use risingwave_common::array::Op;
use risingwave_common::types::{CheckedAdd, Decimal, Interval, ToOwnedDatum};

use super::sort_buffer::SortBuffer;
use crate::executor::prelude::*;

pub struct GapFillExecutor<S: StateStore> {
    input: Executor,
    inner: ExecutorInner<S>,
}

pub enum FillStrategy {
    Locf,
    Interpolate,
    Null,
}

pub struct GapFillExecutorArgs<S: StateStore> {
    pub actor_ctx: ActorContextRef,

    pub input: Executor,

    pub schema: Schema,
    pub buffer_table: StateTable<S>,
    pub chunk_size: usize,
    pub time_column_index: usize,
    pub fill_columns: Vec<(usize, FillStrategy)>,
    pub gap_interval: Interval,
}

struct ExecutorInner<S: StateStore> {
    actor_ctx: ActorContextRef,

    schema: Schema,
    buffer_table: StateTable<S>,
    chunk_size: usize,
    time_column_index: usize,
    fill_columns: Vec<(usize, FillStrategy)>,
    gap_interval: Interval,
}

struct ExecutionVars<S: StateStore> {
    buffer: SortBuffer<S>,
}

impl<S: StateStore> ExecutorInner<S> {
    fn calculate_step(d1: DatumRef<'_>, d2: DatumRef<'_>, steps: usize) -> Datum {
        let (Some(s1), Some(s2)) = (d1, d2) else {
            return None;
        };
        if steps == 0 {
            return None;
        }
        match (s1, s2) {
            (ScalarRefImpl::Int16(v1), ScalarRefImpl::Int16(v2)) => {
                Some(ScalarImpl::Int16((v2 - v1) / steps as i16))
            }
            (ScalarRefImpl::Int32(v1), ScalarRefImpl::Int32(v2)) => {
                Some(ScalarImpl::Int32((v2 - v1) / steps as i32))
            }
            (ScalarRefImpl::Int64(v1), ScalarRefImpl::Int64(v2)) => {
                Some(ScalarImpl::Int64((v2 - v1) / steps as i64))
            }
            (ScalarRefImpl::Float32(v1), ScalarRefImpl::Float32(v2)) => {
                Some(ScalarImpl::Float32((v2 - v1) / steps as f32))
            }
            (ScalarRefImpl::Float64(v1), ScalarRefImpl::Float64(v2)) => {
                Some(ScalarImpl::Float64((v2 - v1) / steps as f64))
            }
            (ScalarRefImpl::Decimal(v1), ScalarRefImpl::Decimal(v2)) => {
                Some(ScalarImpl::Decimal((v2 - v1) / Decimal::from(steps)))
            }
            _ => None,
        }
    }

    fn apply_step(current: &mut Datum, step: &ScalarImpl) {
        if let Some(curr) = current.as_mut() {
            match (curr, step) {
                (ScalarImpl::Int16(v1), &ScalarImpl::Int16(v2)) => *v1 += v2,
                (ScalarImpl::Int32(v1), &ScalarImpl::Int32(v2)) => *v1 += v2,
                (ScalarImpl::Int64(v1), &ScalarImpl::Int64(v2)) => *v1 += v2,
                (ScalarImpl::Float32(v1), &ScalarImpl::Float32(v2)) => *v1 += v2,
                (ScalarImpl::Float64(v1), &ScalarImpl::Float64(v2)) => *v1 += v2,
                (ScalarImpl::Decimal(v1), &ScalarImpl::Decimal(v2)) => *v1 = *v1 + v2,
                _ => return
            }
        };
    }

    fn generate_filled_rows(
        prev_row: &OwnedRow,
        curr_row: &OwnedRow,
        time_column_index: usize,
        fill_columns: &[(usize, FillStrategy)],
        interval: Interval,
    ) -> Vec<OwnedRow> {
        let mut filled_rows = Vec::new();
        let (Some(prev_time_scalar), Some(curr_time_scalar)) = (
            prev_row.datum_at(time_column_index),
            curr_row.datum_at(time_column_index),
        ) else {
            return filled_rows;
        };
        let prev_time = prev_time_scalar.into_timestamp();
        let curr_time = curr_time_scalar.into_timestamp();
        if prev_time >= curr_time {
            return filled_rows;
        }

        // generate fill value for every column
        let mut fill_values: Vec<Datum> = Vec::with_capacity(prev_row.len());
        for i in 0..prev_row.len() {
            if i == time_column_index {
                fill_values.push(None);
            } else if let Some((_, strategy)) = fill_columns.iter().find(|(col, _)| *col == i) {
                match strategy {
                    FillStrategy::Locf | FillStrategy::Interpolate => {
                        fill_values.push(prev_row.datum_at(i).to_owned_datum())
                    }
                    FillStrategy::Null => fill_values.push(None),
                }
            } else {
                fill_values.push(prev_row.datum_at(i).to_owned_datum());
            }
        }
        let row_template: Vec<Datum> = fill_values;
        let mut fill_time = match prev_time.checked_add(interval) {
            Some(t) => t,
            None => return filled_rows,
        };

        let mut data = Vec::new();
        while fill_time < curr_time {
            let mut new_row_data = row_template.clone();
            new_row_data[time_column_index] = Some(fill_time.into());
            data.push(new_row_data);
            fill_time = match fill_time.checked_add(interval) {
                Some(t) => t,
                None => return filled_rows,
            };
        }
        for (col_idx, strategy) in fill_columns.iter() {
            if matches!(strategy, FillStrategy::Interpolate) {
                let steps = data.len();
                let step = Self::calculate_step(
                    prev_row.datum_at(*col_idx),
                    curr_row.datum_at(*col_idx),
                    steps + 1,
                );
                if let Some(step) = step {
                    for (i, row) in data.iter_mut().enumerate() {
                        for _ in 0..=i {
                            Self::apply_step(&mut row[*col_idx], &step);
                        }
                    }
                }
            }
        }
        for row in data {
            filled_rows.push(OwnedRow::new(row));
        }
        filled_rows
    }
}

impl<S: StateStore> Execute for GapFillExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.executor_inner().boxed()
    }
}

impl<S: StateStore> GapFillExecutor<S> {
    pub fn new(args: GapFillExecutorArgs<S>) -> Self {
        Self {
            input: args.input,

            inner: ExecutorInner {
                actor_ctx: args.actor_ctx,
                schema: args.schema,
                buffer_table: args.buffer_table,
                chunk_size: args.chunk_size,
                time_column_index: args.time_column_index,
                fill_columns: args.fill_columns,
                gap_interval: args.gap_interval,
            },
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn executor_inner(self) {
        let Self {
            input,
            inner: mut this,
        } = self;

        let mut input = input.execute();

        let barrier = expect_first_barrier(&mut input).await?;
        let first_epoch = barrier.epoch;
        yield Message::Barrier(barrier);
        this.buffer_table.init_epoch(first_epoch).await?;

        let mut vars = ExecutionVars {
            buffer: SortBuffer::new(this.time_column_index, &this.buffer_table),
        };
        let mut prev_row: Option<OwnedRow> = None;

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
                        if let Some(p_row) = &prev_row {
                            let filled_rows = ExecutorInner::<S>::generate_filled_rows(
                                p_row,
                                &current_row,
                                this.time_column_index,
                                &this.fill_columns,
                                this.gap_interval,
                            );
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
                        prev_row = Some(current_row);
                    }
                    if let Some(chunk) = chunk_builder.take() {
                        yield Message::Chunk(chunk);
                    }

                    yield Message::Watermark(watermark);
                }
                Message::Watermark(_) => {
                    // ignore watermarks on other columns
                    continue;
                }
                Message::Chunk(chunk) => {
                    vars.buffer.apply_chunk(chunk, &mut this.buffer_table);
                    this.buffer_table.try_flush().await?;
                }
                Message::Barrier(barrier) => {
                    let post_commit = this.buffer_table.commit(barrier.epoch).await?;
                    let update_vnode_bitmap = barrier.as_update_vnode_bitmap(this.actor_ctx.id);
                    yield Message::Barrier(barrier);

                    // Update the vnode bitmap for state tables of all agg calls if asked.
                    if let Some((_, cache_may_stale)) =
                        post_commit.post_yield_barrier(update_vnode_bitmap).await?
                    {
                        if cache_may_stale {
                            vars.buffer.refill_cache(None, &this.buffer_table).await?;
                        }
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
    use risingwave_common::types::test_utils::IntervalTestExt;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::common::table::test_utils::gen_pbtable;
    use crate::executor::test_utils::{MessageSender, MockSource, StreamExecutorTestExt};

    async fn create_executor<S: StateStore>(
        time_column_index: usize,
        fill_columns: Vec<(usize, FillStrategy)>,
        gap_interval: Interval,
        store: S,
    ) -> (MessageSender, BoxedMessageStream) {
        let input_schema = Schema::new(vec![
            Field::unnamed(DataType::Timestamp), // pk
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int64),
            Field::unnamed(DataType::Float32),
            Field::unnamed(DataType::Float64),
        ]);
        let input_pk_indices = vec![time_column_index];

        // state table schema = input schema
        let table_columns = vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Timestamp),
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Int32),
            ColumnDesc::unnamed(ColumnId::new(2), DataType::Int64),
            ColumnDesc::unnamed(ColumnId::new(3), DataType::Float32),
            ColumnDesc::unnamed(ColumnId::new(4), DataType::Float64),
        ];

        let table_pk_indices = vec![time_column_index, 0];
        let table_order_types = vec![OrderType::ascending(), OrderType::ascending()];
        let buffer_table = StateTable::from_table_catalog(
            &gen_pbtable(
                TableId::new(1),
                table_columns,
                table_order_types,
                table_pk_indices,
                0,
            ),
            store,
            None,
        )
        .await;

        let (tx, source) = MockSource::channel();
        let source = source.into_executor(input_schema, input_pk_indices);
        let gap_fill_executor = GapFillExecutor::new(GapFillExecutorArgs {
            actor_ctx: ActorContext::for_test(123),
            schema: source.schema().clone(),
            input: source,
            buffer_table,
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
        let fill_columns = vec![
            (1, FillStrategy::Interpolate),
            (2, FillStrategy::Interpolate),
            (3, FillStrategy::Interpolate),
            (4, FillStrategy::Interpolate),
        ];
        let store = MemoryStateStore::new();
        let (mut tx, mut gap_fill_executor) = create_executor(
            time_column_index,
            fill_columns,
            gap_interval,
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
            " TS               i   I    f     F
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
                " TS               i   I    f     F
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
        let fill_columns = vec![
            (1, FillStrategy::Locf),
            (2, FillStrategy::Locf),
            (3, FillStrategy::Locf),
            (4, FillStrategy::Locf),
        ];
        let store = MemoryStateStore::new();
        let (mut tx, mut gap_fill_executor) = create_executor(
            time_column_index,
            fill_columns,
            gap_interval,
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
            " TS               i   I    f     F
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
                " TS               i   I    f     F
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
        let fill_columns = vec![
            (1, FillStrategy::Null),
            (2, FillStrategy::Null),
            (3, FillStrategy::Null),
            (4, FillStrategy::Null),
        ];
        let store = MemoryStateStore::new();
        let (mut tx, mut gap_fill_executor) = create_executor(
            time_column_index,
            fill_columns,
            gap_interval,
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
            " TS               i   I    f     F
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
                " TS               i   I    f     F
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
        let fill_columns = vec![
            (1, FillStrategy::Interpolate),
            (2, FillStrategy::Locf),
            (3, FillStrategy::Null),
            (4, FillStrategy::Interpolate),
        ];
        let store = MemoryStateStore::new();
        let (mut tx, mut gap_fill_executor) = create_executor(
            time_column_index,
            fill_columns,
            gap_interval,
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
            " TS               i   I    f     F
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
                " TS               i   I    f     F
                + 2023-04-01T10:00:00 10 100 1.0 100.0
                + 2023-04-02T10:00:00 20 100 .    125.0
                + 2023-04-03T10:00:00 30 100 .    150.0
                + 2023-04-04T10:00:00 40 100 .    175.0
                + 2023-04-05T10:00:00 50 200 5.0 200.0",
            )
        );
        gap_fill_executor.expect_watermark().await;
    }
}

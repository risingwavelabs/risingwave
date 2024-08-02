// Copyright 2024 RisingWave Labs
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

use core::ops::Bound;

use risingwave_common::array::Op;
use risingwave_common::row::RowExt;
use risingwave_storage::store::PrefetchOptions;

use crate::executor::prelude::*;

pub struct GlobalApproxPercentileExecutor<S: StateStore> {
    _ctx: ActorContextRef,
    pub input: Executor,
    pub quantile: f64,
    pub base: f64,
    pub chunk_size: usize,
    /// Used for the approx percentile buckets.
    pub bucket_state_table: StateTable<S>,
    /// Used for the approx percentile count.
    pub count_state_table: StateTable<S>,
}

impl<S: StateStore> GlobalApproxPercentileExecutor<S> {
    pub fn new(
        _ctx: ActorContextRef,
        input: Executor,
        quantile: f64,
        base: f64,
        chunk_size: usize,
        bucket_state_table: StateTable<S>,
        count_state_table: StateTable<S>,
    ) -> Self {
        Self {
            _ctx,
            input,
            quantile,
            base,
            chunk_size,
            bucket_state_table,
            count_state_table,
        }
    }

    /// TODO(kwannoel): Include cache later.
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let mut bucket_state_table = self.bucket_state_table;
        let mut count_state_table = self.count_state_table;
        let mut input_stream = self.input.execute();

        // Initialize state tables.
        let first_barrier = expect_first_barrier(&mut input_stream).await?;
        bucket_state_table.init_epoch(first_barrier.epoch);
        count_state_table.init_epoch(first_barrier.epoch);
        yield Message::Barrier(first_barrier);

        // Get row count state, and row_count.
        let mut row_count_state = count_state_table.get_row(&[Datum::None; 0]).await?;
        let mut row_count = if let Some(row) = row_count_state.as_ref() {
            row.datum_at(0).unwrap().into_int64()
        } else {
            0
        };

        // Get prev output, based on the current state.
        let mut prev_output = Self::get_output(&bucket_state_table, row_count as u64, self.quantile, self.base)
            .await?;

        let mut received_input = false;

        #[for_await]
        for message in input_stream {
            match message? {
                Message::Chunk(chunk) => {
                    received_input = true;
                    for (_, row) in chunk.rows() {
                        let delta_datum = row.datum_at(1);
                        let delta: i32 = delta_datum.unwrap().into_int32();
                        row_count = row_count.checked_add(delta as i64).unwrap();

                        let pk_datum = row.datum_at(0);
                        let pk = row.project(&[0]);

                        let old_row = bucket_state_table.get_row(pk).await?;
                        let old_bucket_row_count: i64 = if let Some(row) = old_row.as_ref() {
                            row.datum_at(1).unwrap().into_int64()
                        } else {
                            0
                        };

                        let new_value = old_bucket_row_count.checked_add(delta as i64).unwrap();
                        let new_value_datum = Datum::from(ScalarImpl::Int64(new_value));
                        let new_row = &[pk_datum.map(|d| d.into()), new_value_datum];
                        if old_row.is_none() {
                            println!("inserting row: {:?}", new_row);
                            bucket_state_table.insert(new_row);
                        } else {
                            bucket_state_table.update(old_row, new_row);
                        }
                    }
                }
                Message::Barrier(barrier) => {
                    // If we haven't received any input, we don't need to update the state.
                    // This is unless row count state is empty, then we need to persist the state,
                    // and yield a NULL downstream.
                    if !received_input && row_count_state.is_some() {
                        count_state_table.commit(barrier.epoch).await?;
                        bucket_state_table.commit(barrier.epoch).await?;
                        yield Message::Barrier(barrier);
                        continue;
                    }
                    // We maintain an invariant, iff row_count_state is none,
                    // we haven't pushed any data to downstream.
                    // Naturally, if row_count_state is some,
                    // we have pushed data to downstream.
                    let new_output = Self::get_output(&bucket_state_table, row_count as u64, self.quantile, self.base)
                        .await?;
                    let percentile_chunk = if row_count_state.is_none() {
                        StreamChunk::from_rows(
                            &[(Op::Insert, &[new_output.clone()])],
                            &[DataType::Float64],
                        )
                    } else {
                        StreamChunk::from_rows(
                            &[(Op::UpdateDelete, &[prev_output.clone()]), (Op::UpdateInsert, &[new_output.clone()])],
                            &[DataType::Float64],
                        )
                    };
                    prev_output = new_output;
                    yield Message::Chunk(percentile_chunk);

                    let new_row_count_state = &[Datum::from(ScalarImpl::Int64(row_count))];
                    if let Some(row_count_state) = row_count_state {
                        count_state_table.update(row_count_state, new_row_count_state);
                    } else {
                        count_state_table.insert(new_row_count_state);
                    }
                    row_count_state = Some(new_row_count_state.into_owned_row());
                    count_state_table.commit(barrier.epoch).await?;

                    bucket_state_table.commit(barrier.epoch).await?;

                    yield Message::Barrier(barrier);
                }
                m => yield m,
            }
        }
    }

    /// We have these scenarios to consider, based on row count state.
    /// 1. We have no row count state, this means it's the bootstrap init for this executor.
    ///    Output NULL as an INSERT. Persist row count state=0.
    /// 2. We have row count state.
    ///    Output UPDATE (old_state, new_state) to downstream.
    /// FIXME(kwannoel): Only output updates when there's some input.
    async fn get_output(
        bucket_state_table: &StateTable<S>,
        row_count: u64,
        quantile: f64,
        base: f64,
    ) -> StreamExecutorResult<Datum> {
        let quantile_count = (row_count as f64 * quantile).floor() as u64;
        let mut acc_count = 0;
        let bounds: (Bound<OwnedRow>, Bound<OwnedRow>) = (Bound::Unbounded, Bound::Unbounded);
        // Just iterate over the singleton vnode.
        #[for_await]
        for keyed_row in bucket_state_table
            .iter_with_prefix(&[Datum::None; 0], &bounds, PrefetchOptions::default())
            .await?
        {
            println!("twophase order row: {:?}", keyed_row);
            let row = keyed_row?.into_owned_row();
            let count = row.datum_at(1).unwrap().into_int64();
            acc_count += count as u64;
            if acc_count > quantile_count {
                let bucket_id = row.datum_at(0).unwrap().into_int32();
                println!("two-phase bucket_id to output: {:?}, base: {:?}", bucket_id, base);
                let percentile_value = 2.0 * base.powi(bucket_id) / (base + 1.0);
                let percentile_datum = Datum::from(ScalarImpl::Float64(percentile_value.into()));
                return Ok(percentile_datum);
            }
        }
        Ok(Datum::None)
    }
}

impl<S: StateStore> Execute for GlobalApproxPercentileExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

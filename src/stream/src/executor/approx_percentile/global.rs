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
        let mut old_row_count = count_state_table.get_row(&[Datum::None; 0]).await?;
        let mut row_count = if let Some(row) = old_row_count.as_ref() {
            row.datum_at(0).unwrap().into_int64()
        } else {
            0
        };
        #[for_await]
        for message in self.input.execute() {
            match message? {
                Message::Chunk(chunk) => {
                    for (_, row) in chunk.rows() {
                        let pk_datum = row.datum_at(0);
                        let pk = row.project(&[0]);
                        let delta_datum = row.datum_at(1);
                        let delta: i32 = delta_datum.unwrap().into_int32();
                        row_count = row_count.checked_add(delta as i64).unwrap();

                        let old_row = bucket_state_table.get_row(pk).await?;
                        let old_value: i32 = if let Some(row) = old_row.as_ref() {
                            row.datum_at(0).unwrap().into_int32()
                        } else {
                            0
                        };

                        let new_value = old_value + delta;
                        let new_value_datum = Datum::from(ScalarImpl::Int32(new_value));
                        let new_row = &[pk_datum.map(|d| d.into()), new_value_datum];
                        bucket_state_table.update(old_row, new_row);
                    }
                }
                Message::Barrier(barrier) => {
                    let quantile_count = (row_count as f64 * self.quantile).ceil() as u64;
                    let mut acc_count = 0;
                    let bounds: (Bound<OwnedRow>, Bound<OwnedRow>) =
                        (Bound::Unbounded, Bound::Unbounded);
                    // Just iterate over the singleton vnode.
                    #[for_await]
                    for keyed_row in bucket_state_table
                        .iter_with_prefix(&[Datum::None; 0], &bounds, PrefetchOptions::default())
                        .await?
                    {
                        let row = keyed_row?.into_owned_row();
                        let count = row.datum_at(1).unwrap().into_int32();
                        acc_count += count as u64;
                        if acc_count >= quantile_count {
                            let bucket_id = row.datum_at(0).unwrap().into_int32();
                            let percentile_value =
                                2.0 * self.base.powi(bucket_id) / (self.base + 1.0);
                            let percentile_datum =
                                Datum::from(ScalarImpl::Float64(percentile_value.into()));
                            let percentile_row = &[percentile_datum];
                            let percentile_chunk = StreamChunk::from_rows(
                                &[(Op::Insert, percentile_row)],
                                &[DataType::Float64],
                            );
                            yield Message::Chunk(percentile_chunk);
                            break;
                        }
                    }
                    let row_count_to_persist = &[Datum::from(ScalarImpl::Int64(row_count))];
                    if let Some(old_row_count) = old_row_count {
                        count_state_table.update(old_row_count, row_count_to_persist);
                    } else {
                        count_state_table.insert(row_count_to_persist);
                    }
                    old_row_count = Some(row_count_to_persist.into_owned_row());
                    count_state_table.commit(barrier.epoch).await?;
                    bucket_state_table.commit(barrier.epoch).await?;
                    yield Message::Barrier(barrier);
                }
                m => yield m,
            }
        }
    }
}

impl<S: StateStore> Execute for GlobalApproxPercentileExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

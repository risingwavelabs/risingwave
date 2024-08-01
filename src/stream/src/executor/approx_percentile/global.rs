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

use risingwave_common::array::Op;
use risingwave_common::row::RowExt;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_pb::expr::InputRef;

use crate::executor::prelude::*;

use std::collections::HashMap;
use std::iter;

pub struct GlobalApproxPercentile<S: StateStore> {
    _ctx: ActorContextRef,
    pub input: Executor,
    pub quantile: f64,
    pub base: f64,
    pub percentile_col: InputRef,
    pub schema: Schema,
    pub chunk_size: usize,
    /// Used for the approx percentile buckets.
    pub bucket_state_table: StateTable<S>,
    /// Used for the approx percentile count.
    pub count_state_table: StateTable<S>,
}


impl<S: StateStore> GlobalApproxPercentile<S> {
    pub fn new(
        _ctx: ActorContextRef,
        input: Executor,
        quantile: f64,
        base: f64,
        percentile_col: InputRef,
        schema: Schema,
        chunk_size: usize,
        bucket_state_table: StateTable<S>,
        count_state_table: StateTable<S>,
    ) -> Self {
        Self {
            _ctx,
            input,
            quantile,
            base,
            percentile_col,
            schema,
            chunk_size,
            bucket_state_table,
            count_state_table,
        }
    }

    /// TODO(kwannoel): Include cache later.
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let percentile_index = self.percentile_col.index as usize;
        let mut bucket_state_table = self.bucket_state_table;
        let mut count_state_table = self.count_state_table;
        let row_count = count_state_table.get_row(&[Datum::None; 0]).await?;
        let mut row_count = if let Some(row) = row_count {
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
                        row_count.checked_add(delta as i64).unwrap();

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
                b @ Message::Barrier(_) => {
                    // First find the bucket for the quantile.
                    yield b;
                }
                m => yield m,
            }
        }
    }
}

impl<S: StateStore> Execute for GlobalApproxPercentile<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

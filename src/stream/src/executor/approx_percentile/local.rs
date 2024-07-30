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

use std::collections::HashMap;
use std::iter;

use risingwave_common::array::Op;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_pb::expr::InputRef;

use crate::executor::prelude::*;

pub struct LocalApproxPercentile {
    _ctx: ActorContextRef,
    pub input: Executor,
    pub base: f64,
    pub percentile_col: InputRef,
    pub schema: Schema,
    pub chunk_size: usize,
}

impl LocalApproxPercentile {
    pub fn new(
        _ctx: ActorContextRef,
        input: Executor,
        base: f64,
        percentile_col: InputRef,
        schema: Schema,
        chunk_size: usize,
    ) -> Self {
        Self {
            _ctx,
            input,
            base,
            percentile_col,
            schema,
            chunk_size,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let percentile_index = self.percentile_col.index as usize;
        #[for_await]
        for message in self.input.execute() {
            match message? {
                Message::Chunk(chunk) => {
                    let mut builder = DataChunkBuilder::new(vec![DataType::Int32; 2], self.chunk_size);
                    let chunk = chunk.project(&[percentile_index]);
                    let mut pos_counts = HashMap::new();
                    let mut neg_counts = HashMap::new();
                    let mut zero_count = 0;
                    for (op, row) in chunk.rows() {
                        let value = row.datum_at(0).unwrap();
                        let value: f64 = value.into_float64().into_inner();
                        if value < 0.0 {
                            let value = -value;
                            let bucket = value.log(self.base).floor() as i32;
                            let count = neg_counts.entry(bucket).or_insert(0);
                            match op {
                                Op::Insert | Op::UpdateInsert => *count += 1,
                                Op::Delete | Op::UpdateDelete => *count -= 1,
                            }
                        } else if value > 0.0 {
                            let bucket = value.log(self.base).floor() as i32;
                            let count = pos_counts.entry(bucket).or_insert(0);
                            match op {
                                Op::Insert | Op::UpdateInsert => *count += 1,
                                Op::Delete | Op::UpdateDelete => *count -= 1,
                            }
                        } else {
                            match op {
                                Op::Insert | Op::UpdateInsert => zero_count += 1,
                                Op::Delete | Op::UpdateDelete => zero_count -= 1,
                            }
                        }
                    }

                    for (bucket, count) in neg_counts.into_iter().chain(pos_counts.into_iter()).chain(iter::once((0, zero_count))) {
                        let row = [Datum::from(ScalarImpl::Int32(bucket)), Datum::from(ScalarImpl::Int32(count))];
                        if let Some(data_chunk) = builder.append_one_row(&row) {
                            // NOTE(kwannoel): The op here is simply ignored.
                            // The downstream global_approx_percentile will always just update its bucket counts.
                            let ops = vec![Op::Insert; data_chunk.cardinality()];
                            let chunk = StreamChunk::from_parts(ops, data_chunk);
                            yield Message::Chunk(chunk);
                        }
                    }
                    if !builder.is_empty() {
                        let data_chunk = builder.finish();
                        let ops = vec![Op::Insert; data_chunk.cardinality()];
                        let chunk = StreamChunk::from_parts(ops, data_chunk);
                        yield Message::Chunk(chunk);
                    }
                }
                m => yield m,
            }
        }
    }
}

impl Execute for LocalApproxPercentile {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

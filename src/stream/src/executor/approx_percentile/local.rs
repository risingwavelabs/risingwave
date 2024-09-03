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

use crate::executor::prelude::*;

pub struct LocalApproxPercentileExecutor {
    _ctx: ActorContextRef,
    pub input: Executor,
    pub base: f64,
    pub percentile_index: usize,
    pub chunk_size: usize,
}

impl LocalApproxPercentileExecutor {
    pub fn new(
        _ctx: ActorContextRef,
        input: Executor,
        base: f64,
        percentile_index: usize,
        chunk_size: usize,
    ) -> Self {
        Self {
            _ctx,
            input,
            base,
            percentile_index,
            chunk_size,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let percentile_index = self.percentile_index;
        #[for_await]
        for message in self.input.execute() {
            match message? {
                Message::Chunk(chunk) => {
                    let mut builder = DataChunkBuilder::new(
                        vec![DataType::Int16, DataType::Int32, DataType::Int32],
                        self.chunk_size,
                    );
                    let chunk = chunk.project(&[percentile_index]);
                    let mut pos_counts = HashMap::new();
                    let mut neg_counts = HashMap::new();
                    let mut zero_count = 0;
                    for (op, row) in chunk.rows() {
                        let value = row.datum_at(0).unwrap();
                        let value: f64 = value.into_float64().into_inner();
                        if value < 0.0 {
                            let value = -value;
                            let bucket = value.log(self.base).ceil() as i32; // TODO(kwannoel): should this be floor??
                            let count = neg_counts.entry(bucket).or_insert(0);
                            match op {
                                Op::Insert | Op::UpdateInsert => *count += 1,
                                Op::Delete | Op::UpdateDelete => *count -= 1,
                            }
                        } else if value > 0.0 {
                            let bucket = value.log(self.base).ceil() as i32;
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

                    for (sign, bucket, count) in neg_counts
                        .into_iter()
                        .map(|(b, c)| (-1, b, c))
                        .chain(pos_counts.into_iter().map(|(b, c)| (1, b, c)))
                        .chain(iter::once((0, 0, zero_count)))
                    {
                        let row = [
                            Datum::from(ScalarImpl::Int16(sign)),
                            Datum::from(ScalarImpl::Int32(bucket)),
                            Datum::from(ScalarImpl::Int32(count)),
                        ];
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
                b @ Message::Barrier(_) => yield b,
                Message::Watermark(_) => {}
            }
        }
    }
}

impl Execute for LocalApproxPercentileExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

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

use risingwave_common::types::ToOwnedDatum;

use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_common::bail;

use super::barrier_align::*;

use crate::executor::prelude::*;

pub struct MergeProjectExecutor {
    ctx: ActorContextRef,
    pub lhs_input: Executor,
    pub rhs_input: Executor,
    /// Maps input from the lhs to the output.
    pub lhs_mapping: ColIndexMapping,
    /// Maps input from the rhs to the output.
    pub rhs_mapping: ColIndexMapping,
    /// Output schema
    pub schema: Schema,
}

impl MergeProjectExecutor {
    pub fn new(
        ctx: ActorContextRef,
        lhs_input: Executor,
        rhs_input: Executor,
        lhs_mapping: ColIndexMapping,
        rhs_mapping: ColIndexMapping,
        schema: Schema,
    ) -> Self {
        Self {
            ctx,
            lhs_input,
            rhs_input,
            lhs_mapping,
            rhs_mapping,
            schema,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    pub async fn execute_inner(self) {
        let lhs_mapping = self.lhs_mapping;
        let rhs_mapping = self.rhs_mapping;
        let data_types = self.schema.fields().iter().map(|f| f.data_type()).collect::<Vec<_>>();

        {
            let mut lhs_buffer: Option<StreamChunk> = None;
            let mut rhs_buffer: Option<StreamChunk> = None;
            let aligned_stream = barrier_align(
                self.lhs_input.execute(),
                self.rhs_input.execute(),
                self.ctx.id,
                self.ctx.fragment_id,
                self.ctx.streaming_metrics.clone(),
                "Join",
            );
            pin_mut!(aligned_stream);
            #[for_await]
            for message in aligned_stream {
                match message? {
                    AlignedMessage::Left(chunk) => {
                        lhs_buffer = Some(chunk);
                    }
                    AlignedMessage::Right(chunk) => {
                        rhs_buffer = Some(chunk);
                    }
                    AlignedMessage::Barrier(barrier) => {
                        if lhs_buffer.is_none() && rhs_buffer.is_none() {
                            yield Message::Barrier(barrier);
                            continue;
                        }
                        let Some(lhs_chunk) = lhs_buffer.take() else {
                            bail!("lhs buffer should not be empty ");
                        };
                        let Some(rhs_chunk) = rhs_buffer.take() else {
                            bail!("rhs buffer should not be empty ");
                        };

                        if !(1..=2).contains(&lhs_chunk.cardinality()) {
                            bail!("lhs chunk cardinality should be 1 or 2");
                        }
                        if !(1..=2).contains(&rhs_chunk.cardinality()) {
                            bail!("rhs chunk cardinality should be 1 or 2");
                        }
                        if lhs_chunk.cardinality() != rhs_chunk.cardinality() {
                            bail!("lhs and rhs chunk cardinality should be the same");
                        }
                        let cardinality = lhs_chunk.cardinality();
                        let mut ops = Vec::with_capacity(cardinality);
                        let mut merged_rows = vec![vec![Datum::None; data_types.len()]; cardinality];
                        for (i, (op, lhs_row)) in lhs_chunk.rows().enumerate() {
                            ops.push(op);
                            for (j, d) in lhs_row.iter().enumerate() {
                                let out_index = lhs_mapping.map(j);
                                merged_rows[i][out_index] = d.to_owned_datum();
                            }
                        }

                        for (i, (_, rhs_row)) in rhs_chunk.rows().enumerate() {
                            for (j, d) in rhs_row.iter().enumerate() {
                                let out_index = rhs_mapping.map(j);
                                merged_rows[i][out_index] = d.to_owned_datum();
                            }
                        }
                        let mut builder = DataChunkBuilder::new(
                            data_types.clone(),
                            cardinality,
                        );
                        for row in merged_rows {
                            if let Some(chunk) = builder.append_one_row(&row[..]) {
                                yield Message::Chunk(StreamChunk::from_parts(ops, chunk));
                                break;
                            }
                        }
                        if !builder.is_empty() {
                            bail!("builder should be empty");
                        }
                        yield Message::Barrier(barrier);
                    }
                    AlignedMessage::WatermarkLeft(watermark) => {
                        yield Message::Watermark(watermark);
                    }
                    AlignedMessage::WatermarkRight(watermark) => {
                        yield Message::Watermark(watermark);
                    }
                }
            }
        }
    }
}

impl Execute for MergeProjectExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

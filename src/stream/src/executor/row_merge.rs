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

use risingwave_common::bail;
use risingwave_common::types::ToOwnedDatum;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::column_index_mapping::ColIndexMapping;

use super::barrier_align::*;
use crate::executor::prelude::*;

pub struct RowMergeExecutor {
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

impl RowMergeExecutor {
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
        let data_types = self
            .schema
            .fields()
            .iter()
            .map(|f| f.data_type())
            .collect::<Vec<_>>();

        {
            let mut lhs_buffer: Vec<StreamChunk> = Vec::with_capacity(1);
            let mut rhs_buffer: Vec<StreamChunk> = Vec::with_capacity(1);
            let aligned_stream = barrier_align(
                self.lhs_input.execute(),
                self.rhs_input.execute(),
                self.ctx.id,
                self.ctx.fragment_id,
                self.ctx.streaming_metrics.clone(),
                "RowMerge",
            );
            pin_mut!(aligned_stream);
            #[for_await]
            for message in aligned_stream {
                match message? {
                    AlignedMessage::Left(chunk) => {
                        lhs_buffer.push(chunk);
                    }
                    AlignedMessage::Right(chunk) => {
                        rhs_buffer.push(chunk);
                    }
                    AlignedMessage::Barrier(barrier) => {
                        if lhs_buffer.is_empty() && rhs_buffer.is_empty() {
                            yield Message::Barrier(barrier);
                            continue;
                        }
                        #[for_await]
                        for output in Self::flush_buffers(
                            &data_types,
                            &lhs_mapping,
                            &rhs_mapping,
                            &mut lhs_buffer,
                            &mut rhs_buffer,
                        ) {
                            yield output?;
                        }
                        yield Message::Barrier(barrier);
                    }
                    AlignedMessage::WatermarkLeft(watermark) => {
                        tracing::warn!("unexpected watermark from left stream: {:?}", watermark);
                    }
                    AlignedMessage::WatermarkRight(watermark) => {
                        tracing::warn!("unexpected watermark from right stream: {:?}", watermark);
                    }
                }
            }
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn flush_buffers<'a>(
        data_types: &'a [DataType],
        lhs_mapping: &'a ColIndexMapping,
        rhs_mapping: &'a ColIndexMapping,
        lhs_buffer: &'a mut Vec<StreamChunk>,
        rhs_buffer: &'a mut Vec<StreamChunk>,
    ) {
        if lhs_buffer.is_empty() {
            bail!("lhs buffer should not be empty ");
        };
        if rhs_buffer.is_empty() {
            bail!("rhs buffer should not be empty ");
        };

        for lhs_chunk in lhs_buffer.drain(..) {
            for rhs_chunk in rhs_buffer.drain(..) {
                yield Self::build_chunk(
                    data_types,
                    lhs_mapping,
                    rhs_mapping,
                    lhs_chunk.clone(),
                    rhs_chunk,
                )?;
            }
        }
    }

    fn build_chunk(
        data_types: &[DataType],
        lhs_mapping: &ColIndexMapping,
        rhs_mapping: &ColIndexMapping,
        lhs_chunk: StreamChunk,
        rhs_chunk: StreamChunk,
    ) -> Result<Message, StreamExecutorError> {
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
                // NOTE(kwannoel): Unnecessary columns will not have a mapping,
                // for instance extra row count column.
                // those can be skipped here.
                if let Some(out_index) = lhs_mapping.try_map(j) {
                    merged_rows[i][out_index] = d.to_owned_datum();
                }
            }
        }

        for (i, (_, rhs_row)) in rhs_chunk.rows().enumerate() {
            for (j, d) in rhs_row.iter().enumerate() {
                // NOTE(kwannoel): Unnecessary columns will not have a mapping,
                // for instance extra row count column.
                // those can be skipped here.
                if let Some(out_index) = rhs_mapping.try_map(j) {
                    merged_rows[i][out_index] = d.to_owned_datum();
                }
            }
        }
        let mut builder = DataChunkBuilder::new(data_types.to_vec(), cardinality);
        for row in merged_rows {
            if let Some(chunk) = builder.append_one_row(&row[..]) {
                return Ok(Message::Chunk(StreamChunk::from_parts(ops, chunk)));
            }
        }
        bail!("builder should have yielded a chunk")
    }
}

impl Execute for RowMergeExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

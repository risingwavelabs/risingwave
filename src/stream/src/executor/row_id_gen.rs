// Copyright 2023 RisingWave Labs
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

use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::column::Column;
use risingwave_common::array::serial_array::{Serial, SerialArrayBuilder};
use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::{ArrayBuilder, Op, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::util::epoch::UNIX_RISINGWAVE_DATE_EPOCH;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::row_id::RowIdGenerator;

use super::{
    expect_first_barrier, ActorContextRef, BoxedExecutor, Executor, PkIndices, PkIndicesRef,
};
use crate::executor::{Message, StreamExecutorError};

/// [`RowIdGenExecutor`] generates row id for data, where the user has not specified a pk.
pub struct RowIdGenExecutor {
    ctx: ActorContextRef,

    upstream: Option<BoxedExecutor>,

    schema: Schema,

    pk_indices: PkIndices,

    identity: String,

    row_id_index: usize,

    row_id_generator: Vec<RowIdGenerator>,
}

impl RowIdGenExecutor {
    pub fn new(
        ctx: ActorContextRef,
        upstream: BoxedExecutor,
        schema: Schema,
        pk_indices: PkIndices,
        executor_id: u64,
        row_id_index: usize,
        vnodes: Bitmap,
    ) -> Self {
        Self {
            ctx,
            upstream: Some(upstream),
            schema,
            pk_indices,
            identity: format!("RowIdGenExecutor {:X}", executor_id),
            row_id_index,
            row_id_generator: Self::new_generator(&vnodes),
        }
    }

    /// Create a new row id generator based on the assigned vnodes.
    fn new_generator(vnodes: &Bitmap) -> Vec<RowIdGenerator> {
        vnodes
            .iter_ones()
            .map(|idx| RowIdGenerator::with_epoch(idx as u32, *UNIX_RISINGWAVE_DATE_EPOCH))
            .collect()
    }

    /// Generate a row ID column according to ops.
    async fn gen_row_id_column_by_op(
        column: &Column,
        ops: Ops<'_>,
        row_id_generator: &mut RowIdGenerator,
    ) -> Column {
        let len = column.array_ref().len();
        let mut builder = SerialArrayBuilder::new(len);

        for (datum, op) in column.array_ref().iter().zip_eq_fast(ops) {
            // Only refill row_id for insert operation.
            match op {
                Op::Insert => builder.append(Some(row_id_generator.next().await.into())),
                _ => builder.append(Some(Serial::try_from(datum.unwrap()).unwrap())),
            }
        }

        builder.finish().into()
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        let mut upstream = self.upstream.take().unwrap().execute();

        // The first barrier mush propagated.
        let barrier = expect_first_barrier(&mut upstream).await?;
        yield Message::Barrier(barrier);

        let mut generator_idx_cycle = (0..self.row_id_generator.len()).cycle();

        #[for_await]
        for msg in upstream {
            let msg = msg?;

            match msg {
                Message::Chunk(chunk) => {
                    // For chunk message, we fill the row id column and then yield it.
                    let (ops, mut columns, bitmap) = chunk.into_inner();
                    columns[self.row_id_index] = Self::gen_row_id_column_by_op(
                        &columns[self.row_id_index],
                        &ops,
                        &mut self.row_id_generator[generator_idx_cycle.next().unwrap()],
                    )
                    .await;
                    yield Message::Chunk(StreamChunk::new(ops, columns, bitmap));
                }
                Message::Barrier(barrier) => {
                    // Update row id generator if vnode mapping changes.
                    // Note that: since `Update` barrier only exists between `Pause` and `Resume`
                    // barrier, duplicated row id won't be generated.
                    if let Some(vnodes) = barrier.as_update_vnode_bitmap(self.ctx.id) {
                        self.row_id_generator = Self::new_generator(&vnodes);
                    }
                    yield Message::Barrier(barrier);
                }
                Message::Watermark(watermark) => yield Message::Watermark(watermark),
            }
        }
    }
}

impl Executor for RowIdGenExecutor {
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        &self.identity
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{Array, PrimitiveArray};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::DataType;

    use super::*;
    use crate::executor::test_utils::MockSource;
    use crate::executor::{ActorContext, Executor};

    #[tokio::test]
    async fn test_row_id_gen_executor() {
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Serial),
            Field::unnamed(DataType::Int64),
        ]);
        let pk_indices = vec![0];
        let row_id_index = 0;
        let row_id_generator = Bitmap::ones(VirtualNode::COUNT);
        let (mut tx, upstream) = MockSource::channel(schema.clone(), pk_indices.clone());
        let row_id_gen_executor = Box::new(RowIdGenExecutor::new(
            ActorContext::create(233),
            Box::new(upstream),
            schema,
            pk_indices,
            1,
            row_id_index,
            row_id_generator,
        ));

        let mut row_id_gen_executor = row_id_gen_executor.execute();

        // Init barrier
        tx.push_barrier(1, false);
        row_id_gen_executor.next().await.unwrap().unwrap();

        // Insert operation
        let chunk1 = StreamChunk::from_pretty(
            " SRL I
            + . 1
            + . 2
            + . 6
            + . 7",
        );
        tx.push_chunk(chunk1);
        let chunk: StreamChunk = row_id_gen_executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        let row_id_col: &PrimitiveArray<Serial> = chunk.column_at(row_id_index).array_ref().into();
        row_id_col.iter().for_each(|row_id| {
            // Should generate row id for insert operations.
            assert!(row_id.is_some());
        });

        // Update operation
        let chunk2 = StreamChunk::from_pretty(
            "      SRL        I
            U- 32874283748  1
            U+ 32874283748 999",
        );
        tx.push_chunk(chunk2);
        let chunk: StreamChunk = row_id_gen_executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        let row_id_col: &PrimitiveArray<Serial> = chunk.column_at(row_id_index).array_ref().into();
        // Should not generate row id for update operations.
        assert_eq!(row_id_col.value_at(0).unwrap(), Serial::from(32874283748));
        assert_eq!(row_id_col.value_at(1).unwrap(), Serial::from(32874283748));

        // Delete operation
        let chunk3 = StreamChunk::from_pretty(
            "      SRL       I
            - 84629409685  1",
        );
        tx.push_chunk(chunk3);
        let chunk: StreamChunk = row_id_gen_executor
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_chunk()
            .unwrap();
        let row_id_col: &PrimitiveArray<Serial> = chunk.column_at(row_id_index).array_ref().into();
        // Should not generate row id for delete operations.
        assert_eq!(row_id_col.value_at(0).unwrap(), Serial::from(84629409685));
    }
}

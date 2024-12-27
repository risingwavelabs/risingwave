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

use risingwave_common::array::{Array, ArrayBuilder, ArrayRef, Op, SerialArrayBuilder};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::types::Serial;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::row_id::RowIdGenerator;

use crate::executor::prelude::*;

/// [`RowIdGenExecutor`] generates row id for data, where the user has not specified a pk.
pub struct RowIdGenExecutor {
    ctx: ActorContextRef,

    upstream: Option<Executor>,

    row_id_index: usize,

    row_id_generator: RowIdGenerator,
}

impl RowIdGenExecutor {
    pub fn new(
        ctx: ActorContextRef,
        upstream: Executor,
        row_id_index: usize,
        vnodes: Bitmap,
    ) -> Self {
        Self {
            ctx,
            upstream: Some(upstream),
            row_id_index,
            row_id_generator: Self::new_generator(&vnodes),
        }
    }

    /// Create a new row id generator based on the assigned vnodes.
    fn new_generator(vnodes: &Bitmap) -> RowIdGenerator {
        RowIdGenerator::new(vnodes.iter_vnodes(), vnodes.len())
    }

    /// Generate a row ID column according to ops.
    fn gen_row_id_column_by_op(
        &mut self,
        column: &ArrayRef,
        ops: &'_ [Op],
        vis: &Bitmap,
    ) -> ArrayRef {
        let len = column.len();
        let mut builder = SerialArrayBuilder::new(len);

        for ((datum, op), vis) in column.iter().zip_eq_fast(ops).zip_eq_fast(vis.iter()) {
            // Only refill row_id for insert operation.
            match op {
                Op::Insert => builder.append(Some(self.row_id_generator.next().into())),
                _ => {
                    if vis {
                        builder.append(Some(Serial::try_from(datum.unwrap()).unwrap()))
                    } else {
                        builder.append(None)
                    }
                }
            }
        }

        builder.finish().into_ref()
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        let mut upstream = self.upstream.take().unwrap().execute();

        // The first barrier mush propagated.
        let barrier = expect_first_barrier(&mut upstream).await?;
        yield Message::Barrier(barrier);

        #[for_await]
        for msg in upstream {
            let msg = msg?;

            match msg {
                Message::Chunk(chunk) => {
                    // For chunk message, we fill the row id column and then yield it.
                    let (ops, mut columns, bitmap) = chunk.into_inner();
                    columns[self.row_id_index] =
                        self.gen_row_id_column_by_op(&columns[self.row_id_index], &ops, &bitmap);
                    yield Message::Chunk(StreamChunk::with_visibility(ops, columns, bitmap));
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

impl Execute for RowIdGenExecutor {
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::PrimitiveArray;
    use risingwave_common::catalog::Field;
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::util::epoch::test_epoch;

    use super::*;
    use crate::executor::test_utils::MockSource;

    #[tokio::test]
    async fn test_row_id_gen_executor() {
        // This test only works when vnode count is 256.
        assert_eq!(VirtualNode::COUNT_FOR_TEST, 256);

        let schema = Schema::new(vec![
            Field::unnamed(DataType::Serial),
            Field::unnamed(DataType::Int64),
        ]);
        let pk_indices = vec![0];
        let row_id_index = 0;
        let row_id_generator = Bitmap::ones(VirtualNode::COUNT_FOR_TEST);
        let (mut tx, upstream) = MockSource::channel();
        let upstream = upstream.into_executor(schema.clone(), pk_indices.clone());

        let row_id_gen_executor = RowIdGenExecutor::new(
            ActorContext::for_test(233),
            upstream,
            row_id_index,
            row_id_generator,
        );
        let mut row_id_gen_executor = row_id_gen_executor.boxed().execute();

        // Init barrier
        tx.push_barrier(test_epoch(1), false);
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
        let row_id_col: &PrimitiveArray<Serial> = chunk.column_at(row_id_index).as_serial();
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
        let row_id_col: &PrimitiveArray<Serial> = chunk.column_at(row_id_index).as_serial();
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
        let row_id_col: &PrimitiveArray<Serial> = chunk.column_at(row_id_index).as_serial();
        // Should not generate row id for delete operations.
        assert_eq!(row_id_col.value_at(0).unwrap(), Serial::from(84629409685));
    }
}

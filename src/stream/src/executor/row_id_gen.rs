use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::column::Column;
use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::{ArrayBuilder, I64ArrayBuilder, Op, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::util::epoch::UNIX_SINGULARITY_DATE_EPOCH;
use risingwave_source::row_id::RowIdGenerator;

use super::{expect_first_barrier, BoxedExecutor, Executor, PkIndices};
use crate::executor::{Message, StreamExecutorError};

/// [`RowIdGenExecutor`] generates row id for data, where the user has not specified a pk.
pub struct RowIdGenExecutor {
    upstream: Option<BoxedExecutor>,

    schema: Schema,

    pk_indices: PkIndices,

    identity: String,

    row_id_index: usize,

    row_id_generator: RowIdGenerator,
}

impl RowIdGenExecutor {
    pub fn new(
        upstream: BoxedExecutor,
        schema: Schema,
        pk_indices: PkIndices,
        executor_id: u64,
        row_id_index: usize,
        vnodes: Bitmap,
    ) -> Self {
        let vnode = vnodes.next_set_bit(0).unwrap_or(0);
        Self {
            upstream: Some(upstream),
            schema,
            pk_indices,
            identity: format!("RowIdGenExecutor {:X}", executor_id),
            row_id_index,
            row_id_generator: RowIdGenerator::with_epoch(
                vnode as u32,
                *UNIX_SINGULARITY_DATE_EPOCH,
            ),
        }
    }

    /// Generate a row ID column according to ops.
    async fn gen_row_id_column_by_op(&mut self, column: &Column, ops: Ops<'_>) -> Column {
        let len = column.array_ref().len();
        let mut builder = I64ArrayBuilder::new(len);

        for i in 0..len {
            // Only refill row_id for insert operation.
            if ops.get(i) == Some(&Op::Insert) {
                builder.append(Some(self.row_id_generator.next().await));
            } else {
                builder.append(Some(
                    i64::try_from(column.array_ref().datum_at(i).unwrap()).unwrap(),
                ));
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

        #[for_await]
        for msg in upstream {
            let msg = msg?;
            if let Message::Chunk(chunk) = msg {
                // For chunk message, we fill the row id column and then yield it.
                let (ops, mut columns, bitmap) = chunk.into_inner();
                columns[self.row_id_index] = self
                    .gen_row_id_column_by_op(&columns[self.row_id_index], &ops)
                    .await;
                yield Message::Chunk(StreamChunk::new(ops, columns, bitmap));
            } else {
                // For barrier message or watermark message, we just yield it.
                yield msg;
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

    fn pk_indices(&self) -> super::PkIndicesRef<'_> {
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
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::{DataType, VIRTUAL_NODE_COUNT};

    use super::*;
    use crate::executor::test_utils::MockSource;
    use crate::executor::Executor;

    #[tokio::test]
    async fn test_row_id_gen_executor() {
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int64),
            Field::unnamed(DataType::Int64),
        ]);
        let pk_indices = vec![0];
        let row_id_index = 0;
        let row_id_generator = Bitmap::all_high_bits(VIRTUAL_NODE_COUNT);
        let (mut tx, upstream) = MockSource::channel(schema.clone(), pk_indices.clone());
        let row_id_gen_executor = Box::new(RowIdGenExecutor::new(
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
            " I I
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
        let row_id_col: &PrimitiveArray<i64> = chunk.column_at(row_id_index).array_ref().into();
        row_id_col.iter().for_each(|row_id| {
            // Should generate row id for insert operations.
            assert!(row_id.is_some());
        });

        // Update operation
        let chunk2 = StreamChunk::from_pretty(
            "      I        I
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
        let row_id_col: &PrimitiveArray<i64> = chunk.column_at(row_id_index).array_ref().into();
        // Should not generate row id for update operations.
        assert_eq!(row_id_col.value_at(0).unwrap(), 32874283748);
        assert_eq!(row_id_col.value_at(1).unwrap(), 32874283748);

        // Delete operation
        let chunk3 = StreamChunk::from_pretty(
            "      I       I
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
        let row_id_col: &PrimitiveArray<i64> = chunk.column_at(row_id_index).array_ref().into();
        // Should not generate row id for delete operations.
        assert_eq!(row_id_col.value_at(0).unwrap(), 84629409685);
    }
}

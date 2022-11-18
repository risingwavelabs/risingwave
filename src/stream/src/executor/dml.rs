use futures::future::Either;
use futures::stream::select;
use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::{ColumnDesc, Schema, TableId};
use risingwave_source::dml_manager::DmlManagerRef;

use super::error::StreamExecutorError;
use super::{
    expect_first_barrier, BoxedExecutor, BoxedMessageStream, Executor, Message, PkIndices,
    PkIndicesRef,
};

pub struct DmlExecutor {
    upstream: BoxedExecutor,
    schema: Schema,
    pk_indices: PkIndices,
    identity: String,
    dml_manager: DmlManagerRef,
    table_id: TableId,
    column_descs: Vec<ColumnDesc>,
}

impl DmlExecutor {
    pub fn new(
        upstream: BoxedExecutor,
        schema: Schema,
        pk_indices: PkIndices,
        executor_id: u64,
        dml_manager: DmlManagerRef,
        table_id: TableId,
        column_descs: Vec<ColumnDesc>,
    ) -> Self {
        Self {
            upstream,
            schema,
            pk_indices,
            identity: format!("DmlExecutor {:X}", executor_id),
            dml_manager,
            table_id,
            column_descs,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self: Box<Self>) {
        let mut upstream = self.upstream.execute();

        let batch_reader = self
            .dml_manager
            .register_reader(&self.table_id, &self.column_descs);

        let batch_reader = batch_reader
            .stream_reader_v2()
            .into_stream_v2()
            .map(Either::Right);

        let barrier = expect_first_barrier(&mut upstream).await?;

        // The first barrier message should be propagated.
        yield Message::Barrier(barrier);

        let upstream = upstream.map(Either::Left);

        let stream = select(upstream, batch_reader);

        #[for_await]
        for input_msg in stream {
            match input_msg {
                Either::Left(msg) => {
                    let msg: Message = msg?;
                    yield msg;
                }
                Either::Right(chunk) => {
                    let chunk: StreamChunk = chunk.map_err(StreamExecutorError::connector_error)?;
                    yield Message::Chunk(chunk);
                }
            }
        }
    }
}

impl Executor for DmlExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
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
    use std::sync::Arc;

    use risingwave_common::catalog::{ColumnId, Field};
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::DataType;
    use risingwave_source::dml_manager::DmlManager;

    use super::*;
    use crate::executor::test_utils::MockSource;

    #[tokio::test]
    async fn test_dml_executor() {
        let table_id = TableId::default();
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int64),
            Field::unnamed(DataType::Int64),
        ]);
        let column_descs = vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64),
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Int64),
        ];
        let pk_indices = vec![0];
        let dml_manager = Arc::new(DmlManager::new());

        let (mut tx, source) = MockSource::channel(schema.clone(), pk_indices.clone());
        let dml_executor = Box::new(DmlExecutor::new(
            Box::new(source),
            schema,
            pk_indices,
            1,
            dml_manager.clone(),
            table_id,
            column_descs,
        ));
        let mut dml_executor = dml_executor.execute();

        let stream_chunk1 = StreamChunk::from_pretty(
            " I I
            + 1 1
            + 2 2
            + 3 6",
        );
        let stream_chunk2 = StreamChunk::from_pretty(
            " I I
            + 88 43",
        );
        let stream_chunk3 = StreamChunk::from_pretty(
            " I I
            + 199 40
            + 978 72
            + 134 41
            + 398 98",
        );
        let batch_chunk = StreamChunk::from_pretty(
            "  I I
            U+ 1 11
            U+ 2 22",
        );

        // The first barrier
        tx.push_barrier(1, false);
        dml_executor.next().await.unwrap().unwrap();

        // Messages from upstream streaming executor
        tx.push_chunk(stream_chunk1);
        tx.push_chunk(stream_chunk2);
        tx.push_chunk(stream_chunk3);

        // Message from batch
        let _ = dml_manager.write_chunk(&table_id, batch_chunk).unwrap();

        // Consume the 1st message from upstream executor
        let msg = dml_executor.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I I
                + 1 1
                + 2 2
                + 3 6",
            )
        );

        // Consume the message from batch (because dml executor selects from the streams in a round
        // robin way)
        let msg = dml_executor.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                "  I I
                U+ 1 11
                U+ 2 22",
            )
        );

        // Consume the 2nd message from upstream executor
        let msg = dml_executor.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I I
                + 88 43",
            )
        );

        // Consume the 3rd message from upstream executor
        let msg = dml_executor.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I I
                + 199 40
                + 978 72
                + 134 41
                + 398 98",
            )
        );
    }
}

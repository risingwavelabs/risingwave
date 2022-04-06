use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_storage::table::cell_based_table::CellBasedTable;
use risingwave_storage::StateStore;

use super::error::{StreamExecutorError, TracedStreamExecutorError};
use super::{Executor, ExecutorInfo, Message};
use crate::executor_v2::BoxedMessageStream;

pub struct BatchQueryExecutor<S: StateStore> {
    /// The [`CellBasedTable`] that needs to be queried
    table: CellBasedTable<S>,

    /// The number of tuples in one [`StreamChunk`]
    batch_size: usize,

    info: ExecutorInfo,

    #[allow(dead_code)]
    /// Indices of the columns on which key distribution depends.
    key_indices: Vec<usize>,
}

impl<S> BatchQueryExecutor<S>
where
    S: StateStore,
{
    pub fn new(
        table: CellBasedTable<S>,
        batch_size: usize,
        info: ExecutorInfo,
        key_indices: Vec<usize>,
    ) -> Self {
        Self {
            table,
            batch_size,
            info,
            key_indices,
        }
    }

    #[try_stream(ok = Message, error = TracedStreamExecutorError)]
    async fn execute_inner(self, epoch: u64) {
        let mut iter = self
            .table
            .iter(epoch)
            .await
            .map_err(StreamExecutorError::storage)?;

        while let Some(data_chunk) = iter
            .collect_data_chunk(&self.table, Some(self.batch_size))
            .await
            .map_err(StreamExecutorError::storage)?
        {
            let ops = vec![Op::Insert; data_chunk.cardinality()];
            let stream_chunk = StreamChunk::from_parts(ops, data_chunk);
            yield Message::Chunk(stream_chunk);
        }
    }
}

impl<S> Executor for BatchQueryExecutor<S>
where
    S: StateStore,
{
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        unreachable!("should call `execute_with_epoch`")
    }

    fn schema(&self) -> &Schema {
        &self.info.schema
    }

    fn pk_indices(&self) -> super::PkIndicesRef {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }

    fn execute_with_epoch(self: Box<Self>, epoch: u64) -> BoxedMessageStream {
        self.execute_inner(epoch).boxed()
    }
}

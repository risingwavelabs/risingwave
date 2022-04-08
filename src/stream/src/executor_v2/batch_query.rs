use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_storage::table::cell_based_table::CellBasedTable;
use risingwave_storage::StateStore;

use super::error::TracedStreamExecutorError;
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
    pub const DEFAULT_BATCH_SIZE: usize = 100;

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
        let mut iter = self.table.iter(epoch).await?;

        while let Some(data_chunk) = iter
            .collect_data_chunk(&self.table, Some(self.batch_size))
            .await?
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

#[cfg(test)]
mod test {

    use std::vec;

    use futures_async_stream::for_await;

    use super::*;
    use crate::executor_v2::mview::test_utils::gen_basic_table;

    #[tokio::test]
    async fn test_basic() {
        let test_batch_size = 50;
        let test_batch_count = 5;
        let table = gen_basic_table(test_batch_count * test_batch_size).await;

        let info = ExecutorInfo {
            schema: table.schema().clone(),
            pk_indices: vec![0, 1],
            identity: "BatchQuery".to_owned(),
        };
        let executor = Box::new(BatchQueryExecutor::new(
            table,
            test_batch_size,
            info,
            vec![],
        ));

        let stream = executor.execute_with_epoch(u64::MAX);
        let mut batch_cnt = 0;

        #[for_await]
        for msg in stream {
            let msg: Message = msg.unwrap();
            let chunk = msg.as_chunk().unwrap();
            let data = *chunk
                .column_at(0)
                .array_ref()
                .datum_at(0)
                .unwrap()
                .as_int32();
            assert_eq!(data, (batch_cnt * test_batch_size) as i32);
            batch_cnt += 1;
        }

        assert_eq!(batch_cnt, test_batch_count)
    }
}

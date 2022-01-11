use std::sync::Arc;

use async_trait::async_trait;
use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::{Op, RwError, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_storage::table::TableIter;
use risingwave_storage::StateStore;

use super::{MViewTable, MViewTableIter};
use crate::executor::{Executor, Message, PkIndices, PkIndicesRef};
const DEFAULT_BATCH_SIZE: usize = 100;

/// [`BatchQueryExecutor`] pushes m-view data batch to the downstream executor. Currently, this
/// executor is used as input of the [`ChainExecutor`] to support MV-on-MV.
pub struct BatchQueryExecutor<S: StateStore> {
    /// The primary key indices of the schema
    pk_indices: PkIndices,
    /// The [`MViewTable`] that needs to be queried
    table: MViewTable<S>,
    /// The number of tuples in one [`StreamChunk`]
    batch_size: usize,
    /// Inner iterator that read [`MViewTable`]
    iter: Option<MViewTableIter<S>>,
}

impl<S: StateStore> std::fmt::Debug for BatchQueryExecutor<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchQueryExecutor")
            .field("table", &self.table)
            .field("pk_indices", &self.pk_indices)
            .field("batch_size", &self.batch_size)
            .finish()
    }
}

impl<S: StateStore> BatchQueryExecutor<S> {
    fn new(table: MViewTable<S>, pk_indices: PkIndices) -> Self {
        Self::new_with_batch_size(table, pk_indices, DEFAULT_BATCH_SIZE)
    }

    fn new_with_batch_size(table: MViewTable<S>, pk_indices: PkIndices, batch_size: usize) -> Self {
        Self {
            pk_indices,
            table,
            batch_size,
            iter: None,
        }
    }
}
impl<S: StateStore> BatchQueryExecutor<S> {
    async fn next_inner(&mut self) -> Result<Message> {
        if self.iter.is_none() {
            self.iter = Some(self.table.iter().await.unwrap());
        }
        let iter = self.iter.as_mut().unwrap();
        let mut count = 0;
        let mut builders = self.table.schema().create_array_builders(self.batch_size)?;

        while let Some(row) = iter.next().await.unwrap() {
            builders
                .iter_mut()
                .zip(row.0.iter())
                .for_each(|(builder, data)| builder.append_datum(data).unwrap());
            count += 1;
            if count >= self.batch_size {
                break;
            }
        }
        if count == 0 {
            // TODO: May be refactored in the future.
            Err(RwError::from(ErrorCode::EOF))
        } else {
            let columns: Vec<Column> = builders
                .into_iter()
                .map(|builder| -> Result<_> { Ok(Column::new(Arc::new(builder.finish()?))) })
                .try_collect()?;

            Ok(Message::Chunk(StreamChunk::new(
                vec![Op::Insert; count],
                columns,
                None,
            )))
        }
    }
}

#[async_trait]
impl<S: StateStore> Executor for BatchQueryExecutor<S> {
    async fn next(&mut self) -> Result<Message> {
        self.next_inner().await
    }

    fn schema(&self) -> &Schema {
        self.table.schema()
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.pk_indices
    }

    fn identity(&self) -> &'static str {
        "BatchQueryExecutor"
    }
}

#[cfg(test)]
mod test {

    use std::vec;

    use super::BatchQueryExecutor;
    use crate::executor::mview::test_utils::gen_basic_table;
    use crate::executor::{Executor, Message};
    #[tokio::test]
    async fn test_basic() {
        let test_batch_size = 50;
        let test_batch_count = 5;
        let table = gen_basic_table(test_batch_count * test_batch_size).await;
        let mut node = BatchQueryExecutor::new_with_batch_size(table, vec![0, 1], test_batch_size);
        let mut batch_cnt = 0;
        while let Ok(Message::Chunk(sc)) = node.next().await {
            let data = *sc.column(0).array_ref().datum_at(0).unwrap().as_int32();
            assert_eq!(data, (batch_cnt * test_batch_size) as i32);
            batch_cnt += 1;
        }
        assert_eq!(batch_cnt, test_batch_count)
    }
}

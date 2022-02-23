use async_trait::async_trait;
use itertools::Itertools;
use risingwave_common::array::{Op, RwError, StreamChunk};
use risingwave_common::catalog::{Schema, TableId};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::stream_plan;
use risingwave_storage::table::{ScannableTableRef, TableIterRef};

use crate::executor::{Executor, Message, PkIndices, PkIndicesRef};
use crate::task::ExecutorParams;

const DEFAULT_BATCH_SIZE: usize = 100;

/// [`BatchQueryExecutor`] pushes m-view data batch to the downstream executor. Currently, this
/// executor is used as input of the [`ChainExecutor`] to support MV-on-MV.
pub struct BatchQueryExecutor {
    /// The primary key indices of the schema
    pk_indices: PkIndices,
    /// The [`MViewTable`] that needs to be queried
    table: ScannableTableRef,
    /// The number of tuples in one [`StreamChunk`]
    batch_size: usize,
    /// Inner iterator that reads [`MViewTable`]
    iter: Option<TableIterRef>,

    schema: Schema,

    epoch: Option<u64>,
    /// Logical Operator Info
    op_info: String,
}

impl std::fmt::Debug for BatchQueryExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchQueryExecutor")
            .field("table", &self.table)
            .field("pk_indices", &self.pk_indices)
            .field("batch_size", &self.batch_size)
            .finish()
    }
}

impl BatchQueryExecutor {
    pub fn create(params: ExecutorParams, node: &stream_plan::BatchPlanNode) -> Result<Self> {
        let table_id = TableId::from(&node.table_ref_id);
        let table_manager = params.env.table_manager();
        let table = table_manager.get_table(&table_id)?;

        Ok(Self::new(table.clone(), params.pk_indices, params.op_info))
    }

    pub fn new(table: ScannableTableRef, pk_indices: PkIndices, op_info: String) -> Self {
        Self::new_with_batch_size(table, pk_indices, DEFAULT_BATCH_SIZE, op_info)
    }

    pub fn new_with_batch_size(
        table: ScannableTableRef,
        pk_indices: PkIndices,
        batch_size: usize,
        op_info: String,
    ) -> Self {
        let schema = table.schema().into_owned();
        Self {
            pk_indices,
            table,
            batch_size,
            iter: None,
            schema,
            epoch: None,
            op_info,
        }
    }
}

#[async_trait]
impl Executor for BatchQueryExecutor {
    async fn next(&mut self) -> Result<Message> {
        if self.iter.is_none() {
            self.iter = Some(
                self.table
                    .iter(self.epoch.expect("epoch has not been set"))
                    .await
                    .unwrap(),
            );
        }

        let iter = self.iter.as_mut().unwrap();
        let column_indices = (0..self.table.schema().len()).collect_vec();

        let data_chunk = self
            .table
            .collect_from_iter(iter, &column_indices, Some(self.batch_size))
            .await?
            .ok_or_else(|| RwError::from(ErrorCode::Eof))?;

        let ops = vec![Op::Insert; data_chunk.cardinality()];
        let stream_chunk = StreamChunk::from_parts(ops, data_chunk);
        Ok(Message::Chunk(stream_chunk))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.pk_indices
    }

    fn identity(&self) -> &'static str {
        "BatchQueryExecutor"
    }

    fn logical_operator_info(&self) -> &str {
        &self.op_info
    }

    fn init(&mut self, epoch: u64) -> Result<()> {
        if let Some(_old) = self.epoch.replace(epoch) {
            panic!("epoch cannot be initialized twice");
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {

    use std::sync::Arc;
    use std::vec;

    use super::*;
    use crate::executor::mview::test_utils::gen_basic_table;

    #[tokio::test]
    async fn test_basic() {
        let test_batch_size = 50;
        let test_batch_count = 5;
        let table = Arc::new(gen_basic_table(test_batch_count * test_batch_size).await)
            as ScannableTableRef;
        let mut node = BatchQueryExecutor::new_with_batch_size(
            table,
            vec![0, 1],
            test_batch_size,
            "BatchQueryExecutor".to_string(),
        );
        node.init(u64::MAX).unwrap();
        let mut batch_cnt = 0;
        while let Ok(Message::Chunk(sc)) = node.next().await {
            let data = *sc.column_at(0).array_ref().datum_at(0).unwrap().as_int32();
            assert_eq!(data, (batch_cnt * test_batch_size) as i32);
            batch_cnt += 1;
        }
        assert_eq!(batch_cnt, test_batch_count)
    }

    #[should_panic]
    #[tokio::test]
    async fn test_init_epoch_twice() {
        let test_batch_size = 50;
        let test_batch_count = 5;
        let table = Arc::new(gen_basic_table(test_batch_count * test_batch_size).await)
            as ScannableTableRef;
        let mut node = BatchQueryExecutor::new_with_batch_size(
            table,
            vec![0, 1],
            test_batch_size,
            "BatchQueryExecutor".to_string(),
        );
        node.init(u64::MAX).unwrap();
        node.init(0).unwrap();
    }
}

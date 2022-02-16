use std::borrow::Cow;
use std::sync::Arc;

use async_trait::async_trait;
use risingwave_common::array::{DataChunk, StreamChunk};
use risingwave_common::catalog::{Schema, TableId};
use risingwave_common::error::Result;
use risingwave_common::util::downcast_arc;

use super::mview::MViewTable;
use super::{ScannableTable, TableIterRef};
use crate::memory::MemoryStateStore;
use crate::{Keyspace, Table, TableColumnDesc};

#[derive(Debug)]
pub struct TestTable {
    inner: Arc<MViewTable<MemoryStateStore>>,
}

impl TestTable {
    pub fn new(table_id: &TableId, table_columns: Vec<TableColumnDesc>) -> Self {
        let keyspace = Keyspace::table_root(MemoryStateStore::new(), table_id);
        let inner = MViewTable::new_batch(keyspace, table_columns);
        Self {
            inner: Arc::new(inner),
        }
    }

    pub fn from_table_v2(table: Arc<dyn ScannableTable>) -> Self {
        Self {
            inner: downcast_arc::<MViewTable<MemoryStateStore>>(table.into_any()).unwrap(),
        }
    }
}

#[async_trait]
impl ScannableTable for TestTable {
    async fn iter(&self, epoch: u64) -> Result<TableIterRef> {
        self.inner.iter(epoch).await
    }

    fn into_any(self: Arc<Self>) -> Arc<dyn std::any::Any + Sync + Send> {
        self
    }

    fn schema(&self) -> Cow<Schema> {
        self.inner.schema()
    }

    fn column_descs(&self) -> Cow<[TableColumnDesc]> {
        self.inner.column_descs()
    }

    fn is_shared_storage(&self) -> bool {
        self.inner.is_shared_storage()
    }
}

#[async_trait]
impl Table for TestTable {
    async fn append(&self, data: DataChunk) -> Result<usize> {
        todo!()
    }

    fn write(&self, chunk: &StreamChunk) -> Result<usize> {
        todo!()
    }

    fn get_column_ids(&self) -> Vec<i32> {
        todo!()
    }

    fn index_of_column_id(&self, column_id: i32) -> Result<usize> {
        todo!()
    }
}

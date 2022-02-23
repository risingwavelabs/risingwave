use std::borrow::Cow;
use std::sync::Arc;

use async_trait::async_trait;
use risingwave_common::array::{DataChunk, DataChunkRef};
use risingwave_common::catalog::{Field, Schema, TableId};
use risingwave_common::error::Result;
use tokio::sync::RwLock;

use super::{ScannableTable, TableIterRef};
use crate::TableColumnDesc;

#[derive(Debug)]
pub struct TestTable {
    chunks: Arc<RwLock<Vec<DataChunkRef>>>,

    column_descs: Vec<TableColumnDesc>,

    schema: Schema,
}

impl TestTable {
    pub fn new(_table_id: &TableId, column_descs: Vec<TableColumnDesc>) -> Self {
        let schema = {
            let fields = column_descs
                .iter()
                .map(|c| Field::with_name(c.data_type, c.name.clone()))
                .collect();
            Schema::new(fields)
        };

        Self {
            chunks: Arc::new(RwLock::new(Default::default())),
            column_descs,
            schema,
        }
    }
}

#[async_trait]
impl ScannableTable for TestTable {
    async fn iter(&self, _epoch: u64) -> Result<TableIterRef> {
        unimplemented!()
    }

    fn into_any(self: Arc<Self>) -> Arc<dyn std::any::Any + Sync + Send> {
        self
    }

    fn schema(&self) -> Cow<Schema> {
        Cow::Borrowed(&self.schema)
    }

    fn column_descs(&self) -> Cow<[TableColumnDesc]> {
        Cow::Borrowed(&self.column_descs)
    }

    fn is_shared_storage(&self) -> bool {
        false
    }
}

impl TestTable {
    pub async fn append(&self, data: DataChunk) -> Result<()> {
        self.chunks.write().await.push(data.into());
        Ok(())
    }
}

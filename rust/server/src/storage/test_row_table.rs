use crate::storage::{MemRowTableInner, RowTable};
use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender};
use risingwave_common::array::Row;
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;

use super::{Cell, CellId};

pub enum TestRowTableEvent {
    Ingest(Vec<(Row, Option<Row>)>),
    Insert(Row, Row),
    Delete(Row),
    InsertBatch(Vec<(Row, bool)>),
    InsertCell(Row, CellId, Cell),
    DeleteCell(Row, CellId),
}

pub struct TestRowTable {
    schema: Schema,
    pks: Vec<usize>,
    inner: RwLock<MemRowTableInner>,
    sender: UnboundedSender<TestRowTableEvent>,
    receiver: Arc<Mutex<UnboundedReceiver<TestRowTableEvent>>>,
}

impl TestRowTable {
    pub fn new(schema: Schema, pks: Vec<usize>) -> Self {
        let inner = RwLock::new(MemRowTableInner::default());
        let (tx, rx) = futures::channel::mpsc::unbounded();
        Self {
            schema,
            pks,
            inner,
            sender: tx,
            receiver: Arc::new(Mutex::new(rx)),
        }
    }

    pub fn get_receiver(&self) -> Arc<Mutex<UnboundedReceiver<TestRowTableEvent>>> {
        self.receiver.clone()
    }
}

impl RowTable for TestRowTable {
    fn ingest(&self, batch: Vec<(Row, Option<Row>)>) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        inner.ingest(batch.clone())?;
        self.sender
            .unbounded_send(TestRowTableEvent::Ingest(batch))
            .unwrap();
        Ok(())
    }

    fn insert(&self, key: Row, value: Row) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        inner.insert_row(key.clone(), value.clone())?;
        self.sender
            .unbounded_send(TestRowTableEvent::Insert(key, value))
            .unwrap();
        Ok(())
    }

    fn delete(&self, key: Row) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        inner.delete_row(key.clone())?;
        self.sender
            .unbounded_send(TestRowTableEvent::Delete(key))
            .unwrap();
        Ok(())
    }

    fn get(&self, key: Row) -> Result<Option<Row>> {
        let inner = self.inner.read().unwrap();
        inner.get_row(key)
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn get_pk(&self) -> Vec<usize> {
        self.pks.clone()
    }

    fn insert_cell(&self, key: Row, cell_id: super::CellId, cell: super::Cell) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        inner.insert_cell((key.clone(), cell_id), cell.clone())?;
        self.sender
            .unbounded_send(TestRowTableEvent::InsertCell(key, cell_id, cell))
            .unwrap();
        Ok(())
    }

    fn get_cell(&self, key: Row, cell_id: super::CellId) -> Result<Option<super::Cell>> {
        let inner = self.inner.read().unwrap();
        inner.get_cell((key, cell_id))
    }

    fn delete_cell(&self, key: Row, cell_id: super::CellId) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        inner.delete_cell((key.clone(), cell_id))?;
        self.sender
            .unbounded_send(TestRowTableEvent::DeleteCell(key, cell_id))
            .unwrap();
        Ok(())
    }
}

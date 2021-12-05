use async_std::sync::Mutex;
use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender};
use std::collections::btree_map::{BTreeMap, Entry};
use std::sync::{Arc, RwLock};

use risingwave_common::array::data_chunk_iter::Row;
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;
use risingwave_common::types::Datum;

pub type RowTableRef = Arc<dyn RowTable>;

pub type MemRowTableRef = Arc<MemRowTable>;
pub type MemTableRowIter = <BTreeMap<Row, Row> as IntoIterator>::IntoIter;

pub type Cell = Datum;
pub type CellId = i32;

pub trait RowTable: Sync + Send {
    /// Ingest a batch of rows. Insert when `Option` is `Some` and delete when `Option` is None.
    fn ingest(&self, batch: Vec<(Row, Option<Row>)>) -> Result<()>;

    /// Insert a key value pair.
    fn insert(&self, key: Row, value: Row) -> Result<()>;

    /// Insert a (key, cell id) -> cell pair.
    fn insert_cell(&self, key: Row, cell_id: CellId, cell: Cell) -> Result<()>;

    /// Delete a row by key.
    fn delete(&self, key: Row) -> Result<()>;

    /// Delete a cell by (key, cell id)
    fn delete_cell(&self, key: Row, cell_id: CellId) -> Result<()>;

    /// Get a row by key.
    fn get(&self, key: Row) -> Result<Option<Row>>;

    /// Get a cell by (key, cell id)
    fn get_cell(&self, key: Row, cell_id: CellId) -> Result<Option<Cell>>;

    /// Return the schema of the table.
    fn schema(&self) -> &Schema;

    /// Get all primary key columns.
    fn get_pk(&self) -> Vec<usize>;
}

#[derive(Debug, Default)]
pub(crate) struct MemRowTableInner {
    /// data represents a mapping from primary key to row data.
    data: BTreeMap<(Row, CellId), Cell>,
}

pub enum RowTableEvent {
    Ingest(Vec<(Row, Option<Row>)>),
    Insert(Row, Row),
    Delete(Row),
    InsertBatch(Vec<(Row, bool)>),
    InsertCell(Row, CellId, Cell),
    DeleteCell(Row, CellId),
}

pub struct MemRowTable {
    schema: Schema,
    pks: Vec<usize>,
    inner: RwLock<MemRowTableInner>,
    sender: UnboundedSender<RowTableEvent>,
    receiver: Arc<Mutex<UnboundedReceiver<RowTableEvent>>>,
}

impl MemRowTableInner {
    pub fn ingest(&mut self, batch: Vec<(Row, Option<Row>)>) -> Result<()> {
        for (key, value) in batch {
            match value {
                Some(value) => self.insert_row(key, value)?,
                None => self.delete_row(key)?,
            }
        }
        Ok(())
    }

    pub fn insert_cell(&mut self, key: (Row, CellId), value: Cell) -> Result<()> {
        self.data.insert(key, value);
        Ok(())
    }

    pub fn delete_cell(&mut self, key: (Row, CellId)) -> Result<()> {
        self.data.remove(&key);
        Ok(())
    }

    pub fn get_cell(&self, key: (Row, CellId)) -> Result<Option<Cell>> {
        Ok(self.data.get(&key).map(Clone::clone))
    }

    pub fn insert_row(&mut self, key: Row, value: Row) -> Result<()> {
        for (cell_id, cell) in value.0.iter().enumerate() {
            self.insert_cell((key.clone(), cell_id as CellId), cell.clone())?;
        }
        Ok(())
    }

    pub fn delete_row(&mut self, key: Row) -> Result<()> {
        // slow but easy to impl, fine as mock?
        self.data.retain(|(k, _), _| *k != key);
        Ok(())
    }

    pub fn get_row(&self, key: Row) -> Result<Option<Row>> {
        let mut cells = vec![];
        for ((k, cell_id), cell) in self.data.range((key.clone(), 0)..) {
            if *k != key {
                break;
            }
            assert_eq!(*cell_id, cells.len() as CellId);
            cells.push(cell.clone());
        }
        if cells.is_empty() {
            return Ok(None);
        }
        Ok(Some(Row(cells)))
    }
}

impl MemRowTable {
    /// Create an iterator to scan over all records.
    pub fn iter(&self) -> Result<MemTableRowIter> {
        let inner = self.inner.read().unwrap();
        let cells = inner.data.iter().collect::<Vec<_>>();
        let snapshot = cells
            .iter()
            .fold(BTreeMap::new(), |mut m, ((key, _), cell)| {
                match m.entry(key.clone()) {
                    Entry::Vacant(v) => {
                        v.insert(Row(vec![(*cell).clone()]));
                    }
                    Entry::Occupied(mut o) => o.get_mut().0.push((*cell).clone()),
                }
                m
            });
        Ok(snapshot.into_iter())
    }

    /// Init.
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

    pub fn get_receiver(&self) -> Arc<Mutex<UnboundedReceiver<RowTableEvent>>> {
        self.receiver.clone()
    }
}

impl RowTable for MemRowTable {
    fn ingest(&self, batch: Vec<(Row, Option<Row>)>) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        inner.ingest(batch.clone())?;
        self.sender
            .unbounded_send(RowTableEvent::Ingest(batch))
            .unwrap();
        Ok(())
    }

    fn insert(&self, key: Row, value: Row) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        inner.insert_row(key.clone(), value.clone())?;
        self.sender
            .unbounded_send(RowTableEvent::Insert(key, value))
            .unwrap();
        Ok(())
    }

    fn delete(&self, key: Row) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        inner.delete_row(key.clone())?;
        self.sender
            .unbounded_send(RowTableEvent::Delete(key))
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

    fn insert_cell(&self, key: Row, cell_id: CellId, cell: Cell) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        inner.insert_cell((key.clone(), cell_id), cell.clone())?;
        self.sender
            .unbounded_send(RowTableEvent::InsertCell(key, cell_id, cell))
            .unwrap();
        Ok(())
    }

    fn get_cell(&self, key: Row, cell_id: CellId) -> Result<Option<Cell>> {
        let inner = self.inner.read().unwrap();
        inner.get_cell((key, cell_id))
    }

    fn delete_cell(&self, key: Row, cell_id: CellId) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        inner.delete_cell((key.clone(), cell_id))?;
        self.sender
            .unbounded_send(RowTableEvent::DeleteCell(key, cell_id))
            .unwrap();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use risingwave_common::types::Scalar;

    fn mock_pk(pk: i32) -> Row {
        Row(vec![Some(pk.to_scalar_value())])
    }

    fn mock_one_row(v1: i32, v2: i32) -> Row {
        Row(vec![Some(v1.to_scalar_value()), Some(v2.to_scalar_value())])
    }

    #[test]
    fn test_row_table() {
        let mem_table = MemRowTable::new(Schema::default(), vec![]);
        // Insert (1) -> (1,4), (2) -> (6,8)
        mem_table
            .ingest(vec![
                (mock_pk(1), Some(mock_one_row(1, 4))),
                (mock_pk(2), Some(mock_one_row(6, 8))),
            ])
            .unwrap();

        // Check (1) -> (1,4), (2) -> (6,8)
        let res_row1 = mem_table.get(mock_pk(1)).unwrap();
        let datum1 = res_row1.unwrap().0.get(0).unwrap().clone();
        assert_eq!(*datum1.unwrap().as_int32(), 1);
        let res_row2 = mem_table.get(mock_pk(2)).unwrap();
        let datum2 = res_row2.unwrap().0.get(0).unwrap().clone();
        assert_eq!(*datum2.unwrap().as_int32(), 6);

        // Delete (2) -> (6,8)
        mem_table.ingest(vec![(mock_pk(2), None)]).unwrap();
        assert!(mem_table.get(mock_pk(2)).unwrap().is_none());
    }
}

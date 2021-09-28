use crate::error::Result;
use crate::types::Datum;
use std::{
    cmp::Ordering,
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use itertools::Itertools;
use smallvec::SmallVec;

#[derive(Clone, Debug, Default)]
pub struct Row(pub SmallVec<[Datum; 5]>);

impl PartialEq for Row {
    fn eq(&self, other: &Self) -> bool {
        if self.0.len() != other.0.len() {
            return false;
        }
        self.0.iter().zip(other.0.iter()).all_equal()
    }
}

impl PartialOrd for Row {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.0.len() != other.0.len() {
            return None;
        }
        for (x, y) in self.0.iter().zip(other.0.iter()) {
            match x.partial_cmp(y) {
                Some(Ordering::Equal) => continue,
                order => return order,
            }
        }
        Some(Ordering::Equal)
    }
}

impl Eq for Row {}

impl Ord for Row {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

#[derive(Debug, Default)]
struct MemRowTableInner {
    /// data represents a mapping from primary key to row data.
    data: BTreeMap<Row, Row>,
}

#[derive(Debug, Default)]
pub struct MemRowTable {
    inner: RwLock<MemRowTableInner>,
}

pub type MemRowTableRef = Arc<MemRowTable>;

impl MemRowTableInner {
    pub fn ingest(&mut self, batch: Vec<(Row, Option<Row>)>) -> Result<()> {
        for (key, value) in batch {
            match value {
                Some(value) => self.insert(key, value)?,
                None => self.delete(key)?,
            }
        }
        Ok(())
    }

    pub fn insert(&mut self, key: Row, value: Row) -> Result<()> {
        self.data.insert(key, value);
        Ok(())
    }

    pub fn delete(&mut self, key: Row) -> Result<()> {
        self.data.remove(&key);
        Ok(())
    }

    pub fn get(&self, key: Row) -> Result<Option<Row>> {
        Ok(self.data.get(&key).map(Clone::clone))
    }
}

impl MemRowTable {
    pub fn ingest(&self, batch: Vec<(Row, Option<Row>)>) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        inner.ingest(batch)
    }

    pub fn insert(&self, key: Row, value: Row) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        inner.insert(key, value)
    }

    pub fn delete(&self, key: Row) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        inner.delete(key)
    }

    pub fn get(&self, key: Row) -> Result<Option<Row>> {
        let inner = self.inner.read().unwrap();
        inner.get(key)
    }
}

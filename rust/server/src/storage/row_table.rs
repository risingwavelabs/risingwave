use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::Result;
use risingwave_common::types::{Datum, Scalar};
use std::{
    cmp::Ordering,
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use itertools::Itertools;
use risingwave_proto::plan::ColumnDesc;
use smallvec::SmallVec;

#[derive(Clone, Debug, Default)]
pub struct Row(pub SmallVec<[Datum; 5]>);

pub type MemTableRowIter = <BTreeMap<Row, Row> as IntoIterator>::IntoIter;

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
    fields: Vec<ColumnDesc>,
    inner: RwLock<MemRowTableInner>,
    pks: Vec<usize>,
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

    /// A temporary solution for the case when no key specified.
    /// We set the whole row as key, and the multiplicity as value.
    /// This works for two fn: `insert_one_row`, `delete_one_row`
    pub fn insert_one_row(&mut self, row: Row) -> Result<()> {
        let value = self.data.get(&row);
        match value {
            None => {
                let mut value_vec = SmallVec::new();
                let datum = Some(1.to_scalar_value());
                value_vec.push(datum);
                let value_row = Row(value_vec);
                self.data.insert(row, value_row);
            }
            Some(res) => {
                // Create a new value row of multiplicity+1.
                let mut value_vec = SmallVec::new();
                let datum = res.0.get(0).unwrap().clone();
                let occurrences = datum.unwrap();
                let occ_value = occurrences.as_int32() + 1;
                value_vec.push(Some(occ_value.to_scalar_value()));
                let value_row = Row(value_vec);
                self.data.insert(row, value_row);
            }
        };
        Ok(())
    }

    pub fn delete_one_row(&mut self, row: Row) -> Result<()> {
        let value = self.data.get(&row);
        match value {
            None => Err(InternalError("deleting non-existing row".to_string()).into()),
            Some(res) => {
                let datum = res.0.get(0).unwrap().clone();
                let occurrences = datum.unwrap();
                let occ_value = occurrences.as_int32();
                if *occ_value > 1 {
                    let mut value_vec = SmallVec::new();
                    value_vec.push(Some((occ_value - 1).to_scalar_value()));
                    let value_row = Row(value_vec);
                    self.data.insert(row, value_row);
                } else {
                    self.data.remove(&row);
                }
                Ok(())
            }
        }
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

    /// Return the schema of the table.
    pub fn schema(&self) -> Vec<ColumnDesc> {
        self.fields.clone()
    }

    /// Create an iterator to scan over all records.
    pub fn iter(&self) -> Result<MemTableRowIter> {
        let inner = self.inner.read().unwrap();
        let snapshot = inner.data.clone();
        Ok(snapshot.into_iter())
    }

    /// Init.
    pub(crate) fn new(fields: Vec<ColumnDesc>, pks: Vec<usize>) -> Self {
        let inner = RwLock::new(MemRowTableInner::default());
        Self { fields, inner, pks }
    }
    /// Get all primary key columns.
    pub fn get_pk(&self) -> Vec<usize> {
        self.pks.clone()
    }

    /// A temporary solution for the case when no key specified.
    /// We set the whole row as key, and the multiplicity as value.
    /// This works for three fn in `MemRowTable`: `insert_one_row`, `delete_one_row`.
    pub fn insert_one_row(&mut self, row: Row) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        inner.insert_one_row(row)
    }

    pub fn delete_one_row(&mut self, row: Row) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        inner.delete_one_row(row)
    }

    /// `insert_batch` takes a batch of (row,bool) where bool is an indicator.
    pub fn insert_batch(&self, batch: Vec<(Row, bool)>) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        for (key, value) in batch {
            match value {
                true => inner.insert_one_row(key)?,
                false => inner.delete_one_row(key)?,
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn mock_one_row() -> Row {
        let mut value_vec = SmallVec::new();
        value_vec.push(Some(1.to_scalar_value()));
        value_vec.push(Some(4.to_scalar_value()));
        Row(value_vec)
    }

    #[test]
    fn test_row_table() {
        let mut mem_table = MemRowTable::new(vec![], vec![]);
        // Insert (1,4)
        let row1 = mock_one_row();
        let _res1 = mem_table.insert_one_row(row1);

        // Check (1,4) -> (1)
        let row2 = mock_one_row();
        let get_row1 = mem_table.get(row2);
        if let Ok(res_row_in) = get_row1 {
            let datum = res_row_in.unwrap().0.get(0).unwrap().clone();
            // Dirty trick to assert_eq between (&int32 and integer).
            let d_value = datum.unwrap().as_int32() + 1;
            assert_eq!(d_value, 2);
        } else {
            unreachable!();
        }

        // Insert (1,4)
        let row3 = mock_one_row();
        let _res2 = mem_table.insert_one_row(row3);

        // Check (1,4) -> (2)
        let row4 = mock_one_row();
        let get_row2 = mem_table.get(row4);
        if let Ok(res_row_in) = get_row2 {
            let datum = res_row_in.unwrap().0.get(0).unwrap().clone();
            // Dirty trick to assert_eq between (&int32 and integer).
            let d_value = datum.unwrap().as_int32() + 1;
            assert_eq!(d_value, 3);
        } else {
            unreachable!();
        }

        // Delete (1,4)
        let row5 = mock_one_row();
        let _res3 = mem_table.delete_one_row(row5);

        // Check (1,4) -> (1)
        let row6 = mock_one_row();
        let get_row3 = mem_table.get(row6);
        if let Ok(res_row_in) = get_row3 {
            let datum = res_row_in.unwrap().0.get(0).unwrap().clone();
            // Dirty trick to assert_eq between (&int32 and integer).
            let d_value = datum.unwrap().as_int32() + 1;
            assert_eq!(d_value, 2);
        } else {
            unreachable!();
        }
    }
}

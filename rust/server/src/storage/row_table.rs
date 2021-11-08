use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use risingwave_common::array::data_chunk_iter::Row;
use risingwave_common::catalog::Schema;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::Result;
use risingwave_common::types::Scalar;

pub type MemRowTableRef = Arc<MemRowTable>;
pub type MemTableRowIter = <BTreeMap<Row, Row> as IntoIterator>::IntoIter;

#[derive(Debug, Default)]
struct MemRowTableInner {
    /// data represents a mapping from primary key to row data.
    data: BTreeMap<Row, Row>,
}

#[derive(Debug, Default)]
pub struct MemRowTable {
    schema: Schema,
    inner: RwLock<MemRowTableInner>,
    pks: Vec<usize>,
}

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
                let mut value_vec = vec![];
                let datum = Some(1.to_scalar_value());
                value_vec.push(datum);
                let value_row = Row(value_vec);
                self.data.insert(row, value_row);
            }
            Some(res) => {
                // Create a new value row of multiplicity+1.
                let mut value_vec = vec![];
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
                    let value_row = Row(vec![Some((occ_value - 1).to_scalar_value())]);
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
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Create an iterator to scan over all records.
    pub fn iter(&self) -> Result<MemTableRowIter> {
        let inner = self.inner.read().unwrap();
        let snapshot = inner.data.clone();
        Ok(snapshot.into_iter())
    }

    /// Init.
    pub(crate) fn new(schema: Schema, pks: Vec<usize>) -> Self {
        let inner = RwLock::new(MemRowTableInner::default());
        Self { schema, inner, pks }
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
        Row(vec![Some(1.to_scalar_value()), Some(4.to_scalar_value())])
    }

    #[test]
    fn test_row_table() {
        let mut mem_table = MemRowTable::new(Schema::default(), vec![]);
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

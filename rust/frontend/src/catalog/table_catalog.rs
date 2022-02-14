use std::collections::HashMap;

use risingwave_common::array::RwError;
use risingwave_common::error::Result;
use risingwave_pb::meta::Table;

use crate::catalog::column_catalog::{ColumnCatalog, ColumnDesc};
use crate::catalog::{CatalogError, TableId};

#[derive(Clone)]
pub struct TableCatalog {
    table_id: TableId,
    next_column_id: u64,
    column_by_name: HashMap<String, ColumnCatalog>,
}

pub static ROWID_NAME: &str = "_row_id";

impl TableCatalog {
    pub fn new(table_id: TableId) -> Self {
        Self {
            table_id,
            next_column_id: 0,
            column_by_name: HashMap::new(),
        }
    }

    pub fn add_column(&mut self, col_name: &str, col_desc: ColumnDesc) -> Result<()> {
        let col_catalog = ColumnCatalog::new(self.next_column_id, col_name.to_string(), col_desc);
        self.next_column_id += 1;
        self.column_by_name
            .try_insert(col_name.to_string(), col_catalog)
            .map_err(|_| RwError::from(CatalogError::Duplicated("column", col_name.to_string())))?;
        Ok(())
    }

    /// Used by binder to do column name resolving: column name to column id.
    pub fn get_column_by_name(&self, col_name: &str) -> Option<&ColumnCatalog> {
        self.column_by_name.get(col_name)
    }

    pub fn columns(&self) -> &HashMap<String, ColumnCatalog> {
        &self.column_by_name
    }

    pub fn id(&self) -> TableId {
        self.table_id
    }
}

impl TryFrom<&Table> for TableCatalog {
    type Error = RwError;

    fn try_from(tb: &Table) -> Result<Self> {
        let mut table_catalog = Self::new(tb.get_table_ref_id()?.table_id as u64);
        for col in &tb.column_descs {
            table_catalog.add_column(&col.name, ColumnDesc::new(col.get_column_type()?.into()))?;
        }
        Ok(table_catalog)
    }
}

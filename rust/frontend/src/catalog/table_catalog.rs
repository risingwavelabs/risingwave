use std::collections::HashSet;

use risingwave_common::error::{Result, RwError};
use risingwave_pb::meta::Table;

use crate::catalog::column_catalog::{ColumnCatalog, ColumnDesc};
use crate::catalog::{CatalogError, ColumnId, TableId};

#[derive(Clone)]
pub struct TableCatalog {
    table_id: TableId,
    next_column_id: i32,
    columns: Vec<ColumnCatalog>,
}

pub const ROWID_NAME: &str = "_row_id";

impl TableCatalog {
    pub fn new(table_id: TableId) -> Self {
        Self {
            table_id,
            next_column_id: 0,
            columns: Vec::new(),
        }
    }

    pub fn add_column(&mut self, col_name: &str, col_desc: ColumnDesc) -> Result<()> {
        let col_catalog = ColumnCatalog::new(
            ColumnId::from(self.next_column_id),
            col_name.to_string(),
            col_desc,
        );
        self.next_column_id += 1;
        self.columns.push(col_catalog);
        Ok(())
    }

    pub fn columns(&self) -> &[ColumnCatalog] {
        &self.columns
    }

    pub fn id(&self) -> TableId {
        self.table_id
    }
}

impl TryFrom<&Table> for TableCatalog {
    type Error = RwError;

    fn try_from(tb: &Table) -> Result<Self> {
        let mut table_catalog = Self::new(TableId::from(&tb.table_ref_id));
        let mut names = HashSet::new();
        for col in &tb.column_descs {
            if !names.insert(col.name.clone()) {
                return Err(CatalogError::Duplicated("column", col.name.clone()).into());
            }
            table_catalog.add_column(&col.name, ColumnDesc::new(col.get_column_type()?.into()))?;
        }
        Ok(table_catalog)
    }
}

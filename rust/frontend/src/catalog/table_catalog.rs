use std::sync::atomic::{AtomicU64, Ordering};

use risingwave_common::array::RwError;
use risingwave_common::error::Result;
use risingwave_pb::meta::Table;

use crate::catalog::column_catalog::{ColumnCatalog, ColumnDesc};
use crate::catalog::{ColumnId, TableId};

pub struct TableCatalog {
    table_id: TableId,
    next_column_id: AtomicU64,
    column_by_name: Vec<(String, ColumnCatalog)>,
    primary_keys: Vec<ColumnId>,
}

impl TableCatalog {
    pub fn new(table_id: TableId) -> Self {
        Self {
            table_id,
            next_column_id: AtomicU64::new(0),
            column_by_name: vec![],
            primary_keys: vec![],
        }
    }

    pub fn add_column(&mut self, col_name: &str, col_desc: ColumnDesc) -> Result<()> {
        let col_catalog = ColumnCatalog::new(
            self.next_column_id.fetch_add(1, Ordering::Relaxed),
            col_name.to_string(),
            col_desc.clone(),
        );
        if col_desc.is_primary() {
            self.primary_keys.push(col_catalog.id());
        }
        self.column_by_name
            .push((col_name.to_string(), col_catalog));
        Ok(())
    }

    pub fn get_column_by_id(&self, col_id: ColumnId) -> Option<&ColumnCatalog> {
        self.column_by_name
            .get(col_id as usize)
            .map(|(_, col_catalog)| col_catalog)
    }

    pub fn columns(&self) -> &[(String, ColumnCatalog)] {
        self.column_by_name.as_slice()
    }

    pub fn id(&self) -> TableId {
        self.table_id
    }

    pub fn get_pks(&self) -> Vec<u64> {
        self.primary_keys.clone()
    }
}

impl TryFrom<&Table> for TableCatalog {
    type Error = RwError;

    fn try_from(tb: &Table) -> Result<Self> {
        let mut table_catalog = Self::new(tb.get_table_ref_id()?.table_id as u64);
        for col in &tb.column_descs {
            table_catalog.add_column(
                &col.name,
                ColumnDesc::new(col.get_column_type()?.into(), col.is_primary),
            )?;
        }
        Ok(table_catalog)
    }
}

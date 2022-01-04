use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};

use risingwave_common::error::Result;

use crate::catalog::column_catalog::{ColumnCatalog, ColumnDesc};
use crate::catalog::{CatalogError, ColumnId};

pub struct TableCatalog {
    next_column_id: AtomicU32,
    column_by_name: HashMap<String, ColumnCatalog>,
    primary_keys: Vec<ColumnId>,
}

impl TableCatalog {
    pub fn new() -> Self {
        Self {
            next_column_id: AtomicU32::new(0),
            column_by_name: HashMap::new(),
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
            .try_insert(col_name.to_string(), col_catalog)
            .map(|_val| ())
            .map_err(|_| CatalogError::Duplicated("column", col_name.to_string()).into())
    }
    pub fn get_column_by_name(&self, col_name: &str) -> Option<&ColumnCatalog> {
        self.column_by_name.get(col_name)
    }
}

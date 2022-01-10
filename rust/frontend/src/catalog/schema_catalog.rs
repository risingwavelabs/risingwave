use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};

use risingwave_common::error::Result;

use crate::catalog::create_table_info::CreateTableInfo;
use crate::catalog::table_catalog::TableCatalog;
use crate::catalog::{CatalogError, SchemaId, TableId};

pub struct SchemaCatalog {
    schema_id: SchemaId,
    next_table_id: AtomicU32,
    table_by_name: HashMap<String, TableCatalog>,
}

impl SchemaCatalog {
    pub fn new(schema_id: SchemaId) -> Self {
        Self {
            schema_id,
            next_table_id: AtomicU32::new(0),
            table_by_name: HashMap::new(),
        }
    }
    pub fn create_table_local(&mut self, info: &CreateTableInfo) -> Result<()> {
        self.create_table_with_id(info, self.next_table_id.fetch_add(1, Ordering::Relaxed))
    }

    pub fn create_table_with_id(
        &mut self,
        info: &CreateTableInfo,
        table_id: TableId,
    ) -> Result<()> {
        let table_name = info.get_name().to_string();

        // Wrap info into table catalog.
        let mut table_catalog = TableCatalog::new(table_id);
        let columns = info.get_columns();
        for (col_name, col_desc) in columns {
            table_catalog
                .add_column(col_name, col_desc.clone())
                .unwrap();
        }

        self.table_by_name
            .try_insert(table_name.clone(), table_catalog)
            .map(|_val| ())
            .map_err(|_| CatalogError::Duplicated("table", table_name).into())
    }

    pub fn get_table(&self, table_name: &str) -> Option<&TableCatalog> {
        self.table_by_name.get(table_name)
    }

    pub fn id(&self) -> SchemaId {
        self.schema_id
    }
}

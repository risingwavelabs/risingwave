use std::collections::HashMap;

use risingwave_common::array::RwError;
use risingwave_common::error::Result;
use risingwave_pb::meta::Table;

use crate::catalog::table_catalog::TableCatalog;
use crate::catalog::{CatalogError, SchemaId};

pub struct SchemaCatalog {
    schema_id: SchemaId,
    table_by_name: HashMap<String, TableCatalog>,
}

impl SchemaCatalog {
    pub fn new(schema_id: SchemaId) -> Self {
        Self {
            schema_id,
            table_by_name: HashMap::new(),
        }
    }
    pub fn create_table(&mut self, table: &Table) -> Result<()> {
        let table_name = &table.table_name;
        let table_catalog = table.try_into()?;

        self.table_by_name
            .try_insert(table_name.clone(), table_catalog)
            .map(|_val| ())
            .map_err(|_| CatalogError::Duplicated("table", table_name.clone()).into())
    }

    pub fn drop_table(&mut self, table_name: &str) -> Result<()> {
        self.table_by_name.remove(table_name).ok_or_else(|| {
            RwError::from(CatalogError::NotFound("table", table_name.to_string()))
        })?;
        Ok(())
    }

    pub fn get_table(&self, table_name: &str) -> Option<&TableCatalog> {
        self.table_by_name.get(table_name)
    }

    pub fn id(&self) -> SchemaId {
        self.schema_id
    }
}

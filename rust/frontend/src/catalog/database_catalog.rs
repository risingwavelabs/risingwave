use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};

use risingwave_common::error::Result;

use crate::catalog::schema_catalog::SchemaCatalog;
use crate::catalog::{CatalogError, DatabaseId};

pub struct DatabaseCatalog {
    id: DatabaseId,
    next_schema_id: AtomicU32,
    schema_by_name: HashMap<String, SchemaCatalog>,
}

impl DatabaseCatalog {
    pub fn new(id: DatabaseId) -> Self {
        Self {
            id,
            next_schema_id: AtomicU32::new(0),
            schema_by_name: HashMap::new(),
        }
    }
    pub fn create_schema(&mut self, schema_name: &str) -> Result<()> {
        self.schema_by_name
            .try_insert(
                schema_name.to_string(),
                SchemaCatalog::new(self.next_schema_id.fetch_add(1, Ordering::Relaxed)),
            )
            .map(|_val| ())
            .map_err(|_| CatalogError::Duplicated("table", schema_name.to_string()).into())
    }

    pub fn get_schema(&self, schema: &str) -> Option<&SchemaCatalog> {
        self.schema_by_name.get(schema)
    }

    pub fn get_schema_mut(&mut self, schema: &str) -> Option<&mut SchemaCatalog> {
        self.schema_by_name.get_mut(schema)
    }

    pub fn id(&self) -> u32 {
        self.id
    }
}

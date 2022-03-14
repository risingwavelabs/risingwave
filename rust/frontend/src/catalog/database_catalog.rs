use std::collections::HashMap;

use risingwave_common::error::{Result, RwError};

use crate::catalog::schema_catalog::SchemaCatalog;
use crate::catalog::{CatalogError, DatabaseId, SchemaId};

#[derive(Clone)]
pub struct DatabaseCatalog {
    id: DatabaseId,
    schema_by_name: HashMap<String, SchemaCatalog>,
    schema_name_by_id: HashMap<SchemaId, String>,
}

impl DatabaseCatalog {
    pub fn new(id: DatabaseId) -> Self {
        Self {
            id,
            schema_by_name: HashMap::new(),
            schema_name_by_id: HashMap::new(),
        }
    }

    pub fn create_schema_with_id(&mut self, schema_name: &str, schema_id: SchemaId) {
        self.schema_by_name
            .try_insert(
                schema_name.to_string(),
                SchemaCatalog::new(schema_id, schema_name.to_string()),
            )
            .unwrap();
        self.schema_name_by_id
            .try_insert(schema_id, schema_name.to_string())
            .unwrap();
    }

    pub fn drop_schema(&mut self, schema_id: SchemaId) {
        let name = self.schema_name_by_id.remove(&schema_id).unwrap();
        self.schema_by_name.remove(&name).unwrap();
    }

    pub fn get_schema_by_name(&self, name: &str) -> Option<&SchemaCatalog> {
        self.schema_by_name.get(name)
    }

    pub fn get_schema_mut(&mut self, schema_id: SchemaId) -> Option<&mut SchemaCatalog> {
        let name = self.schema_name_by_id.get(&schema_id).unwrap();
        self.schema_by_name.get_mut(name)
    }

    pub fn id(&self) -> u64 {
        self.id
    }
}

use std::collections::HashMap;

use risingwave_common::array::RwError;
use risingwave_common::error::Result;

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

    pub fn create_schema_with_id(&mut self, schema_name: &str, schema_id: SchemaId) -> Result<()> {
        self.schema_by_name
            .try_insert(schema_name.to_string(), SchemaCatalog::new(schema_id))
            .map(|_val| ())
            .map_err(|_| CatalogError::Duplicated("schema", schema_name.to_string()))?;
        self.schema_name_by_id
            .try_insert(schema_id, schema_name.to_string())
            .map(|_val| ())
            .map_err(|_| CatalogError::Duplicated("schema id", schema_id.to_string()).into())
    }

    pub fn drop_schema(&mut self, schema_name: &str) -> Result<()> {
        let schema = self.schema_by_name.remove(schema_name).ok_or_else(|| {
            RwError::from(CatalogError::NotFound("schema", schema_name.to_string()))
        })?;
        self.schema_name_by_id.remove(&schema.id()).ok_or_else(|| {
            RwError::from(CatalogError::NotFound("schema id", schema.id().to_string()))
        })?;
        Ok(())
    }

    pub fn get_schema(&self, schema: &str) -> Option<&SchemaCatalog> {
        self.schema_by_name.get(schema)
    }

    pub fn get_schema_mut(&mut self, schema: &str) -> Option<&mut SchemaCatalog> {
        self.schema_by_name.get_mut(schema)
    }

    pub fn get_schema_name(&self, schema_id: SchemaId) -> Option<String> {
        self.schema_name_by_id.get(&schema_id).cloned()
    }

    pub fn id(&self) -> u64 {
        self.id
    }
}

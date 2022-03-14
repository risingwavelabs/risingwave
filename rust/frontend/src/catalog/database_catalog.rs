use std::collections::HashMap;

use risingwave_common::error::{Result, RwError};
use risingwave_pb::catalog::{Database as ProstDatabase, Schema as ProstSchema};

use crate::catalog::schema_catalog::SchemaCatalog;
use crate::catalog::{CatalogError, DatabaseId, SchemaId};
#[derive(Clone, Debug)]
pub struct DatabaseCatalog {
    id: DatabaseId,
    name: String,
    schema_by_name: HashMap<String, SchemaCatalog>,
    schema_name_by_id: HashMap<SchemaId, String>,
}

impl DatabaseCatalog {
    pub fn create_schema(&mut self, proto: &ProstSchema) {
        let name = proto.name;
        let id = proto.id;
        let schema = proto.into();
        self.schema_by_name.try_insert(name, schema).unwrap();
        self.schema_name_by_id.try_insert(id.into(), name).unwrap();
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

    pub fn id(&self) -> DatabaseId {
        self.id
    }
}
impl From<&ProstDatabase> for DatabaseCatalog {
    fn from(db: &ProstDatabase) -> Self {
        Self {
            id: db.id.into(),
            name: db.name,
            schema_by_name: HashMap::new(),
            schema_name_by_id: HashMap::new(),
        }
    }
}

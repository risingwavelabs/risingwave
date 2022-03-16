use std::collections::HashMap;

use risingwave_common::catalog::TableId;
use risingwave_common::error::{Result, RwError};
use risingwave_meta::manager::SourceId;
use risingwave_pb::catalog::{Schema as ProstSchema, Source as ProstSource, Table as ProstTable};

use crate::catalog::table_catalog::TableCatalog;
use crate::catalog::{CatalogError, SchemaId};

#[derive(Clone, Debug)]
pub struct SchemaCatalog {
    id: SchemaId,
    name: String,
    table_by_name: HashMap<String, TableCatalog>,
    table_name_by_id: HashMap<TableId, String>,
    source_by_name: HashMap<String, ProstSource>,
    source_name_by_id: HashMap<SourceId, String>,
}

impl SchemaCatalog {
    pub fn create_table(&mut self, prost: &ProstTable) {
        let name = prost.name;
        let id = prost.id.into();
        let table = prost.into();

        self.table_by_name.try_insert(name, table).unwrap();
        self.table_name_by_id.try_insert(id, name).unwrap();
    }

    pub fn drop_table(&mut self, id: TableId) {
        let name = self.table_name_by_id.remove(&id).unwrap();
        self.table_by_name.remove(&name).unwrap();
    }
    pub fn create_source(&mut self, prost: ProstSource) {
        let name = prost.name;
        let id = prost.id.into();

        self.source_by_name.try_insert(name, prost).unwrap();
        self.table_name_by_id.try_insert(id, name).unwrap();
    }
    pub fn drop_source(&mut self, id: SourceId) {
        let name = self.source_name_by_id.remove(&id).unwrap();
        self.source_by_name.remove(&name).unwrap();
    }

    pub fn get_table_by_name(&self, table_name: &str) -> Option<&TableCatalog> {
        self.table_by_name.get(table_name)
    }

    pub fn id(&self) -> SchemaId {
        self.id
    }
}

impl From<&ProstSchema> for SchemaCatalog {
    fn from(schema: &ProstSchema) -> Self {
        Self {
            id: schema.id.into(),
            name: schema.name,
            table_by_name: HashMap::new(),
            table_name_by_id: HashMap::new(),
            source_by_name: HashMap::new(),
            source_name_by_id: HashMap::new(),
        }
    }
}

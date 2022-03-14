use std::collections::HashMap;

use risingwave_common::catalog::TableId;
use risingwave_common::error::{Result, RwError};
use risingwave_pb::catalog::Table as ProstTable;

use crate::catalog::table_catalog::TableCatalog;
use crate::catalog::{CatalogError, SchemaId};

#[derive(Clone, Debug)]
pub struct SchemaCatalog {
    schema_id: SchemaId,
    name: String,
    table_by_name: HashMap<String, TableCatalog>,
    table_name_by_id: HashMap<TableId, String>,
}

impl SchemaCatalog {
    pub fn new(schema_id: SchemaId, name: String) -> Self {
        Self {
            schema_id,
            name,
            table_by_name: HashMap::new(),
            table_name_by_id: HashMap::new(),
        }
    }
    pub fn add_table(&mut self, prost: &ProstTable) {
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

    pub fn get_table_by_name(&self, table_name: &str) -> Option<&TableCatalog> {
        self.table_by_name.get(table_name)
    }

    pub fn id(&self) -> SchemaId {
        self.schema_id
    }
}

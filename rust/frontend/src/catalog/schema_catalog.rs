use std::collections::HashMap;

use risingwave_common::catalog::TableId;
use risingwave_common::error::{Result, RwError};
use risingwave_pb::catalog::Table as ProstTable;

use crate::catalog::table_catalog::TableCatalog;
use crate::catalog::{CatalogError, SchemaId};

#[derive(Clone, Debug)]
pub struct SchemaCatalog {
    schema_id: SchemaId,
    tables: HashMap<TableId, TableCatalog>,
    table_name_to_id: HashMap<String, TableId>,
}

impl SchemaCatalog {
    pub fn new(schema_id: SchemaId) -> Self {
        Self {
            schema_id,
            tables: HashMap::new(),
            table_name_to_id: HashMap::new(),
        }
    }
    pub fn add_table(&mut self, prost: &ProstTable) {
        let name = prost.name;
        let id = prost.id.into();
        let table = prost.into();

        self.tables.try_insert(id, table).unwrap();
        self.table_name_to_id.try_insert(name, id).unwrap();
    }

    pub fn drop_table(&mut self, id: TableId) {
        let table = self.tables.remove(&id).unwrap();
        self.table_name_to_id.remove(table.name()).unwrap();
    }

    pub fn get_table_by_name(&self, table_name: &str) -> Option<&TableCatalog> {
        let id = self.table_name_to_id.get(table_name)?;
        self.tables.get(id)
    }

    pub fn id(&self) -> SchemaId {
        self.schema_id
    }
}

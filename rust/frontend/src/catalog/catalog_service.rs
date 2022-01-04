use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};

use risingwave_common::error::Result;

use crate::catalog::create_table_info::CreateTableInfo;
use crate::catalog::database_catalog::DatabaseCatalog;
use crate::catalog::schema_catalog::SchemaCatalog;
use crate::catalog::table_catalog::TableCatalog;
use crate::catalog::CatalogError;

const DEFAULT_DATABASE_NAME: &str = "dev";
const DEFAULT_SCHEMA_NAME: &str = "dev";

struct LocalCatalogManager {
    next_database_id: AtomicU32,
    database_by_name: HashMap<String, DatabaseCatalog>,
}

impl LocalCatalogManager {
    pub fn new() -> Self {
        Self {
            next_database_id: AtomicU32::new(0),
            database_by_name: HashMap::new(),
        }
    }

    fn create_database(&mut self, db_name: &str) -> Result<()> {
        self.database_by_name
            .try_insert(
                db_name.to_string(),
                DatabaseCatalog::new(self.next_database_id.fetch_add(1, Ordering::Relaxed)),
            )
            .map(|_| ())
            .map_err(|_| CatalogError::Duplicated("database", db_name.to_string()).into())
    }

    fn get_database(&self, db_name: &str) -> Option<&DatabaseCatalog> {
        self.database_by_name.get(db_name)
    }

    fn get_database_mut(&mut self, db_name: &str) -> Option<&mut DatabaseCatalog> {
        self.database_by_name.get_mut(db_name)
    }

    fn create_schema(&mut self, db_name: &str, schema_name: &str) -> Result<()> {
        self.get_database_mut(db_name).map_or(
            Err(CatalogError::NotFound("schema", db_name.to_string()).into()),
            |db| db.create_schema(schema_name),
        )
    }

    fn get_schema(&self, db_name: &str, schema_name: &str) -> Option<&SchemaCatalog> {
        self.get_database(db_name)
            .and_then(|db| db.get_schema(schema_name))
    }

    fn get_schema_mut(&mut self, db_name: &str, schema_name: &str) -> Option<&mut SchemaCatalog> {
        self.get_database_mut(db_name)
            .and_then(|db| db.get_schema_mut(schema_name))
    }

    fn create_table(
        &mut self,
        db_name: &str,
        schema_name: &str,
        info: CreateTableInfo,
    ) -> Result<()> {
        self.get_schema_mut(db_name, schema_name).map_or(
            Err(CatalogError::NotFound("schema", schema_name.to_string()).into()),
            |schema| schema.create_table(info),
        )
    }

    fn get_table(
        &self,
        db_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Option<&TableCatalog> {
        self.get_schema(db_name, schema_name)
            .and_then(|schema| schema.get_table(table_name))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_common::types::{DataTypeKind, Int32Type};

    use crate::catalog::catalog_service::{
        LocalCatalogManager, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME,
    };
    use crate::catalog::column_catalog::ColumnDesc;
    use crate::catalog::create_table_info::CreateTableInfo;

    #[test]
    fn test_create_table() {
        let mut catalog_manager = LocalCatalogManager::new();
        catalog_manager
            .create_database(DEFAULT_DATABASE_NAME)
            .unwrap();
        let db = catalog_manager.get_database(DEFAULT_DATABASE_NAME).unwrap();
        assert_eq!(db.id(), 0);
        catalog_manager
            .create_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME)
            .unwrap();
        let schema = catalog_manager
            .get_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME)
            .unwrap();
        assert_eq!(schema.id(), 0);
        let columns = vec![
            (
                "v1".to_string(),
                ColumnDesc::new(Arc::new(Int32Type::new(true)), false),
            ),
            (
                "v2".to_string(),
                ColumnDesc::new(Arc::new(Int32Type::new(false)), false),
            ),
        ];
        catalog_manager
            .create_table(
                DEFAULT_DATABASE_NAME,
                DEFAULT_SCHEMA_NAME,
                CreateTableInfo::new("t", columns),
            )
            .unwrap();
        let table = catalog_manager
            .get_table(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "t")
            .unwrap();
        let col2 = table.get_column_by_name("v2").unwrap();
        let col1 = table.get_column_by_name("v1").unwrap();
        assert!(col1.is_nullable());
        assert_eq!(col1.id(), 0);
        assert_eq!(col1.datatype_clone().data_type_kind(), DataTypeKind::Int32);
        assert_eq!(col2.name(), "v2");
        assert_eq!(col2.id(), 1);
    }
}

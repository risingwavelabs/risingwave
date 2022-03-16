use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::lock_api::ArcRwLockReadGuard;
use parking_lot::{RawRwLock, RwLock};
use risingwave_common::catalog::{CatalogVersion, TableId};
use risingwave_meta::manager::SourceId;
use risingwave_pb::catalog::{
    Database as ProstDatabase, Schema as ProstSchema, Source as ProstSource, Table as ProstTable,
};

use crate::catalog::database_catalog::DatabaseCatalog;
use crate::catalog::schema_catalog::SchemaCatalog;
use crate::catalog::table_catalog::TableCatalog;
use crate::catalog::{DatabaseId, SchemaId};
pub type CatalogReadGuard = ArcRwLockReadGuard<RawRwLock, Catalog>;

#[derive(Clone)]
pub struct CatalogReader(pub Arc<RwLock<Catalog>>);
impl CatalogReader {
    fn read_guard(&self) -> CatalogReadGuard {
        self.0.read_arc()
    }
}

/// Root catalog of database catalog. Manage all database/schema/table in memory on frontend. it
/// is protected by a RwLock. only [`ObserverManager`] will get its mut reference and do write to
/// sync with the meta catalog. Other situations it is read only with a read guard.
///
/// - catalog (root catalog)
///   - database catalog
///     - schema catalog
///       - table catalog
///        - column catalog
pub struct Catalog {
    version: CatalogVersion,
    database_by_name: HashMap<String, DatabaseCatalog>,
    db_name_by_id: HashMap<DatabaseId, String>,
}

impl Default for Catalog {
    fn default() -> Self {
        Self {
            version: 0,
            database_by_name: HashMap::new(),
            db_name_by_id: HashMap::new(),
        }
    }
}

impl Catalog {
    fn get_database_mut(&mut self, db_id: DatabaseId) -> Option<&mut DatabaseCatalog> {
        let name = self.db_name_by_id.get(&db_id)?;
        self.database_by_name.get_mut(name)
    }

    pub fn create_database(&mut self, db: &ProstDatabase) {
        let name = db.name;
        let id = db.id.into();

        self.database_by_name.try_insert(name, db.into()).unwrap();
        self.db_name_by_id.try_insert(id, name).unwrap();
    }

    pub fn create_schema(&mut self, proto: &ProstSchema) {
        self.get_database_mut(proto.database_id)
            .unwrap()
            .create_schema(proto);
    }

    pub fn create_table(&mut self, proto: &ProstTable) {
        self.get_database_mut(proto.database_id)
            .unwrap()
            .get_schema_mut(proto.schema_id)
            .unwrap()
            .create_table(proto);
    }
    pub fn create_source(&mut self, proto: ProstSource) {
        self.get_database_mut(proto.database_id)
            .unwrap()
            .get_schema_mut(proto.schema_id)
            .unwrap()
            .create_source(proto);
    }

    pub fn drop_database(&mut self, db_id: DatabaseId) {
        let name = self.db_name_by_id.remove(&db_id).unwrap();
        let database = self.database_by_name.remove(&name).unwrap();
    }

    pub fn drop_schema(&mut self, db_id: DatabaseId, schema_id: SchemaId) {
        self.get_database_mut(db_id).unwrap().drop_schema(schema_id);
    }

    pub fn drop_table(&mut self, db_id: DatabaseId, schema_id: SchemaId, tb_id: TableId) {
        self.get_database_mut(db_id)
            .unwrap()
            .get_schema_mut(schema_id)
            .unwrap()
            .drop_table(tb_id);
    }

    pub fn drop_source(&mut self, db_id: DatabaseId, schema_id: SchemaId, source_id: SourceId) {
        self.get_database_mut(db_id)
            .unwrap()
            .get_schema_mut(schema_id)
            .unwrap()
            .drop_source(source_id);
    }

    pub fn get_database_by_name(&self, db_name: &str) -> Option<&DatabaseCatalog> {
        self.database_by_name.get(db_name)
    }

    pub fn get_schema_by_name(&self, db_name: &str, schema_name: &str) -> Option<&SchemaCatalog> {
        self.get_database_by_name(db_name)?
            .get_schema_by_name(schema_name)
    }

    pub fn get_table_by_name(
        &self,
        db_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Option<&TableCatalog> {
        self.get_schema_by_name(db_name, schema_name)?
            .get_table_by_name(table_name)
    }

    /// Get the catalog cache's catalog version.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Set the catalog cache's catalog version.
    pub fn set_version(&mut self, catalog_version: CatalogVersion) {
        self.version = catalog_version;
    }
}

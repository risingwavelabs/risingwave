// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
use std::collections::HashMap;

use risingwave_common::catalog::{CatalogVersion, TableId};
use risingwave_common::error::Result;
use risingwave_meta::manager::SourceId;
use risingwave_pb::catalog::{
    source, Database as ProstDatabase, Schema as ProstSchema, Source as ProstSource,
    Table as ProstTable,
};

use super::CatalogError;
use crate::catalog::database_catalog::DatabaseCatalog;
use crate::catalog::schema_catalog::SchemaCatalog;
use crate::catalog::table_catalog::TableCatalog;
use crate::catalog::{DatabaseId, SchemaId};

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

#[allow(clippy::derivable_impls)]
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

    pub fn create_database(&mut self, db: ProstDatabase) {
        let name = db.name.clone();
        let id = db.id;

        self.database_by_name
            .try_insert(name.clone(), (&db).into())
            .unwrap();
        self.db_name_by_id.try_insert(id, name).unwrap();
    }

    pub fn create_schema(&mut self, proto: ProstSchema) {
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
        let _database = self.database_by_name.remove(&name).unwrap();
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

    /// check the name if duplicated with existing table, materialized view or source;
    pub fn check_relation_name(
        &self,
        db_name: &str,
        schema_name: &str,
        relation_name: &str,
    ) -> Result<(DatabaseId, SchemaId)> {
        let db = self
            .get_database_by_name(db_name)
            .ok_or_else(|| CatalogError::NotFound("database", db_name.to_string()))?;
        let schema = db
            .get_schema_by_name(schema_name)
            .ok_or_else(|| CatalogError::NotFound("schema", schema_name.to_string()))?;
        if let Some(source) = schema.get_source_by_name(relation_name) {
            // TODO: check if it is a materialized source and improve the err msg
            return match source.info {
                Some(source::Info::TableSource(_)) => {
                    Err(CatalogError::Duplicated("table", relation_name.to_string()).into())
                }
                Some(source::Info::StreamSource(_)) => {
                    Err(CatalogError::Duplicated("source", relation_name.to_string()).into())
                }
                None => unreachable!(),
            };
        }

        if let Some(_table) = schema.get_table_by_name(relation_name) {
            return Err(
                CatalogError::Duplicated("materialized view", schema_name.to_string()).into(),
            );
        }
        Ok((db.id(), schema.id()))
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

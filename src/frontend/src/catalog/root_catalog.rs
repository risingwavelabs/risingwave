// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::catalog::{CatalogVersion, FunctionId, IndexId, TableId};
use risingwave_common::session_config::{SearchPath, USER_NAME_WILD_CARD};
use risingwave_common::types::DataType;
use risingwave_connector::sink::catalog::SinkCatalog;
use risingwave_pb::catalog::{
    Database as ProstDatabase, Function as ProstFunction, Index as ProstIndex,
    Schema as ProstSchema, Sink as ProstSink, Source as ProstSource, Table as ProstTable,
    View as ProstView,
};

use super::function_catalog::FunctionCatalog;
use super::source_catalog::SourceCatalog;
use super::system_catalog::get_sys_catalogs_in_schema;
use super::view_catalog::ViewCatalog;
use super::{CatalogError, CatalogResult, SinkId, SourceId, ViewId};
use crate::catalog::database_catalog::DatabaseCatalog;
use crate::catalog::schema_catalog::SchemaCatalog;
use crate::catalog::system_catalog::SystemCatalog;
use crate::catalog::table_catalog::TableCatalog;
use crate::catalog::{DatabaseId, IndexCatalog, SchemaId};

#[derive(Copy, Clone)]
pub enum SchemaPath<'a> {
    Name(&'a str),
    /// (search_path, user_name).
    Path(&'a SearchPath, &'a str),
}

impl<'a> SchemaPath<'a> {
    pub fn new(
        schema_name: Option<&'a str>,
        search_path: &'a SearchPath,
        user_name: &'a str,
    ) -> Self {
        match schema_name {
            Some(schema_name) => SchemaPath::Name(schema_name),
            None => SchemaPath::Path(search_path, user_name),
        }
    }

    /// Call function `f` for each schema name. Return the first `Some` result.
    pub fn try_find<T, E>(
        &self,
        mut f: impl FnMut(&str) -> Result<Option<T>, E>,
    ) -> Result<Option<(T, &'a str)>, E> {
        match self {
            SchemaPath::Name(schema_name) => Ok(f(schema_name)?.map(|t| (t, *schema_name))),
            SchemaPath::Path(search_path, user_name) => {
                for schema_name in search_path.path() {
                    let mut schema_name: &str = schema_name;
                    if schema_name == USER_NAME_WILD_CARD {
                        schema_name = user_name;
                    }
                    if let Ok(Some(res)) = f(schema_name) {
                        return Ok(Some((res, schema_name)));
                    }
                }
                Ok(None)
            }
        }
    }
}

/// Root catalog of database catalog. It manages all database/schema/table in memory on frontend.
/// It is protected by a `RwLock`. Only [`crate::observer::FrontendObserverNode`]
/// will acquire the write lock and sync it with the meta catalog. In other situations, it is
/// read only.
///
/// - catalog (root catalog)
///   - database catalog
///     - schema catalog
///       - function catalog
///       - table/sink/source/index/view catalog
///        - column catalog
pub struct Catalog {
    version: CatalogVersion,
    database_by_name: HashMap<String, DatabaseCatalog>,
    db_name_by_id: HashMap<DatabaseId, String>,
    /// all table catalogs in the cluster identified by universal unique table id.
    table_by_id: HashMap<TableId, TableCatalog>,
}

#[expect(clippy::derivable_impls)]
impl Default for Catalog {
    fn default() -> Self {
        Self {
            version: 0,
            database_by_name: HashMap::new(),
            db_name_by_id: HashMap::new(),
            table_by_id: HashMap::new(),
        }
    }
}

impl Catalog {
    fn get_database_mut(&mut self, db_id: DatabaseId) -> Option<&mut DatabaseCatalog> {
        let name = self.db_name_by_id.get(&db_id)?;
        self.database_by_name.get_mut(name)
    }

    pub fn clear(&mut self) {
        self.database_by_name.clear();
        self.db_name_by_id.clear();
        self.table_by_id.clear();
    }

    pub fn create_database(&mut self, db: &ProstDatabase) {
        let name = db.name.clone();
        let id = db.id;

        self.database_by_name
            .try_insert(name.clone(), db.into())
            .unwrap();
        self.db_name_by_id.try_insert(id, name).unwrap();
    }

    pub fn create_schema(&mut self, proto: &ProstSchema) {
        self.get_database_mut(proto.database_id)
            .unwrap()
            .create_schema(proto);

        if let Some(sys_tables) = get_sys_catalogs_in_schema(proto.name.as_str()) {
            sys_tables.into_iter().for_each(|sys_table| {
                self.get_database_mut(proto.database_id)
                    .unwrap()
                    .get_schema_mut(proto.id)
                    .unwrap()
                    .create_sys_table(sys_table);
            });
        }
    }

    pub fn create_table(&mut self, proto: &ProstTable) {
        self.table_by_id.insert(proto.id.into(), proto.into());
        self.get_database_mut(proto.database_id)
            .unwrap()
            .get_schema_mut(proto.schema_id)
            .unwrap()
            .create_table(proto);
    }

    pub fn create_index(&mut self, proto: &ProstIndex) {
        self.get_database_mut(proto.database_id)
            .unwrap()
            .get_schema_mut(proto.schema_id)
            .unwrap()
            .create_index(proto);
    }

    pub fn create_source(&mut self, proto: &ProstSource) {
        self.get_database_mut(proto.database_id)
            .unwrap()
            .get_schema_mut(proto.schema_id)
            .unwrap()
            .create_source(proto);
    }

    pub fn create_sink(&mut self, proto: &ProstSink) {
        self.get_database_mut(proto.database_id)
            .unwrap()
            .get_schema_mut(proto.schema_id)
            .unwrap()
            .create_sink(proto);
    }

    pub fn create_view(&mut self, proto: &ProstView) {
        self.get_database_mut(proto.database_id)
            .unwrap()
            .get_schema_mut(proto.schema_id)
            .unwrap()
            .create_view(proto);
    }

    pub fn create_function(&mut self, proto: &ProstFunction) {
        self.get_database_mut(proto.database_id)
            .unwrap()
            .get_schema_mut(proto.schema_id)
            .unwrap()
            .create_function(proto);
    }

    pub fn drop_database(&mut self, db_id: DatabaseId) {
        let name = self.db_name_by_id.remove(&db_id).unwrap();
        let _database = self.database_by_name.remove(&name).unwrap();
    }

    pub fn drop_schema(&mut self, db_id: DatabaseId, schema_id: SchemaId) {
        self.get_database_mut(db_id).unwrap().drop_schema(schema_id);
    }

    pub fn drop_table(&mut self, db_id: DatabaseId, schema_id: SchemaId, tb_id: TableId) {
        self.table_by_id.remove(&tb_id);
        self.get_database_mut(db_id)
            .unwrap()
            .get_schema_mut(schema_id)
            .unwrap()
            .drop_table(tb_id);
    }

    pub fn update_table(&mut self, proto: &ProstTable) {
        self.table_by_id.insert(proto.id.into(), proto.into());
        self.get_database_mut(proto.database_id)
            .unwrap()
            .get_schema_mut(proto.schema_id)
            .unwrap()
            .update_table(proto);
    }

    pub fn drop_source(&mut self, db_id: DatabaseId, schema_id: SchemaId, source_id: SourceId) {
        self.get_database_mut(db_id)
            .unwrap()
            .get_schema_mut(schema_id)
            .unwrap()
            .drop_source(source_id);
    }

    pub fn drop_sink(&mut self, db_id: DatabaseId, schema_id: SchemaId, sink_id: SinkId) {
        self.get_database_mut(db_id)
            .unwrap()
            .get_schema_mut(schema_id)
            .unwrap()
            .drop_sink(sink_id);
    }

    pub fn update_sink(&mut self, proto: &ProstSink) {
        self.get_database_mut(proto.database_id)
            .unwrap()
            .get_schema_mut(proto.schema_id)
            .unwrap()
            .update_sink(proto);
    }

    pub fn drop_index(&mut self, db_id: DatabaseId, schema_id: SchemaId, index_id: IndexId) {
        self.get_database_mut(db_id)
            .unwrap()
            .get_schema_mut(schema_id)
            .unwrap()
            .drop_index(index_id);
    }

    pub fn drop_view(&mut self, db_id: DatabaseId, schema_id: SchemaId, view_id: ViewId) {
        self.get_database_mut(db_id)
            .unwrap()
            .get_schema_mut(schema_id)
            .unwrap()
            .drop_view(view_id);
    }

    pub fn update_view(&mut self, proto: &ProstView) {
        self.get_database_mut(proto.database_id)
            .unwrap()
            .get_schema_mut(proto.schema_id)
            .unwrap()
            .update_view(proto);
    }

    pub fn drop_function(
        &mut self,
        db_id: DatabaseId,
        schema_id: SchemaId,
        function_id: FunctionId,
    ) {
        self.get_database_mut(db_id)
            .unwrap()
            .get_schema_mut(schema_id)
            .unwrap()
            .drop_function(function_id);
    }

    pub fn get_database_by_name(&self, db_name: &str) -> CatalogResult<&DatabaseCatalog> {
        self.database_by_name
            .get(db_name)
            .ok_or_else(|| CatalogError::NotFound("database", db_name.to_string()))
    }

    pub fn get_database_by_id(&self, db_id: &DatabaseId) -> CatalogResult<&DatabaseCatalog> {
        let db_name = self
            .db_name_by_id
            .get(db_id)
            .ok_or_else(|| CatalogError::NotFound("db_id", db_id.to_string()))?;
        self.database_by_name
            .get(db_name)
            .ok_or_else(|| CatalogError::NotFound("database", db_name.to_string()))
    }

    pub fn get_all_schema_names(&self, db_name: &str) -> CatalogResult<Vec<String>> {
        Ok(self.get_database_by_name(db_name)?.get_all_schema_names())
    }

    pub fn get_all_schema_info(&self, db_name: &str) -> CatalogResult<Vec<ProstSchema>> {
        Ok(self.get_database_by_name(db_name)?.get_all_schema_info())
    }

    pub fn iter_schemas(
        &self,
        db_name: &str,
    ) -> CatalogResult<impl Iterator<Item = &SchemaCatalog>> {
        Ok(self.get_database_by_name(db_name)?.iter_schemas())
    }

    pub fn get_all_database_names(&self) -> Vec<String> {
        self.database_by_name.keys().cloned().collect_vec()
    }

    pub fn get_schema_by_name(
        &self,
        db_name: &str,
        schema_name: &str,
    ) -> CatalogResult<&SchemaCatalog> {
        self.get_database_by_name(db_name)?
            .get_schema_by_name(schema_name)
            .ok_or_else(|| CatalogError::NotFound("schema", schema_name.to_string()))
    }

    pub fn get_table_name_by_id(&self, table_id: TableId) -> CatalogResult<String> {
        self.get_table_by_id(&table_id).map(|table| table.name)
    }

    pub fn get_schema_by_id(
        &self,
        db_id: &DatabaseId,
        schema_id: &SchemaId,
    ) -> CatalogResult<&SchemaCatalog> {
        self.get_database_by_id(db_id)?
            .get_schema_by_id(schema_id)
            .ok_or_else(|| CatalogError::NotFound("schema_id", schema_id.to_string()))
    }

    /// Refer to [`SearchPath`].
    pub fn first_valid_schema(
        &self,
        db_name: &str,
        search_path: &SearchPath,
        user_name: &str,
    ) -> CatalogResult<&SchemaCatalog> {
        for path in search_path.real_path() {
            let mut schema_name: &str = path;
            if schema_name == USER_NAME_WILD_CARD {
                schema_name = user_name;
            }

            if let schema_catalog @ Ok(_) = self.get_schema_by_name(db_name, schema_name) {
                return schema_catalog;
            }
        }
        Err(CatalogError::NotFound(
            "first valid schema",
            "no schema has been selected to create in".to_string(),
        ))
    }

    pub fn get_table_by_name<'a>(
        &self,
        db_name: &str,
        schema_path: SchemaPath<'a>,
        table_name: &str,
    ) -> CatalogResult<(&Arc<TableCatalog>, &'a str)> {
        schema_path
            .try_find(|schema_name| {
                Ok(self
                    .get_schema_by_name(db_name, schema_name)?
                    .get_table_by_name(table_name))
            })?
            .ok_or_else(|| CatalogError::NotFound("table", table_name.to_string()))
    }

    pub fn get_table_by_id(&self, table_id: &TableId) -> CatalogResult<TableCatalog> {
        self.table_by_id
            .get(table_id)
            .cloned()
            .ok_or_else(|| CatalogError::NotFound("table id", table_id.to_string()))
    }

    // Used by test_utils only.
    pub fn alter_table_name_by_id(&mut self, table_id: &TableId, table_name: &str) {
        let (mut database_id, mut schema_id) = (0, 0);
        let mut found = false;
        for database in self.database_by_name.values() {
            if !found {
                for schema in database.iter_schemas() {
                    if schema.iter_table().any(|t| t.id() == *table_id) {
                        found = true;
                        database_id = database.id();
                        schema_id = schema.id();
                        break;
                    }
                }
            }
        }

        if found {
            let mut table = self.get_table_by_id(table_id).unwrap();
            table.name = table_name.to_string();
            self.update_table(&table.to_prost(schema_id, database_id));
        }
    }

    #[cfg(test)]
    pub fn insert_table_id_mapping(&mut self, table_id: TableId, fragment_id: super::FragmentId) {
        self.table_by_id.insert(
            table_id,
            TableCatalog {
                fragment_id,
                ..Default::default()
            },
        );
    }

    pub fn get_sys_table_by_name(
        &self,
        db_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> CatalogResult<&SystemCatalog> {
        self.get_schema_by_name(db_name, schema_name)
            .unwrap()
            .get_system_table_by_name(table_name)
            .ok_or_else(|| CatalogError::NotFound("table", table_name.to_string()))
    }

    pub fn get_source_by_name<'a>(
        &self,
        db_name: &str,
        schema_path: SchemaPath<'a>,
        source_name: &str,
    ) -> CatalogResult<(&Arc<SourceCatalog>, &'a str)> {
        schema_path
            .try_find(|schema_name| {
                Ok(self
                    .get_schema_by_name(db_name, schema_name)?
                    .get_source_by_name(source_name))
            })?
            .ok_or_else(|| CatalogError::NotFound("source", source_name.to_string()))
    }

    pub fn get_sink_by_name<'a>(
        &self,
        db_name: &str,
        schema_path: SchemaPath<'a>,
        sink_name: &str,
    ) -> CatalogResult<(&Arc<SinkCatalog>, &'a str)> {
        schema_path
            .try_find(|schema_name| {
                Ok(self
                    .get_schema_by_name(db_name, schema_name)?
                    .get_sink_by_name(sink_name))
            })?
            .ok_or_else(|| CatalogError::NotFound("sink", sink_name.to_string()))
    }

    pub fn get_index_by_name<'a>(
        &self,
        db_name: &str,
        schema_path: SchemaPath<'a>,
        index_name: &str,
    ) -> CatalogResult<(&Arc<IndexCatalog>, &'a str)> {
        schema_path
            .try_find(|schema_name| {
                Ok(self
                    .get_schema_by_name(db_name, schema_name)?
                    .get_index_by_name(index_name))
            })?
            .ok_or_else(|| CatalogError::NotFound("index", index_name.to_string()))
    }

    pub fn get_view_by_name<'a>(
        &self,
        db_name: &str,
        schema_path: SchemaPath<'a>,
        view_name: &str,
    ) -> CatalogResult<(&Arc<ViewCatalog>, &'a str)> {
        schema_path
            .try_find(|schema_name| {
                Ok(self
                    .get_schema_by_name(db_name, schema_name)?
                    .get_view_by_name(view_name))
            })?
            .ok_or_else(|| CatalogError::NotFound("view", view_name.to_string()))
    }

    pub fn get_function_by_name_args<'a>(
        &self,
        db_name: &str,
        schema_path: SchemaPath<'a>,
        function_name: &str,
        args: &[DataType],
    ) -> CatalogResult<(&Arc<FunctionCatalog>, &'a str)> {
        schema_path
            .try_find(|schema_name| {
                Ok(self
                    .get_schema_by_name(db_name, schema_name)?
                    .get_function_by_name_args(function_name, args))
            })?
            .ok_or_else(|| CatalogError::NotFound("function", function_name.to_string()))
    }

    /// Check the name if duplicated with existing table, materialized view or source.
    pub fn check_relation_name_duplicated(
        &self,
        db_name: &str,
        schema_name: &str,
        relation_name: &str,
    ) -> CatalogResult<()> {
        let schema = self.get_schema_by_name(db_name, schema_name)?;

        if let Some(table) = schema.get_table_by_name(relation_name) {
            if table.is_index() {
                Err(CatalogError::Duplicated("index", relation_name.to_string()))
            } else if table.is_mview() {
                Err(CatalogError::Duplicated(
                    "materialized view",
                    relation_name.to_string(),
                ))
            } else {
                Err(CatalogError::Duplicated("table", relation_name.to_string()))
            }
        } else if schema.get_source_by_name(relation_name).is_some() {
            Err(CatalogError::Duplicated(
                "source",
                relation_name.to_string(),
            ))
        } else if schema.get_sink_by_name(relation_name).is_some() {
            Err(CatalogError::Duplicated("sink", relation_name.to_string()))
        } else if schema.get_view_by_name(relation_name).is_some() {
            Err(CatalogError::Duplicated("view", relation_name.to_string()))
        } else {
            Ok(())
        }
    }

    /// Get the catalog cache's catalog version.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Set the catalog cache's catalog version.
    pub fn set_version(&mut self, catalog_version: CatalogVersion) {
        self.version = catalog_version;
    }

    pub fn get_all_indexes_related_to_object(
        &self,
        db_id: DatabaseId,
        schema_id: SchemaId,
        mv_id: TableId,
    ) -> Vec<Arc<IndexCatalog>> {
        self.get_database_by_id(&db_id)
            .unwrap()
            .get_schema_by_id(&schema_id)
            .unwrap()
            .get_indexes_by_table_id(&mv_id)
    }

    pub fn get_id_by_class_name(
        &self,
        db_name: &str,
        schema_path: SchemaPath<'_>,
        class_name: &str,
    ) -> CatalogResult<u32> {
        schema_path
            .try_find(|schema_name| {
                let schema = self.get_schema_by_name(db_name, schema_name)?;
                #[allow(clippy::manual_map)]
                if let Some(item) = schema.get_system_table_by_name(class_name) {
                    Ok(Some(item.id().into()))
                } else if let Some(item) = schema.get_table_by_name(class_name) {
                    Ok(Some(item.id().into()))
                } else if let Some(item) = schema.get_index_by_name(class_name) {
                    Ok(Some(item.id.into()))
                } else if let Some(item) = schema.get_source_by_name(class_name) {
                    Ok(Some(item.id))
                } else if let Some(item) = schema.get_view_by_name(class_name) {
                    Ok(Some(item.id))
                } else {
                    Ok(None)
                }
            })?
            .map(|(id, _)| id)
            .ok_or_else(|| CatalogError::NotFound("class", class_name.to_string()))
    }
}

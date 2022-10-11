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

use std::str::FromStr;
use std::sync::Arc;

use risingwave_common::catalog::{ColumnDesc, PG_CATALOG_SCHEMA_NAME};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::session_config::SearchPath;
use risingwave_sqlparser::ast::TableAlias;

use crate::binder::{Binder, Relation};
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::source_catalog::SourceCatalog;
use crate::catalog::system_catalog::SystemCatalog;
use crate::catalog::table_catalog::TableCatalog;
use crate::catalog::{CatalogError, IndexCatalog, TableId};
use crate::user::UserId;

#[derive(Debug, Clone)]
pub struct BoundBaseTable {
    pub name: String, // explain-only
    pub table_id: TableId,
    pub table_catalog: TableCatalog,
    pub table_indexes: Vec<Arc<IndexCatalog>>,
}

/// `BoundTableSource` is used by DML statement on table source like insert, update.
#[derive(Debug)]
pub struct BoundTableSource {
    pub name: String,       // explain-only
    pub source_id: TableId, // TODO: refactor to source id
    pub associated_mview_id: TableId,
    pub columns: Vec<ColumnDesc>,
    pub append_only: bool,
    pub owner: UserId,
}

#[derive(Debug, Clone)]
pub struct BoundSystemTable {
    pub name: String, // explain-only
    pub table_id: TableId,
    pub sys_table_catalog: SystemCatalog,
}

#[derive(Debug, Clone)]
pub struct BoundSource {
    pub catalog: SourceCatalog,
}

impl From<&SourceCatalog> for BoundSource {
    fn from(s: &SourceCatalog) -> Self {
        Self { catalog: s.clone() }
    }
}

impl Binder {
    pub fn bind_table_or_source(
        &mut self,
        schema_name: Option<&str>,
        table_name: &str,
        alias: Option<TableAlias>,
    ) -> Result<Relation> {
        let (ret, columns) = {
            let catalog = &self.catalog;
            let db_name = &self.db_name;

            let resolve_sys_table_relation = |sys_table_catalog: &SystemCatalog| {
                let table = BoundSystemTable {
                    table_id: sys_table_catalog.id(),
                    name: table_name.to_string(),
                    sys_table_catalog: sys_table_catalog.clone(),
                };
                (
                    Relation::SystemTable(Box::new(table)),
                    sys_table_catalog.columns.clone(),
                )
            };

            let resolve_table_relation = |table_catalog: &TableCatalog, schema_name| {
                let table_id = table_catalog.id();
                let table_catalog = table_catalog.clone();
                let columns = table_catalog.columns.clone();
                let table_indexes = self.resolve_table_indexes(schema_name, table_id)?;

                let table = BoundBaseTable {
                    name: table_name.to_string(),
                    table_id,
                    table_catalog,
                    table_indexes,
                };

                Ok::<_, RwError>((Relation::BaseTable(Box::new(table)), columns))
            };

            let resolve_source_relation = |source_catalog: &SourceCatalog| {
                (
                    Relation::Source(Box::new(source_catalog.into())),
                    source_catalog.columns.clone(),
                )
            };

            match schema_name {
                Some(schema_name) => {
                    let schema_path = SchemaPath::Name(schema_name);
                    if schema_name == PG_CATALOG_SCHEMA_NAME {
                        if let Ok(sys_table_catalog) =
                            catalog.get_sys_table_by_name(db_name, table_name)
                        {
                            resolve_sys_table_relation(sys_table_catalog)
                        } else {
                            return Err(ErrorCode::NotImplemented(
                                format!(
                                    r###"pg_catalog.{} is not supported, please use `SHOW` commands for now.
`SHOW TABLES`,
`SHOW MATERIALIZED VIEWS`,
`DESCRIBE <table>`,
`SHOW COLUMNS FROM [table]`
"###,
                                    table_name
                                ),
                                1695.into(),
                            ).into());
                        }
                    } else if let Ok((table_catalog, schema_name)) =
                        catalog.get_table_by_name(db_name, schema_path, table_name)
                    {
                        resolve_table_relation(table_catalog, schema_name)?
                    } else if let Ok((source_catalog, _)) =
                        catalog.get_source_by_name(db_name, schema_path, table_name)
                    {
                        resolve_source_relation(source_catalog)
                    } else {
                        return Err(RwError::from(CatalogError::NotFound(
                            "table or source",
                            table_name.to_string(),
                        )));
                    }
                }
                None => {
                    let user_name = &self.auth_context.user_name;
                    let paths = self.search_path.path();
                    let pg_catalog_position = paths
                        .iter()
                        .position(|path| path == PG_CATALOG_SCHEMA_NAME)
                        .unwrap();

                    let search_path_left =
                        SearchPath::from_str(&paths[0..pg_catalog_position].join(", "))?;
                    let schema_path_left = SchemaPath::Path(&search_path_left, user_name);
                    let search_path_right = SearchPath::from_str(
                        &paths[pg_catalog_position + 1..paths.len()].join(", "),
                    )?;
                    let schema_path_right = SchemaPath::Path(&search_path_right, user_name);
                    let schema_path = SchemaPath::Path(&self.search_path, user_name);

                    if let Ok((table_catalog, schema_name)) =
                        catalog.get_table_by_name(db_name, schema_path_left, table_name)
                    {
                        resolve_table_relation(table_catalog, schema_name)?
                    } else if let Ok(sys_table_catalog) =
                        catalog.get_sys_table_by_name(db_name, table_name)
                    {
                        resolve_sys_table_relation(sys_table_catalog)
                    } else if let Ok((table_catalog, schema_name)) =
                        catalog.get_table_by_name(db_name, schema_path_right, table_name)
                    {
                        resolve_table_relation(table_catalog, schema_name)?
                    } else if let Ok((source_catalog, _)) =
                        catalog.get_source_by_name(db_name, schema_path, table_name)
                    {
                        resolve_source_relation(source_catalog)
                    } else {
                        return Err(RwError::from(CatalogError::NotFound(
                            "table or source",
                            table_name.to_string(),
                        )));
                    }
                }
            }
        };

        self.bind_table_to_context(
            columns
                .iter()
                .map(|c| (c.is_hidden, (&c.column_desc).into())),
            table_name.to_string(),
            alias,
        )?;
        Ok(ret)
    }

    fn resolve_table_indexes(
        &self,
        schema_name: &str,
        table_id: TableId,
    ) -> Result<Vec<Arc<IndexCatalog>>> {
        Ok(self
            .catalog
            .get_schema_by_name(&self.db_name, schema_name)?
            .iter_index()
            .filter(|index| index.primary_table.id == table_id)
            .map(|index| index.clone().into())
            .collect())
    }

    pub(crate) fn bind_table(
        &mut self,
        schema_name: Option<&str>,
        table_name: &str,
        alias: Option<TableAlias>,
    ) -> Result<BoundBaseTable> {
        let db_name = &self.db_name;
        let schema_path = match schema_name {
            Some(schema_name) => SchemaPath::Name(schema_name),
            None => SchemaPath::Path(&self.search_path, &self.auth_context.user_name),
        };
        let (table_catalog, schema_name) =
            self.catalog
                .get_table_by_name(db_name, schema_path, table_name)?;
        let table_catalog = table_catalog.clone();

        let table_id = table_catalog.id();
        let table_indexes = self.resolve_table_indexes(schema_name, table_id)?;

        let columns = table_catalog.columns.clone();

        self.bind_table_to_context(
            columns
                .iter()
                .map(|c| (c.is_hidden, (&c.column_desc).into())),
            table_name.to_string(),
            alias,
        )?;

        Ok(BoundBaseTable {
            name: table_name.to_string(),
            table_id,
            table_catalog,
            table_indexes,
        })
    }

    pub(crate) fn bind_table_source(
        &mut self,
        schema_name: Option<&str>,
        source_name: &str,
    ) -> Result<BoundTableSource> {
        let db_name = &self.db_name;
        let schema_path = match schema_name {
            Some(schema_name) => SchemaPath::Name(schema_name),
            None => SchemaPath::Path(&self.search_path, &self.auth_context.user_name),
        };
        let (associate_table, schema_name) =
            self.catalog
                .get_table_by_name(db_name, schema_path, source_name)?;
        let associate_table_id = associate_table.id();

        let (source, _) = self.catalog.get_source_by_name(
            &self.db_name,
            SchemaPath::Name(schema_name),
            source_name,
        )?;

        let source_id = TableId::new(source.id);

        let append_only = source.append_only;
        let columns = source
            .columns
            .iter()
            .filter(|c| !c.is_hidden)
            .map(|c| c.column_desc.clone())
            .collect();

        let owner = source.owner;

        // Note(bugen): do not bind context here.

        Ok(BoundTableSource {
            name: source_name.to_string(),
            source_id,
            associated_mview_id: associate_table_id,
            columns,
            append_only,
            owner,
        })
    }
}

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

use std::ops::Deref;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::catalog::{Field, SYSTEM_SCHEMAS};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::session_config::USER_NAME_WILD_CARD;
use risingwave_sqlparser::ast::{Statement, TableAlias};
use risingwave_sqlparser::parser::Parser;

use super::BoundShare;
use crate::binder::relation::BoundSubquery;
use crate::binder::{Binder, Relation};
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::source_catalog::SourceCatalog;
use crate::catalog::system_catalog::SystemCatalog;
use crate::catalog::table_catalog::{TableCatalog, TableType};
use crate::catalog::view_catalog::ViewCatalog;
use crate::catalog::{CatalogError, IndexCatalog, TableId};

#[derive(Debug, Clone)]
pub struct BoundBaseTable {
    pub table_id: TableId,
    pub table_catalog: TableCatalog,
    pub table_indexes: Vec<Arc<IndexCatalog>>,
    pub for_system_time_as_of_now: bool,
}

#[derive(Debug, Clone)]
pub struct BoundSystemTable {
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
    /// Binds table or source, or logical view according to what we get from the catalog.
    pub fn bind_relation_by_name_inner(
        &mut self,
        schema_name: Option<&str>,
        table_name: &str,
        alias: Option<TableAlias>,
        for_system_time_as_of_now: bool,
    ) -> Result<Relation> {
        fn is_system_schema(schema_name: &str) -> bool {
            SYSTEM_SCHEMAS.iter().any(|s| *s == schema_name)
        }

        // define some helper functions converting catalog to bound relation
        let resolve_sys_table_relation = |sys_table_catalog: &SystemCatalog| {
            let table = BoundSystemTable {
                table_id: sys_table_catalog.id(),
                sys_table_catalog: sys_table_catalog.clone(),
            };
            (
                Relation::SystemTable(Box::new(table)),
                sys_table_catalog
                    .columns
                    .iter()
                    .map(|c| (c.is_hidden, Field::from(&c.column_desc)))
                    .collect_vec(),
            )
        };

        // start to bind
        let (ret, columns) = {
            match schema_name {
                Some(schema_name) => {
                    let schema_path = SchemaPath::Name(schema_name);
                    if is_system_schema(schema_name) {
                        if let Ok(sys_table_catalog) = self.catalog.get_sys_table_by_name(
                            &self.db_name,
                            schema_name,
                            table_name,
                        ) {
                            resolve_sys_table_relation(sys_table_catalog)
                        } else {
                            return Err(ErrorCode::NotImplemented(
                                format!(
                                    r###"{}.{} is not supported, please use `SHOW` commands for now.
`SHOW TABLES`,
`SHOW MATERIALIZED VIEWS`,
`DESCRIBE <table>`,
`SHOW COLUMNS FROM [table]`
"###,
                                    schema_name, table_name
                                ),
                                1695.into(),
                            )
                            .into());
                        }
                    } else if let Ok((table_catalog, schema_name)) =
                        self.catalog
                            .get_table_by_name(&self.db_name, schema_path, table_name)
                    {
                        self.resolve_table_relation(
                            &table_catalog.clone(),
                            schema_name,
                            for_system_time_as_of_now,
                        )?
                    } else if let Ok((source_catalog, _)) =
                        self.catalog
                            .get_source_by_name(&self.db_name, schema_path, table_name)
                    {
                        self.resolve_source_relation(&source_catalog.clone())
                    } else if let Ok((view_catalog, _)) =
                        self.catalog
                            .get_view_by_name(&self.db_name, schema_path, table_name)
                    {
                        self.resolve_view_relation(&view_catalog.clone())?
                    } else {
                        return Err(CatalogError::NotFound(
                            "table or source",
                            table_name.to_string(),
                        )
                        .into());
                    }
                }
                None => (|| {
                    let user_name = &self.auth_context.user_name;

                    for path in self.search_path.path() {
                        if is_system_schema(path) {
                            if let Ok(sys_table_catalog) =
                                self.catalog
                                    .get_sys_table_by_name(&self.db_name, path, table_name)
                            {
                                return Ok(resolve_sys_table_relation(sys_table_catalog));
                            }
                        } else {
                            let schema_name = if path == USER_NAME_WILD_CARD {
                                user_name
                            } else {
                                path
                            };

                            if let Ok(schema) =
                                self.catalog.get_schema_by_name(&self.db_name, schema_name)
                            {
                                if let Some(table_catalog) = schema.get_table_by_name(table_name) {
                                    return self.resolve_table_relation(
                                        &table_catalog.clone(),
                                        &schema_name.clone(),
                                        for_system_time_as_of_now,
                                    );
                                } else if let Some(source_catalog) =
                                    schema.get_source_by_name(table_name)
                                {
                                    return Ok(
                                        self.resolve_source_relation(&source_catalog.clone())
                                    );
                                } else if let Some(view_catalog) =
                                    schema.get_view_by_name(table_name)
                                {
                                    return self.resolve_view_relation(&view_catalog.clone());
                                }
                            }
                        }
                    }

                    Err(CatalogError::NotFound("table or source", table_name.to_string()).into())
                })()?,
            }
        };

        self.bind_table_to_context(columns, table_name.to_string(), alias)?;
        Ok(ret)
    }

    fn resolve_table_relation(
        &mut self,
        table_catalog: &TableCatalog,
        schema_name: &str,
        for_system_time_as_of_now: bool,
    ) -> Result<(Relation, Vec<(bool, Field)>)> {
        let table_id = table_catalog.id();
        let table_catalog = table_catalog.clone();
        let columns = table_catalog
            .columns
            .iter()
            .map(|c| (c.is_hidden, Field::from(&c.column_desc)))
            .collect_vec();
        self.included_relations.insert(table_id);
        let table_indexes = self.resolve_table_indexes(schema_name, table_id)?;

        let table = BoundBaseTable {
            table_id,
            table_catalog,
            table_indexes,
            for_system_time_as_of_now,
        };

        Ok::<_, RwError>((Relation::BaseTable(Box::new(table)), columns))
    }

    fn resolve_source_relation(
        &mut self,
        source_catalog: &SourceCatalog,
    ) -> (Relation, Vec<(bool, Field)>) {
        self.included_relations.insert(source_catalog.id.into());
        (
            Relation::Source(Box::new(source_catalog.into())),
            source_catalog
                .columns
                .iter()
                .map(|c| (c.is_hidden, Field::from(&c.column_desc)))
                .collect_vec(),
        )
    }

    fn resolve_view_relation(
        &mut self,
        view_catalog: &ViewCatalog,
    ) -> Result<(Relation, Vec<(bool, Field)>)> {
        let ast = Parser::parse_sql(&view_catalog.sql)
            .expect("a view's sql should be parsed successfully");
        let Statement::Query(query) = ast
            .into_iter()
            .exactly_one()
            .expect("a view should contain only one statement") else {
            unreachable!("a view should contain a query statement");
        };
        let query = self.bind_query(*query).map_err(|e| {
            ErrorCode::BindError(format!(
                "failed to bind view {}, sql: {}\nerror: {}",
                view_catalog.name, view_catalog.sql, e
            ))
        })?;
        let columns = view_catalog.columns.clone();
        let share_id = match self.shared_views.get(&view_catalog.id) {
            Some(share_id) => *share_id,
            None => {
                let share_id = self.next_share_id();
                self.shared_views.insert(view_catalog.id, share_id);
                self.included_relations.insert(view_catalog.id.into());
                share_id
            }
        };
        let input = Relation::Subquery(Box::new(BoundSubquery { query }));
        Ok((
            Relation::Share(Box::new(BoundShare { share_id, input })),
            columns.iter().map(|c| (false, c.clone())).collect_vec(),
        ))
    }

    fn resolve_table_indexes(
        &self,
        schema_name: &str,
        table_id: TableId,
    ) -> Result<Vec<Arc<IndexCatalog>>> {
        Ok(self
            .catalog
            .get_schema_by_name(&self.db_name, schema_name)?
            .get_indexes_by_table_id(&table_id))
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
        let table_catalog = table_catalog.deref().clone();

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
            table_id,
            table_catalog,
            table_indexes,
            for_system_time_as_of_now: false,
        })
    }

    pub(crate) fn get_table_by_id(&mut self, table_id: &TableId) -> Result<BoundBaseTable> {
        let db = &self.db_name;
        // Find the schema catalog that contains this TableId
        let schema = self.catalog.get_schema_by_table_id(db, table_id)?;
        // Get the Indexes for this table
        let table_indexes = schema.get_indexes_by_table_id(table_id);
        // Get the table catalog for this table
        let table_catalog = self.catalog.get_table_by_id(table_id)?;
        // Get the columns for this table
        // let columns = table_catalog.columns.clone();

        // self.bind_table_to_context(
        // columns
        // .iter()
        // .map(|c| (c.is_hidden, (&c.column_desc).into())),
        // table_catalog.name().to_string(),
        // None,
        // )?;

        // Create a BoundBaseTable
        Ok(BoundBaseTable {
            table_id: *table_id,
            table_catalog,
            table_indexes,
            for_system_time_as_of_now: false,
        })
    }

    pub(crate) fn resolve_dml_table<'a>(
        &'a self,
        schema_name: Option<&str>,
        table_name: &str,
        is_insert: bool,
    ) -> Result<&'a TableCatalog> {
        let db_name = &self.db_name;
        let schema_path = match schema_name {
            Some(schema_name) => SchemaPath::Name(schema_name),
            None => SchemaPath::Path(&self.search_path, &self.auth_context.user_name),
        };

        let (table, _schema_name) =
            self.catalog
                .get_table_by_name(db_name, schema_path, table_name)?;

        match table.table_type() {
            TableType::Table => {}
            TableType::Index => {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "cannot change index \"{table_name}\""
                ))
                .into())
            }
            TableType::MaterializedView => {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "cannot change materialized view \"{table_name}\""
                ))
                .into())
            }
            TableType::Internal => {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "cannot change internal table \"{table_name}\""
                ))
                .into())
            }
        }

        if table.append_only && !is_insert {
            return Err(ErrorCode::BindError(
                "append-only table does not support update or delete".to_string(),
            )
            .into());
        }

        Ok(table)
    }

    pub(crate) fn resolve_regclass(&self, class_name: &str) -> Result<u32> {
        let schema_path = SchemaPath::Path(&self.search_path, &self.auth_context.user_name);
        Ok(self
            .catalog
            .get_id_by_class_name(&self.db_name, schema_path, class_name)?)
    }
}

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

use std::collections::hash_map::Entry;
use std::str::FromStr;

use itertools::Itertools;
use risingwave_common::catalog::{ColumnDesc, DEFAULT_SCHEMA_NAME};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::types::DataType;
use risingwave_pb::plan::JoinType;
use risingwave_sqlparser::ast::{
    JoinConstraint, JoinOperator, ObjectName, Query, TableAlias, TableFactor, TableWithJoins,
};

use super::bind_context::ColumnBinding;
use super::{BoundQuery, BoundWindowTableFunction, WindowTableFunctionKind, UNNAMED_SUBQUERY};
use crate::binder::Binder;
use crate::catalog::source_catalog::SourceCatalog;
use crate::catalog::table_catalog::TableCatalog;
use crate::catalog::{CatalogError, TableId};
use crate::expr::{Expr, ExprImpl};

/// A validated item that refers to a table-like entity, including base table, subquery, join, etc.
/// It is usually part of the `from` clause.
#[derive(Debug)]
pub enum Relation {
    Source(Box<BoundSource>),
    BaseTable(Box<BoundBaseTable>),
    Subquery(Box<BoundSubquery>),
    Join(Box<BoundJoin>),
    WindowTableFunction(Box<BoundWindowTableFunction>),
}

#[derive(Debug)]
pub struct BoundJoin {
    pub join_type: JoinType,
    pub left: Relation,
    pub right: Relation,
    pub cond: ExprImpl,
}

#[derive(Debug)]
pub struct BoundBaseTable {
    pub name: String, // explain-only
    pub table_id: TableId,
    pub table_catalog: TableCatalog,
}

impl From<&TableCatalog> for BoundBaseTable {
    fn from(t: &TableCatalog) -> Self {
        Self {
            name: t.name.clone(),
            table_id: t.id,
            table_catalog: t.clone(),
        }
    }
}

#[derive(Debug)]
pub struct BoundSubquery {
    pub query: BoundQuery,
}

/// `BoundTableSource` is used by DML statement on table source like insert, updata
#[derive(Debug)]
pub struct BoundTableSource {
    pub name: String,       // explain-only
    pub source_id: TableId, // TODO: refactor to source id
    pub columns: Vec<ColumnDesc>,
}

#[derive(Debug)]
pub struct BoundSource {
    pub catalog: SourceCatalog,
}

impl From<&SourceCatalog> for BoundSource {
    fn from(s: &SourceCatalog) -> Self {
        Self { catalog: s.clone() }
    }
}

impl Binder {
    pub(super) fn bind_vec_table_with_joins(
        &mut self,
        from: Vec<TableWithJoins>,
    ) -> Result<Option<Relation>> {
        let mut from_iter = from.into_iter();
        let first = match from_iter.next() {
            Some(t) => t,
            None => return Ok(None),
        };
        let mut root = self.bind_table_with_joins(first)?;
        for t in from_iter {
            let right = self.bind_table_with_joins(t)?;
            root = Relation::Join(Box::new(BoundJoin {
                join_type: JoinType::Inner,
                left: root,
                right,
                cond: ExprImpl::literal_bool(true),
            }));
        }
        Ok(Some(root))
    }

    fn bind_table_with_joins(&mut self, table: TableWithJoins) -> Result<Relation> {
        let mut root = self.bind_table_factor(table.relation)?;
        for join in table.joins {
            let right = self.bind_table_factor(join.relation)?;
            let (constraint, join_type) = match join.join_operator {
                JoinOperator::Inner(constraint) => (constraint, JoinType::Inner),
                JoinOperator::LeftOuter(constraint) => (constraint, JoinType::LeftOuter),
                JoinOperator::RightOuter(constraint) => (constraint, JoinType::RightOuter),
                JoinOperator::FullOuter(constraint) => (constraint, JoinType::FullOuter),
                // Cross join equals to inner join with with no constraint.
                JoinOperator::CrossJoin => (JoinConstraint::None, JoinType::Inner),
            };
            let cond = self.bind_join_constraint(constraint)?;
            let join = BoundJoin {
                join_type,
                left: root,
                right,
                cond,
            };
            root = Relation::Join(Box::new(join));
        }

        Ok(root)
    }

    fn bind_join_constraint(&mut self, constraint: JoinConstraint) -> Result<ExprImpl> {
        Ok(match constraint {
            JoinConstraint::None => ExprImpl::literal_bool(true),
            JoinConstraint::Natural => {
                return Err(ErrorCode::NotImplemented("Natural join".into(), 1633.into()).into())
            }
            JoinConstraint::On(expr) => {
                let bound_expr = self.bind_expr(expr)?;
                if bound_expr.return_type() != DataType::Boolean {
                    return Err(ErrorCode::InternalError(format!(
                        "argument of ON must be boolean, not type {:?}",
                        bound_expr.return_type()
                    ))
                    .into());
                }
                bound_expr
            }
            JoinConstraint::Using(_columns) => {
                return Err(ErrorCode::NotImplemented("USING".into(), 1636.into()).into())
            }
        })
    }

    pub(super) fn bind_table_factor(&mut self, table_factor: TableFactor) -> Result<Relation> {
        match table_factor {
            TableFactor::Table { name, alias, args } => {
                if args.is_empty() {
                    let (schema_name, table_name) = Self::resolve_table_name(name)?;
                    self.bind_table_or_source(&schema_name, &table_name, alias)
                } else {
                    let kind =
                        WindowTableFunctionKind::from_str(&name.0[0].value).map_err(|_| {
                            ErrorCode::NotImplemented(
                                format!("unknown window function kind: {}", name.0[0].value),
                                1191.into(),
                            )
                        })?;
                    Ok(Relation::WindowTableFunction(Box::new(
                        self.bind_window_table_function(kind, args)?,
                    )))
                }
            }
            TableFactor::Derived {
                lateral,
                subquery,
                alias,
            } => {
                if lateral {
                    Err(ErrorCode::NotImplemented("unsupported lateral".into(), None.into()).into())
                } else {
                    Ok(Relation::Subquery(Box::new(
                        self.bind_subquery_relation(*subquery, alias)?,
                    )))
                }
            }
            _ => Err(ErrorCode::NotImplemented(
                format!("unsupported table factor {:?}", table_factor),
                None.into(),
            )
            .into()),
        }
    }

    pub(super) fn bind_table_or_source(
        &mut self,
        schema_name: &str,
        table_name: &str,
        alias: Option<TableAlias>,
    ) -> Result<Relation> {
        if schema_name == "pg_catalog" {
            // TODO: support pg_catalog.
            return Err(ErrorCode::NotImplemented(
                // TODO: We can ref the document of `SHOW` commands here if ready.
                r###"pg_catalog is not supported, please use `SHOW` commands for now.
`SHOW TABLES`,
`SHOW MATERIALIZED VIEWS`,
`DESCRIBE <table>`,
`SHOW COLUMNS FROM [table]`
"###
                .into(),
                1695.into(),
            )
            .into());
        }

        let (ret, columns) = {
            let catalog = &self.catalog;

            catalog
                .get_table_by_name(&self.db_name, schema_name, table_name)
                .map(|t| (Relation::BaseTable(Box::new(t.into())), t.columns.clone()))
                .or_else(|_| {
                    catalog
                        .get_source_by_name(&self.db_name, schema_name, table_name)
                        .map(|s| (Relation::Source(Box::new(s.into())), s.columns.clone()))
                })
                .map_err(|_| {
                    RwError::from(CatalogError::NotFound(
                        "table or source",
                        table_name.to_string(),
                    ))
                })?
        };

        self.bind_context(
            columns
                .iter()
                .cloned()
                .map(|c| (c.name().to_string(), c.data_type().clone(), c.is_hidden)),
            table_name.to_string(),
            alias,
        )?;
        Ok(ret)
    }

    pub(super) fn bind_table(
        &mut self,
        schema_name: &str,
        table_name: &str,
        alias: Option<TableAlias>,
    ) -> Result<BoundBaseTable> {
        let table_catalog = self
            .catalog
            .get_table_by_name(&self.db_name, schema_name, table_name)?
            .clone();
        let columns = table_catalog.columns.clone();

        self.bind_context(
            columns
                .iter()
                .cloned()
                .map(|c| (c.name().to_string(), c.data_type().clone(), c.is_hidden)),
            table_name.to_string(),
            alias,
        )?;

        let table_id = table_catalog.id();
        Ok(BoundBaseTable {
            name: table_name.to_string(),
            table_id,
            table_catalog,
        })
    }

    /// return the (`schema_name`, `table_name`)
    pub fn resolve_table_name(name: ObjectName) -> Result<(String, String)> {
        let mut identifiers = name.0;
        let table_name = identifiers
            .pop()
            .ok_or_else(|| ErrorCode::InternalError("empty table name".into()))?
            .value;

        let schema_name = identifiers
            .pop()
            .map(|ident| ident.value)
            .unwrap_or_else(|| DEFAULT_SCHEMA_NAME.into());

        Ok((schema_name, table_name))
    }

    pub(super) fn bind_table_source(&mut self, name: ObjectName) -> Result<BoundTableSource> {
        let (schema_name, source_name) = Self::resolve_table_name(name)?;
        let source = self
            .catalog
            .get_source_by_name(&self.db_name, &schema_name, &source_name)?;

        let source_id = TableId::new(source.id);

        let columns = source
            .columns
            .iter()
            .filter(|c| !c.is_hidden)
            .map(|c| c.column_desc.clone())
            .collect();

        // Note(bugen): do not bind context here.

        Ok(BoundTableSource {
            name: source_name,
            source_id,
            columns,
        })
    }

    /// Fill the [`BindContext`](super::BindContext) for table.
    pub(super) fn bind_context(
        &mut self,
        columns: impl IntoIterator<Item = (String, DataType, bool)>,
        table_name: String,
        alias: Option<TableAlias>,
    ) -> Result<()> {
        let (table_name, column_aliases) = match alias {
            None => (table_name, vec![]),
            Some(TableAlias { name, columns }) => (name.value, columns),
        };

        let begin = self.context.columns.len();
        // Column aliases can be less than columns, but not more.
        // It also needs to skip hidden columns.
        let mut alias_iter = column_aliases.into_iter().fuse();
        columns
            .into_iter()
            .enumerate()
            .for_each(|(index, (name, data_type, is_hidden))| {
                let name = match is_hidden {
                    true => name,
                    false => alias_iter.next().map(|t| t.value).unwrap_or(name),
                };
                self.context.columns.push(ColumnBinding::new(
                    table_name.clone(),
                    name.clone(),
                    begin + index,
                    data_type,
                    is_hidden,
                ));
                self.context
                    .indexs_of
                    .entry(name)
                    .or_default()
                    .push(self.context.columns.len() - 1);
            });
        if alias_iter.next().is_some() {
            return Err(ErrorCode::BindError(format!(
                "table \"{table_name}\" has less columns available but more aliases specified",
            ))
            .into());
        }

        match self.context.range_of.entry(table_name.clone()) {
            Entry::Occupied(_) => Err(ErrorCode::InternalError(format!(
                "Duplicated table name while binding context: {}",
                table_name
            ))
            .into()),
            Entry::Vacant(entry) => {
                entry.insert((begin, self.context.columns.len()));
                Ok(())
            }
        }
    }

    /// Binds a subquery using [`bind_query`](Self::bind_query), which will use a new empty
    /// [`BindContext`](super::BindContext) for it.
    ///
    /// After finishing binding, we update the current context with the output of the subquery.
    pub(super) fn bind_subquery_relation(
        &mut self,
        query: Query,
        alias: Option<TableAlias>,
    ) -> Result<BoundSubquery> {
        let query = self.bind_query(query)?;
        let sub_query_id = self.next_subquery_id();
        self.bind_context(
            query
                .names()
                .into_iter()
                .zip_eq(query.data_types().into_iter())
                .map(|(x, y)| (x, y, false)),
            format!("{}_{}", UNNAMED_SUBQUERY, sub_query_id),
            alias,
        )?;
        Ok(BoundSubquery { query })
    }
}

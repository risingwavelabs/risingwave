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
use risingwave_common::catalog::{Field, TableId, DEFAULT_SCHEMA_NAME, RW_TABLE_FUNCTION_NAME};
use risingwave_common::error::{internal_error, ErrorCode, Result, RwError};
use risingwave_sqlparser::ast::{FunctionArg, Ident, ObjectName, TableAlias, TableFactor};

use super::bind_context::ColumnBinding;
use crate::binder::{Binder, BoundSetExpr};
use crate::expr::{Expr, ExprImpl, TableFunction, TableFunctionType};

mod join;
mod subquery;
mod table_or_source;
mod window_table_function;

pub use join::BoundJoin;
pub use subquery::BoundSubquery;
pub use table_or_source::{BoundBaseTable, BoundSource, BoundSystemTable, BoundTableSource};
pub use window_table_function::{BoundWindowTableFunction, WindowTableFunctionKind};

use crate::expr::{CorrelatedId, Depth};

/// A validated item that refers to a table-like entity, including base table, subquery, join, etc.
/// It is usually part of the `from` clause.
#[derive(Debug, Clone)]
pub enum Relation {
    Source(Box<BoundSource>),
    BaseTable(Box<BoundBaseTable>),
    SystemTable(Box<BoundSystemTable>),
    Subquery(Box<BoundSubquery>),
    Join(Box<BoundJoin>),
    WindowTableFunction(Box<BoundWindowTableFunction>),
    TableFunction(Box<TableFunction>),
}

impl Relation {
    pub fn contains_sys_table(&self) -> bool {
        match self {
            Relation::SystemTable(_) => true,
            Relation::Subquery(s) => {
                if let BoundSetExpr::Select(select) = &s.query.body
                    && let Some(relation) = &select.from {
                    relation.contains_sys_table()
                } else {
                    false
                }
            },
            Relation::Join(j) => {
                j.left.contains_sys_table() || j.right.contains_sys_table()
            },
            _ => false,
        }
    }

    pub fn is_correlated(&self) -> bool {
        match self {
            Relation::Subquery(subquery) => subquery.query.is_correlated(),
            Relation::Join(join) => {
                join.cond.has_correlated_input_ref_by_depth()
                    || join.left.is_correlated()
                    || join.right.is_correlated()
            }
            _ => false,
        }
    }

    pub fn collect_correlated_indices_by_depth_and_assign_id(
        &mut self,
        depth: Depth,
        correlated_id: CorrelatedId,
    ) -> Vec<usize> {
        match self {
            Relation::Subquery(subquery) => subquery
                .query
                .collect_correlated_indices_by_depth_and_assign_id(depth + 1, correlated_id),
            Relation::Join(join) => {
                let mut correlated_indices = vec![];
                correlated_indices.extend(
                    join.cond
                        .collect_correlated_indices_by_depth_and_assign_id(depth, correlated_id),
                );
                correlated_indices.extend(
                    join.left
                        .collect_correlated_indices_by_depth_and_assign_id(depth, correlated_id),
                );
                correlated_indices.extend(
                    join.right
                        .collect_correlated_indices_by_depth_and_assign_id(depth, correlated_id),
                );
                correlated_indices
            }
            _ => vec![],
        }
    }
}

impl Binder {
    /// return first and second name in identifiers,
    /// must have one name and can use default name as other one.
    fn resolve_double_name(
        mut identifiers: Vec<Ident>,
        err_str: &str,
        default_name: &str,
    ) -> Result<(String, String)> {
        let second_name = identifiers
            .pop()
            .ok_or_else(|| ErrorCode::InternalError(err_str.into()))?
            .real_value();

        let first_name = identifiers
            .pop()
            .map(|ident| ident.real_value())
            .unwrap_or_else(|| default_name.into());

        Ok((first_name, second_name))
    }

    /// return first name in identifiers, must have only one name.
    fn resolve_single_name(mut identifiers: Vec<Ident>, ident_desc: &str) -> Result<String> {
        if identifiers.len() > 1 {
            return Err(internal_error(format!(
                "{} must contain 1 argument",
                ident_desc
            )));
        }
        let name = identifiers
            .pop()
            .ok_or_else(|| ErrorCode::InternalError(format!("empty {}", ident_desc)))?
            .real_value();

        Ok(name)
    }

    /// return the (`schema_name`, `table_name`)
    pub fn resolve_table_name(name: ObjectName) -> Result<(String, String)> {
        Self::resolve_double_name(name.0, "empty table name", DEFAULT_SCHEMA_NAME)
    }

    /// return the ( `database_name`, `schema_name`)
    pub fn resolve_schema_name(
        default_db_name: &str,
        name: ObjectName,
    ) -> Result<(String, String)> {
        Self::resolve_double_name(name.0, "empty schema name", default_db_name)
    }

    /// return the `database_name`
    pub fn resolve_database_name(name: ObjectName) -> Result<String> {
        Self::resolve_single_name(name.0, "database name")
    }

    /// return the `user_name`
    pub fn resolve_user_name(name: ObjectName) -> Result<String> {
        Self::resolve_single_name(name.0, "user name")
    }

    /// return the (`schema_name`, `index_name`)
    pub fn resolve_index_name(name: ObjectName) -> Result<(String, String)> {
        Self::resolve_double_name(name.0, "empty index name", DEFAULT_SCHEMA_NAME)
    }

    /// Fill the [`BindContext`](super::BindContext) for table.
    pub(super) fn bind_table_to_context(
        &mut self,
        columns: impl IntoIterator<Item = (bool, Field)>, // bool indicates if the field is hidden
        table_name: String,
        alias: Option<TableAlias>,
    ) -> Result<()> {
        let (table_name, column_aliases) = match alias {
            None => (table_name, vec![]),
            Some(TableAlias { name, columns }) => (name.real_value(), columns),
        };

        let num_col_aliases = column_aliases.len();

        let begin = self.context.columns.len();
        // Column aliases can be less than columns, but not more.
        // It also needs to skip hidden columns.
        let mut alias_iter = column_aliases.into_iter().fuse();
        let mut index = 0;
        columns.into_iter().for_each(|(is_hidden, mut field)| {
            let name = match is_hidden {
                true => field.name.to_string(),
                false => alias_iter
                    .next()
                    .map(|t| t.value)
                    .unwrap_or_else(|| field.name.to_string()),
            };
            field.name = name.clone();
            self.context.columns.push(ColumnBinding::new(
                table_name.clone(),
                begin + index,
                is_hidden,
                field,
            ));
            self.context
                .indices_of
                .entry(name)
                .or_default()
                .push(self.context.columns.len() - 1);
            index += 1;
        });

        let num_cols = index;
        if num_cols < num_col_aliases {
            return Err(ErrorCode::BindError(format!(
                "table \"{table_name}\" has {num_cols} columns available but {num_col_aliases} column aliases specified",
            ))
            .into());
        }

        match self.context.range_of.entry(table_name.clone()) {
            Entry::Occupied(_) => Err(ErrorCode::InternalError(format!(
                "Duplicated table name while binding table to context: {}",
                table_name
            ))
            .into()),
            Entry::Vacant(entry) => {
                entry.insert((begin, self.context.columns.len()));
                Ok(())
            }
        }
    }

    pub(super) fn bind_relation_by_name(
        &mut self,
        name: ObjectName,
        alias: Option<TableAlias>,
    ) -> Result<Relation> {
        let has_schema_name = name.0.len() > 1;
        let (schema_name, table_name) = Self::resolve_table_name(name)?;
        if !has_schema_name
            && let Some(bound_query) = self.cte_to_relation.get(&table_name)
        {
            let (query, mut original_alias) = bound_query.clone();
            debug_assert_eq!(original_alias.name.real_value(), table_name); // The original CTE alias ought to be its table name.

            if let Some(from_alias) = alias {
                original_alias.name = from_alias.name;
                let mut alias_iter = from_alias.columns.into_iter();
                original_alias.columns = original_alias.columns.into_iter().map(|ident| {
                    alias_iter
                        .next()
                        .unwrap_or(ident)
                }).collect();
            }

            self.bind_table_to_context(
                query
                    .body
                    .schema()
                    .fields
                    .iter()
                    .map(|f| (false, f.clone())),
                table_name,
                Some(original_alias),
            )?;
            Ok(Relation::Subquery(Box::new(BoundSubquery { query })))
        } else {
            self.bind_table_or_source(&schema_name, &table_name, alias)
        }
    }

    pub(super) fn bind_relation_by_id(
        &mut self,
        table_id: TableId,
        schema_name: String,
        alias: Option<TableAlias>,
    ) -> Result<Relation> {
        let table_name =
            self.catalog
                .get_table_name_by_id(table_id, &self.db_name, &schema_name)?;
        self.bind_table_or_source(&schema_name, &table_name, alias)
    }

    fn resolve_table_id(&self, args: Vec<FunctionArg>) -> Result<(String, TableId)> {
        if args.is_empty() || args.len() > 2 {
            return Err(ErrorCode::BindError(
                "usage: rw_table(table_id[,schema_name])".to_string(),
            )
            .into());
        }

        let table_id: TableId = args[0]
            .to_string()
            .parse::<u32>()
            .map_err(|err| {
                RwError::from(ErrorCode::BindError(format!("invalid table id: {}", err)))
            })?
            .into();

        let schema = args
            .get(1)
            .map_or(DEFAULT_SCHEMA_NAME.to_string(), |arg| arg.to_string());
        Ok((schema, table_id))
    }

    pub(super) fn bind_table_factor(&mut self, table_factor: TableFactor) -> Result<Relation> {
        match table_factor {
            TableFactor::Table { name, alias, args } => {
                if args.is_empty() {
                    self.bind_relation_by_name(name, alias)
                } else {
                    let func_name = &name.0[0].value;
                    if func_name.eq_ignore_ascii_case(RW_TABLE_FUNCTION_NAME) {
                        let (schema, table_id) = self.resolve_table_id(args)?;
                        return self.bind_relation_by_id(table_id, schema, alias);
                    }
                    if let Ok(table_function_type) = TableFunctionType::from_str(func_name) {
                        let args: Vec<ExprImpl> = args
                            .into_iter()
                            .map(|arg| self.bind_function_arg(arg))
                            .flatten_ok()
                            .try_collect()?;
                        let tf = TableFunction::new(table_function_type, args)?;
                        let columns = [(
                            false,
                            Field {
                                data_type: tf.return_type(),
                                name: tf.function_type.name().to_string(),
                                sub_fields: vec![],
                                type_name: "".to_string(),
                            },
                        )]
                        .into_iter();

                        self.bind_table_to_context(
                            columns,
                            tf.function_type.name().to_string(),
                            alias,
                        )?;

                        return Ok(Relation::TableFunction(Box::new(tf)));
                    }
                    let kind = WindowTableFunctionKind::from_str(func_name).map_err(|_| {
                        ErrorCode::NotImplemented(
                            format!("unknown table function kind: {}", name.0[0].value),
                            1191.into(),
                        )
                    })?;
                    Ok(Relation::WindowTableFunction(Box::new(
                        self.bind_window_table_function(alias, kind, args)?,
                    )))
                }
            }
            TableFactor::Derived {
                lateral,
                subquery,
                alias,
            } => {
                if lateral {
                    // If we detect a lateral, we mark the lateral context as visible.
                    self.try_mark_lateral_as_visible();

                    // Bind lateral subquery here.

                    // Mark the lateral context as invisible once again.
                    self.try_mark_lateral_as_invisible();
                    Err(ErrorCode::NotImplemented(
                        "lateral subqueries are not yet supported".into(),
                        Some(3815).into(),
                    )
                    .into())
                } else {
                    // Non-lateral subqueries to not have access to the join-tree context.
                    self.push_lateral_context();
                    let bound_subquery = self.bind_subquery_relation(*subquery, alias)?;
                    self.pop_and_merge_lateral_context()?;
                    Ok(Relation::Subquery(Box::new(bound_subquery)))
                }
            }
            TableFactor::NestedJoin(table_with_joins) => {
                self.push_lateral_context();
                let bound_join = self.bind_table_with_joins(*table_with_joins)?;
                self.pop_and_merge_lateral_context()?;
                Ok(bound_join)
            }

            // TODO: if and when we allow nested joins (binding table factors which are themselves
            // joins), We need to `self.push_table_context()` prior to binding the join and
            // `self.pop_and_merge_table_context()` after. This ensures that the nested join's
            // `BindContext` references only the columns that are visible to it.
            _ => Err(ErrorCode::NotImplemented(
                format!("unsupported table factor {:?}", table_factor),
                None.into(),
            )
            .into()),
        }
    }
}

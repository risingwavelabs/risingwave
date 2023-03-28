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

use std::collections::hash_map::Entry;
use std::ops::Deref;
use std::str::FromStr;

use itertools::Itertools;
use risingwave_common::catalog::{
    Field, Schema, TableId, DEFAULT_SCHEMA_NAME, PG_CATALOG_SCHEMA_NAME,
    RW_INTERNAL_TABLE_FUNCTION_NAME,
};
use risingwave_common::error::{internal_error, ErrorCode, Result, RwError};
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{
    Expr as ParserExpr, FunctionArg, FunctionArgExpr, Ident, ObjectName, TableAlias, TableFactor,
};

use self::watermark::is_watermark_func;
use super::bind_context::ColumnBinding;
use super::statement::RewriteExprsRecursive;
use crate::binder::Binder;
use crate::catalog::function_catalog::FunctionKind;
use crate::catalog::system_catalog::pg_catalog::{
    PG_GET_KEYWORDS_FUNC_NAME, PG_KEYWORDS_TABLE_NAME,
};
use crate::expr::{Expr, ExprImpl, InputRef, TableFunction, TableFunctionType};

mod join;
mod share;
mod subquery;
mod table_or_source;
mod watermark;
mod window_table_function;

pub use join::BoundJoin;
pub use share::BoundShare;
pub use subquery::BoundSubquery;
pub use table_or_source::{BoundBaseTable, BoundSource, BoundSystemTable};
pub use watermark::BoundWatermark;
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
    Watermark(Box<BoundWatermark>),
    Share(Box<BoundShare>),
}

impl RewriteExprsRecursive for Relation {
    fn rewrite_exprs_recursive(&mut self, rewriter: &mut impl crate::expr::ExprRewriter) {
        match self {
            Relation::Subquery(inner) => inner.rewrite_exprs_recursive(rewriter),
            Relation::Join(inner) => inner.rewrite_exprs_recursive(rewriter),
            Relation::WindowTableFunction(inner) => inner.rewrite_exprs_recursive(rewriter),
            Relation::Watermark(inner) => inner.rewrite_exprs_recursive(rewriter),
            Relation::Share(inner) => inner.rewrite_exprs_recursive(rewriter),
            Relation::TableFunction(inner) => {
                let new_args = std::mem::take(&mut inner.args)
                    .into_iter()
                    .map(|expr| rewriter.rewrite_expr(expr))
                    .collect();
                inner.args = new_args;
            }
            _ => {}
        }
    }
}

impl Relation {
    pub fn contains_sys_table(&self) -> bool {
        match self {
            Relation::SystemTable(_) => true,
            Relation::Subquery(s) => s.query.contains_sys_table(),
            Relation::Join(j) => j.left.contains_sys_table() || j.right.contains_sys_table(),
            Relation::Share(s) => s.input.contains_sys_table(),
            _ => false,
        }
    }

    pub fn is_correlated(&self, depth: Depth) -> bool {
        match self {
            Relation::Subquery(subquery) => subquery.query.is_correlated(depth),
            Relation::Join(join) => {
                join.cond.has_correlated_input_ref_by_depth(depth)
                    || join.left.is_correlated(depth)
                    || join.right.is_correlated(depth)
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
    /// return (`schema_name`, `name`)
    pub fn resolve_schema_qualified_name(
        db_name: &str,
        name: ObjectName,
    ) -> Result<(Option<String>, String)> {
        let mut indentifiers = name.0;

        if indentifiers.len() > 3 {
            return Err(internal_error(
                "schema qualified name can contain at most 3 arguments".to_string(),
            ));
        }

        let name = indentifiers.pop().unwrap().real_value();

        let schema_name = indentifiers.pop().map(|ident| ident.real_value());
        let database_name = indentifiers.pop().map(|ident| ident.real_value());

        if let Some(database_name) = database_name && database_name != db_name {
            return Err(internal_error(format!(
                "database in schema qualified name {}.{}.{} is not equal to current database name {}",
                database_name, schema_name.unwrap(), name, db_name
            )));
        }

        Ok((schema_name, name))
    }

    /// return first name in identifiers, must have only one name.
    fn resolve_single_name(mut identifiers: Vec<Ident>, ident_desc: &str) -> Result<String> {
        if identifiers.len() > 1 {
            return Err(internal_error(format!(
                "{} must contain 1 argument",
                ident_desc
            )));
        }
        let name = identifiers.pop().unwrap().real_value();

        Ok(name)
    }

    /// return the `database_name`
    pub fn resolve_database_name(name: ObjectName) -> Result<String> {
        Self::resolve_single_name(name.0, "database name")
    }

    /// return the `schema_name`
    pub fn resolve_schema_name(name: ObjectName) -> Result<String> {
        Self::resolve_single_name(name.0, "schema name")
    }

    /// return the `index_name`
    pub fn resolve_index_name(name: ObjectName) -> Result<String> {
        Self::resolve_single_name(name.0, "index name")
    }

    /// return the `view_name`
    pub fn resolve_view_name(name: ObjectName) -> Result<String> {
        Self::resolve_single_name(name.0, "view name")
    }

    /// return the `sink_name`
    pub fn resolve_sink_name(name: ObjectName) -> Result<String> {
        Self::resolve_single_name(name.0, "sink name")
    }

    /// return the `table_name`
    pub fn resolve_table_name(name: ObjectName) -> Result<String> {
        Self::resolve_single_name(name.0, "table name")
    }

    /// return the `user_name`
    pub fn resolve_user_name(name: ObjectName) -> Result<String> {
        Self::resolve_single_name(name.0, "user name")
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
                    .map(|t| t.real_value())
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

    /// Binds a relation, which can be:
    /// - a table/source/materialized view
    /// - a reference to a CTE
    /// - a logical view
    pub fn bind_relation_by_name(
        &mut self,
        name: ObjectName,
        alias: Option<TableAlias>,
        for_system_time_as_of_now: bool,
    ) -> Result<Relation> {
        let (schema_name, table_name) = Self::resolve_schema_qualified_name(&self.db_name, name)?;
        if schema_name.is_none() && let Some(item) = self.context.cte_to_relation.get(&table_name) {
            // Handles CTE

            let (share_id, query, mut original_alias) = item.deref().clone();
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
                table_name.clone(),
                Some(original_alias),
            )?;

            // Share the CTE.
            let input_relation = Relation::Subquery(Box::new(BoundSubquery { query }));
            let share_relation = Relation::Share(Box::new(BoundShare { share_id, input: input_relation }));
            Ok(share_relation)
        } else {

            self.bind_relation_by_name_inner(schema_name.as_deref(), &table_name, alias, for_system_time_as_of_now)
        }
    }

    // Bind a relation provided a function arg.
    fn bind_relation_by_function_arg(
        &mut self,
        arg: Option<FunctionArg>,
        err_msg: &str,
    ) -> Result<(Relation, ObjectName)> {
        let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))) = arg else {
            return Err(ErrorCode::BindError(err_msg.to_string()).into());
        };
        let table_name = match expr {
            ParserExpr::Identifier(ident) => Ok::<_, RwError>(ObjectName(vec![ident])),
            ParserExpr::CompoundIdentifier(idents) => Ok(ObjectName(idents)),
            _ => Err(ErrorCode::BindError(err_msg.to_string()).into()),
        }?;

        Ok((
            self.bind_relation_by_name(table_name.clone(), None, false)?,
            table_name,
        ))
    }

    // Bind column provided a function arg.
    fn bind_column_by_function_args(
        &mut self,
        arg: Option<FunctionArg>,
        err_msg: &str,
    ) -> Result<Box<InputRef>> {
        if let Some(time_col_arg) = arg
          && let Some(ExprImpl::InputRef(time_col)) = self.bind_function_arg(time_col_arg)?.into_iter().next() {
            Ok(time_col)
        } else {
            Err(ErrorCode::BindError(err_msg.to_string()).into())
        }
    }

    /// `rw_table(table_id[,schema_name])` which queries internal table
    fn bind_internal_table(
        &mut self,
        args: Vec<FunctionArg>,
        alias: Option<TableAlias>,
    ) -> Result<Relation> {
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

        let table_name = self.catalog.get_table_name_by_id(table_id)?;
        self.bind_relation_by_name_inner(Some(&schema), &table_name, alias, false)
    }

    pub(super) fn bind_table_factor(&mut self, table_factor: TableFactor) -> Result<Relation> {
        match table_factor {
            TableFactor::Table {
                name,
                alias,
                for_system_time_as_of_now,
            } => self.bind_relation_by_name(name, alias, for_system_time_as_of_now),
            TableFactor::TableFunction { name, alias, args } => {
                let func_name = &name.0[0].real_value();
                if func_name.eq_ignore_ascii_case(RW_INTERNAL_TABLE_FUNCTION_NAME) {
                    return self.bind_internal_table(args, alias);
                }
                if func_name.eq_ignore_ascii_case(PG_GET_KEYWORDS_FUNC_NAME)
                    || name.real_value().eq_ignore_ascii_case(
                        format!("{}.{}", PG_CATALOG_SCHEMA_NAME, PG_GET_KEYWORDS_FUNC_NAME)
                            .as_str(),
                    )
                {
                    return self.bind_relation_by_name_inner(
                        Some(PG_CATALOG_SCHEMA_NAME),
                        PG_KEYWORDS_TABLE_NAME,
                        alias,
                        false,
                    );
                }
                if let Ok(kind) = WindowTableFunctionKind::from_str(func_name) {
                    return Ok(Relation::WindowTableFunction(Box::new(
                        self.bind_window_table_function(alias, kind, args)?,
                    )));
                }
                if is_watermark_func(func_name) {
                    return Ok(Relation::Watermark(Box::new(
                        self.bind_watermark(alias, args)?,
                    )));
                };

                let args: Vec<ExprImpl> = args
                    .into_iter()
                    .map(|arg| self.bind_function_arg(arg))
                    .flatten_ok()
                    .try_collect()?;
                let tf = if let Some(func) = self
                    .catalog
                    .first_valid_schema(
                        &self.db_name,
                        &self.search_path,
                        &self.auth_context.user_name,
                    )?
                    .get_function_by_name_args(
                        func_name,
                        &args.iter().map(|arg| arg.return_type()).collect_vec(),
                    ) && matches!(func.kind, FunctionKind::Table { .. })
                {
                    TableFunction::new_user_defined(func.clone(), args)
                } else if let Ok(table_function_type) = TableFunctionType::from_str(func_name) {
                    TableFunction::new(table_function_type, args)?
                } else {
                    return Err(ErrorCode::NotImplemented(
                        format!("unknown table function: {}", func_name),
                        1191.into(),
                    )
                    .into());
                };
                let columns = if let DataType::Struct(s) = tf.return_type() {
                    let schema = Schema::from(&*s);
                    schema.fields.into_iter().map(|f| (false, f)).collect_vec()
                } else {
                    vec![(false, Field::with_name(tf.return_type(), tf.name()))]
                };

                self.bind_table_to_context(columns, tf.name().to_string(), alias)?;

                Ok(Relation::TableFunction(Box::new(tf)))
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
        }
    }
}

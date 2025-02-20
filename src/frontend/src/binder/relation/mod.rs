// Copyright 2025 RisingWave Labs
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

use either::Either;
use itertools::{EitherOrBoth, Itertools};
use risingwave_common::bail;
use risingwave_common::catalog::{Field, TableId};
use risingwave_sqlparser::ast::{
    AsOf, Expr as ParserExpr, FunctionArg, FunctionArgExpr, Ident, ObjectName, TableAlias,
    TableFactor,
};
use thiserror::Error;
use thiserror_ext::AsReport;

use super::bind_context::ColumnBinding;
use super::statement::RewriteExprsRecursive;
use crate::binder::bind_context::{BindingCte, BindingCteState};
use crate::binder::Binder;
use crate::error::{ErrorCode, Result, RwError};
use crate::expr::{ExprImpl, InputRef};

mod cte_ref;
mod join;
mod share;
mod subquery;
mod table_function;
mod table_or_source;
mod watermark;
mod window_table_function;

pub use cte_ref::BoundBackCteRef;
pub use join::BoundJoin;
pub use share::{BoundShare, BoundShareInput};
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
    Apply(Box<BoundJoin>),
    WindowTableFunction(Box<BoundWindowTableFunction>),
    /// Table function or scalar function.
    TableFunction {
        expr: ExprImpl,
        with_ordinality: bool,
    },
    Watermark(Box<BoundWatermark>),
    /// rcte is implicitly included in share
    Share(Box<BoundShare>),
    BackCteRef(Box<BoundBackCteRef>),
}

impl RewriteExprsRecursive for Relation {
    fn rewrite_exprs_recursive(&mut self, rewriter: &mut impl crate::expr::ExprRewriter) {
        match self {
            Relation::Subquery(inner) => inner.rewrite_exprs_recursive(rewriter),
            Relation::Join(inner) => inner.rewrite_exprs_recursive(rewriter),
            Relation::Apply(inner) => inner.rewrite_exprs_recursive(rewriter),
            Relation::WindowTableFunction(inner) => inner.rewrite_exprs_recursive(rewriter),
            Relation::Watermark(inner) => inner.rewrite_exprs_recursive(rewriter),
            Relation::Share(inner) => inner.rewrite_exprs_recursive(rewriter),
            Relation::TableFunction { expr: inner, .. } => {
                *inner = rewriter.rewrite_expr(inner.take())
            }
            Relation::BackCteRef(inner) => inner.rewrite_exprs_recursive(rewriter),
            _ => {}
        }
    }
}

impl Relation {
    pub fn is_correlated(&self, depth: Depth) -> bool {
        match self {
            Relation::Subquery(subquery) => subquery.query.is_correlated(depth),
            Relation::Join(join) | Relation::Apply(join) => {
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
                .collect_correlated_indices_by_depth_and_assign_id(depth, correlated_id),
            Relation::Join(join) | Relation::Apply(join) => {
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
            Relation::TableFunction {
                expr: table_function,
                with_ordinality: _,
            } => table_function
                .collect_correlated_indices_by_depth_and_assign_id(depth + 1, correlated_id),
            Relation::Share(share) => match &mut share.input {
                BoundShareInput::Query(query) => match query {
                    Either::Left(query) => query
                        .collect_correlated_indices_by_depth_and_assign_id(depth, correlated_id),
                    Either::Right(_) => vec![],
                },
                BoundShareInput::ChangeLog(change_log) => change_log
                    .collect_correlated_indices_by_depth_and_assign_id(depth, correlated_id),
            },
            _ => vec![],
        }
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub enum ResolveQualifiedNameErrorKind {
    QualifiedNameTooLong,
    NotCurrentDatabase,
}

#[derive(Debug, Error)]
pub struct ResolveQualifiedNameError {
    qualified: String,
    kind: ResolveQualifiedNameErrorKind,
}

impl std::fmt::Display for ResolveQualifiedNameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            ResolveQualifiedNameErrorKind::QualifiedNameTooLong => write!(
                f,
                "improper qualified name (too many dotted names): {}",
                self.qualified
            ),
            ResolveQualifiedNameErrorKind::NotCurrentDatabase => write!(
                f,
                "cross-database references are not implemented: \"{}\"",
                self.qualified
            ),
        }
    }
}

impl ResolveQualifiedNameError {
    pub fn new(qualified: String, kind: ResolveQualifiedNameErrorKind) -> Self {
        Self { qualified, kind }
    }
}

impl From<ResolveQualifiedNameError> for RwError {
    fn from(e: ResolveQualifiedNameError) -> Self {
        ErrorCode::InvalidInputSyntax(format!("{}", e.as_report())).into()
    }
}

impl Binder {
    /// return (`schema_name`, `name`)
    pub fn resolve_schema_qualified_name(
        db_name: &str,
        name: ObjectName,
    ) -> std::result::Result<(Option<String>, String), ResolveQualifiedNameError> {
        let formatted_name = name.to_string();
        let mut identifiers = name.0;

        if identifiers.len() > 3 {
            return Err(ResolveQualifiedNameError::new(
                formatted_name,
                ResolveQualifiedNameErrorKind::QualifiedNameTooLong,
            ));
        }

        let name = identifiers.pop().unwrap().real_value();

        let schema_name = identifiers.pop().map(|ident| ident.real_value());
        let database_name = identifiers.pop().map(|ident| ident.real_value());

        if let Some(database_name) = database_name
            && database_name != db_name
        {
            return Err(ResolveQualifiedNameError::new(
                formatted_name,
                ResolveQualifiedNameErrorKind::NotCurrentDatabase,
            ));
        }

        Ok((schema_name, name))
    }

    /// check whether the name is a cross-database reference
    pub fn validate_cross_db_reference(
        db_name: &str,
        name: &ObjectName,
    ) -> std::result::Result<(), ResolveQualifiedNameError> {
        let formatted_name = name.to_string();
        let identifiers = &name.0;
        if identifiers.len() > 3 {
            return Err(ResolveQualifiedNameError::new(
                formatted_name,
                ResolveQualifiedNameErrorKind::QualifiedNameTooLong,
            ));
        }

        if identifiers.len() == 3 && identifiers[0].real_value() != db_name {
            return Err(ResolveQualifiedNameError::new(
                formatted_name,
                ResolveQualifiedNameErrorKind::NotCurrentDatabase,
            ));
        }

        Ok(())
    }

    /// return (`database_name`, `schema_name`, `name`)
    pub fn resolve_db_schema_qualified_name(
        name: ObjectName,
    ) -> std::result::Result<(Option<String>, Option<String>, String), ResolveQualifiedNameError>
    {
        let formatted_name = name.to_string();
        let mut identifiers = name.0;

        if identifiers.len() > 3 {
            return Err(ResolveQualifiedNameError::new(
                formatted_name,
                ResolveQualifiedNameErrorKind::QualifiedNameTooLong,
            ));
        }

        let name = identifiers.pop().unwrap().real_value();
        let schema_name = identifiers.pop().map(|ident| ident.real_value());
        let database_name = identifiers.pop().map(|ident| ident.real_value());

        Ok((database_name, schema_name, name))
    }

    /// return first name in identifiers, must have only one name.
    fn resolve_single_name(mut identifiers: Vec<Ident>, ident_desc: &str) -> Result<String> {
        if identifiers.len() > 1 {
            bail!("{} must contain 1 argument", ident_desc);
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

    /// return the `subscription_name`
    pub fn resolve_subscription_name(name: ObjectName) -> Result<String> {
        Self::resolve_single_name(name.0, "subscription name")
    }

    /// return the `table_name`
    pub fn resolve_table_name(name: ObjectName) -> Result<String> {
        Self::resolve_single_name(name.0, "table name")
    }

    /// return the `source_name`
    pub fn resolve_source_name(name: ObjectName) -> Result<String> {
        Self::resolve_single_name(name.0, "source name")
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
                true => field.name.clone(),
                false => alias_iter
                    .next()
                    .map(|t| t.real_value())
                    .unwrap_or_else(|| field.name.clone()),
            };
            field.name.clone_from(&name);
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
        as_of: Option<AsOf>,
        allow_cross_db: bool,
    ) -> Result<Relation> {
        let (db_name, schema_name, table_name) = if allow_cross_db {
            Self::resolve_db_schema_qualified_name(name)?
        } else {
            let (schema_name, table_name) =
                Self::resolve_schema_qualified_name(&self.db_name, name)?;
            (None, schema_name, table_name)
        };

        if schema_name.is_none()
            // the `table_name` here is the name of the currently binding cte.
            && let Some(item) = self.context.cte_to_relation.get(&table_name)
        {
            // Handles CTE

            if as_of.is_some() {
                return Err(ErrorCode::BindError(
                    "Right table of a temporal join should not be a CTE. \
                 It should be a table, index, or materialized view"
                        .to_owned(),
                )
                .into());
            }

            let BindingCte {
                share_id,
                state: cte_state,
                alias: mut original_alias,
            } = item.deref().borrow().clone();

            // The original CTE alias ought to be its table name.
            debug_assert_eq!(original_alias.name.real_value(), table_name);

            if let Some(from_alias) = alias {
                original_alias.name = from_alias.name;
                original_alias.columns = original_alias
                    .columns
                    .into_iter()
                    .zip_longest(from_alias.columns)
                    .map(EitherOrBoth::into_right)
                    .collect();
            }

            match cte_state {
                BindingCteState::Init => {
                    Err(ErrorCode::BindError("Base term of recursive CTE not found, consider writing it to left side of the `UNION ALL` operator".to_owned()).into())
                }
                BindingCteState::BaseResolved { base } => {
                    self.bind_table_to_context(
                        base.schema().fields.iter().map(|f| (false, f.clone())),
                        table_name.clone(),
                        Some(original_alias),
                    )?;
                    Ok(Relation::BackCteRef(Box::new(BoundBackCteRef { share_id, base })))
                }
                BindingCteState::Bound { query } => {
                    let input = BoundShareInput::Query(query);
                    self.bind_table_to_context(
                        input.fields()?,
                        table_name.clone(),
                        Some(original_alias),
                    )?;
                    // we could always share the cte,
                    // no matter it's recursive or not.
                    Ok(Relation::Share(Box::new(BoundShare { share_id, input})))
                }
                BindingCteState::ChangeLog { table } => {
                    let input = BoundShareInput::ChangeLog(table);
                    self.bind_table_to_context(
                        input.fields()?,
                        table_name.clone(),
                        Some(original_alias),
                    )?;
                    Ok(Relation::Share(Box::new(BoundShare { share_id, input })))
                },
            }
        } else {
            self.bind_relation_by_name_inner(
                db_name.as_deref(),
                schema_name.as_deref(),
                &table_name,
                alias,
                as_of,
            )
        }
    }

    // Bind a relation provided a function arg.
    fn bind_relation_by_function_arg(
        &mut self,
        arg: Option<FunctionArg>,
        err_msg: &str,
    ) -> Result<(Relation, ObjectName)> {
        let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))) = arg else {
            return Err(ErrorCode::BindError(err_msg.to_owned()).into());
        };
        let table_name = match expr {
            ParserExpr::Identifier(ident) => Ok::<_, RwError>(ObjectName(vec![ident])),
            ParserExpr::CompoundIdentifier(idents) => Ok(ObjectName(idents)),
            _ => Err(ErrorCode::BindError(err_msg.to_owned()).into()),
        }?;

        Ok((
            self.bind_relation_by_name(table_name.clone(), None, None, true)?,
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
            && let Some(ExprImpl::InputRef(time_col)) =
                self.bind_function_arg(time_col_arg)?.into_iter().next()
        {
            Ok(time_col)
        } else {
            Err(ErrorCode::BindError(err_msg.to_owned()).into())
        }
    }

    /// `rw_table(table_id[,schema_name])` which queries internal table
    fn bind_internal_table(
        &mut self,
        args: Vec<FunctionArg>,
        alias: Option<TableAlias>,
    ) -> Result<Relation> {
        if args.is_empty() || args.len() > 2 {
            return Err(
                ErrorCode::BindError("usage: rw_table(table_id[,schema_name])".to_owned()).into(),
            );
        }

        let table_id: TableId = args[0]
            .to_string()
            .parse::<u32>()
            .map_err(|err| {
                RwError::from(ErrorCode::BindError(format!(
                    "invalid table id: {}",
                    err.as_report()
                )))
            })?
            .into();

        let schema = args.get(1).map(|arg| arg.to_string());

        let table_name = self.catalog.get_table_name_by_id(table_id)?;
        self.bind_relation_by_name_inner(None, schema.as_deref(), &table_name, alias, None)
    }

    pub(super) fn bind_table_factor(&mut self, table_factor: TableFactor) -> Result<Relation> {
        match table_factor {
            TableFactor::Table { name, alias, as_of } => {
                self.bind_relation_by_name(name, alias, as_of, true)
            }
            TableFactor::TableFunction {
                name,
                alias,
                args,
                with_ordinality,
            } => {
                self.try_mark_lateral_as_visible();
                let result = self.bind_table_function(name, alias, args, with_ordinality);
                self.try_mark_lateral_as_invisible();
                result
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
                    let bound_subquery = self.bind_subquery_relation(*subquery, alias, true)?;

                    // Mark the lateral context as invisible once again.
                    self.try_mark_lateral_as_invisible();
                    Ok(Relation::Subquery(Box::new(bound_subquery)))
                } else {
                    // Non-lateral subqueries to not have access to the join-tree context.
                    self.push_lateral_context();
                    let bound_subquery = self.bind_subquery_relation(*subquery, alias, false)?;
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

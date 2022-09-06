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

use std::fmt::Debug;

use itertools::Itertools;
use risingwave_common::catalog::{Field, Schema, PG_CATALOG_SCHEMA_NAME};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;
use risingwave_pb::expr::expr_node::Type;
use risingwave_sqlparser::ast::{Expr, Select, SelectItem};

use super::bind_context::{Clause, ColumnBinding};
use super::UNNAMED_COLUMN;
use crate::binder::{Binder, Relation};
use crate::catalog::check_valid_column_name;
use crate::catalog::pg_catalog::pg_user::{
    PG_USER_ID_INDEX, PG_USER_NAME_INDEX, PG_USER_TABLE_NAME,
};
use crate::expr::{
    CorrelatedId, CorrelatedInputRef, Depth, Expr as _, ExprImpl, ExprType, FunctionCall, InputRef,
};

#[derive(Debug, Clone)]
pub struct BoundSelect {
    pub distinct: bool,
    pub select_items: Vec<ExprImpl>,
    pub aliases: Vec<Option<String>>,
    pub from: Option<Relation>,
    pub where_clause: Option<ExprImpl>,
    pub group_by: Vec<ExprImpl>,
    pub having: Option<ExprImpl>,
    schema: Schema,
}

impl BoundSelect {
    /// The schema returned by this [`BoundSelect`].
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn exprs(&self) -> impl Iterator<Item = &ExprImpl> {
        self.select_items
            .iter()
            .chain(self.group_by.iter())
            .chain(self.where_clause.iter())
            .chain(self.having.iter())
    }

    pub fn exprs_mut(&mut self) -> impl Iterator<Item = &mut ExprImpl> {
        self.select_items
            .iter_mut()
            .chain(self.group_by.iter_mut())
            .chain(self.where_clause.iter_mut())
        // TODO: uncomment `having` below after #4850 is fixed
        // .chain(self.having.iter_mut())
    }

    pub fn is_correlated(&self) -> bool {
        self.exprs()
            .any(|expr| expr.has_correlated_input_ref_by_depth())
            || match self.from.as_ref() {
                Some(relation) => relation.is_correlated(),
                None => false,
            }
    }

    pub fn collect_correlated_indices_by_depth_and_assign_id(
        &mut self,
        depth: Depth,
        correlated_id: CorrelatedId,
    ) -> Vec<usize> {
        let mut correlated_indices = self
            .exprs_mut()
            .flat_map(|expr| {
                expr.collect_correlated_indices_by_depth_and_assign_id(depth, correlated_id)
            })
            .collect_vec();

        if let Some(relation) = self.from.as_mut() {
            correlated_indices.extend(
                relation.collect_correlated_indices_by_depth_and_assign_id(depth, correlated_id),
            );
        }

        correlated_indices
    }
}

impl Binder {
    pub(super) fn bind_select(&mut self, select: Select) -> Result<BoundSelect> {
        // Bind FROM clause.
        let from = self.bind_vec_table_with_joins(select.from)?;

        // Bind SELECT clause.
        let (select_items, aliases) = self.bind_select_list(select.projection)?;

        // Bind WHERE clause.
        self.context.clause = Some(Clause::Where);
        let selection = select
            .selection
            .map(|expr| self.bind_expr(expr))
            .transpose()?;
        self.context.clause = None;

        Self::require_bool_clause(&selection, "WHERE")?;

        // Bind GROUP BY clause.
        self.context.clause = Some(Clause::GroupBy);
        let group_by = select
            .group_by
            .into_iter()
            .map(|expr| self.bind_expr(expr))
            .try_collect()?;
        self.context.clause = None;

        // Bind HAVING clause.
        self.context.clause = Some(Clause::Having);
        let having = select.having.map(|expr| self.bind_expr(expr)).transpose()?;
        Self::require_bool_clause(&having, "HAVING")?;
        self.context.clause = None;

        // Store field from `ExprImpl` to support binding `field_desc` in `subquery`.
        let fields = select_items
            .iter()
            .zip_eq(aliases.iter())
            .map(|(s, a)| {
                let name = a.clone().unwrap_or_else(|| UNNAMED_COLUMN.to_string());
                Ok(Field::with_name(s.return_type(), name))
            })
            .collect::<Result<Vec<Field>>>()?;

        Ok(BoundSelect {
            distinct: select.distinct,
            select_items,
            aliases,
            from,
            where_clause: selection,
            group_by,
            having,
            schema: Schema { fields },
        })
    }

    pub fn bind_select_list(
        &mut self,
        select_items: Vec<SelectItem>,
    ) -> Result<(Vec<ExprImpl>, Vec<Option<String>>)> {
        let mut select_list = vec![];
        let mut aliases = vec![];
        for item in select_items {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let (select_expr, alias) = match &expr.clone() {
                        Expr::Identifier(ident) => {
                            (self.bind_expr(expr)?, Some(ident.real_value()))
                        }
                        Expr::CompoundIdentifier(idents) => (
                            self.bind_expr(expr)?,
                            idents.last().map(|ident| ident.real_value()),
                        ),
                        Expr::FieldIdentifier(field_expr, idents) => (
                            self.bind_single_field_column(*field_expr.clone(), idents)?,
                            idents.last().map(|ident| ident.real_value()),
                        ),
                        _ => (self.bind_expr(expr)?, None),
                    };
                    select_list.push(select_expr);
                    aliases.push(alias);
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    check_valid_column_name(&alias.real_value())?;

                    let expr = self.bind_expr(expr)?;
                    select_list.push(expr);
                    aliases.push(Some(alias.real_value()));
                }
                SelectItem::QualifiedWildcard(obj_name) => {
                    let table_name = &obj_name.0.last().unwrap().real_value();
                    let (begin, end) = self.context.range_of.get(table_name).ok_or_else(|| {
                        ErrorCode::ItemNotFound(format!("relation \"{}\"", table_name))
                    })?;
                    let (exprs, names) = Self::iter_bound_columns(
                        self.context.columns[*begin..*end]
                            .iter()
                            .filter(|c| !c.is_hidden),
                    );
                    select_list.extend(exprs);
                    aliases.extend(names);
                }
                SelectItem::ExprQualifiedWildcard(expr, idents) => {
                    let (exprs, names) = self.bind_wildcard_field_column(expr, &idents.0)?;
                    select_list.extend(exprs);
                    aliases.extend(names);
                }
                SelectItem::Wildcard => {
                    if self.context.range_of.is_empty() {
                        return Err(ErrorCode::BindError(
                            "SELECT * with no tables specified is not valid".into(),
                        )
                        .into());
                    }
                    // Bind the column groups
                    // In psql, the USING and NATURAL columns come before the rest of the columns in
                    // a SELECT * statement
                    let (exprs, names) = self.iter_column_groups();
                    select_list.extend(exprs);
                    aliases.extend(names);

                    // Bind columns that are not in groups
                    let (exprs, names) =
                        Self::iter_bound_columns(self.context.columns[..].iter().filter(|c| {
                            !c.is_hidden
                                && !self
                                    .context
                                    .column_group_context
                                    .mapping
                                    .contains_key(&c.index)
                        }));
                    select_list.extend(exprs);
                    aliases.extend(names);

                    // TODO: we will need to be able to handle wildcard expressions bound to aliases
                    // in the future. We'd then need a `NaturalGroupContext`
                    // bound to each alias to correctly disambiguate column
                    // references
                    //
                    // We may need to refactor `NaturalGroupContext` to become span aware in that
                    // case.
                }
            }
        }
        Ok((select_list, aliases))
    }

    /// `bind_get_user_by_id_select` binds a select statement that returns a single user name by id,
    /// this is used for function `pg_catalog.get_user_by_id()`.
    pub fn bind_get_user_by_id_select(&mut self, input: &ExprImpl) -> Result<BoundSelect> {
        let select_items = vec![ExprImpl::InputRef(Box::new(InputRef::new(
            PG_USER_NAME_INDEX,
            DataType::Varchar,
        )))];
        let schema = Schema {
            fields: vec![Field::with_name(
                DataType::Varchar,
                UNNAMED_COLUMN.to_string(),
            )],
        };
        let input = match input {
            ExprImpl::InputRef(input_ref) => ExprImpl::CorrelatedInputRef(Box::new(
                CorrelatedInputRef::new(input_ref.index, input_ref.data_type.clone(), 1),
            )),
            ExprImpl::CorrelatedInputRef(col_input_ref) => {
                ExprImpl::CorrelatedInputRef(Box::new(CorrelatedInputRef::new(
                    col_input_ref.index(),
                    col_input_ref.return_type(),
                    col_input_ref.depth() + 1,
                )))
            }
            ExprImpl::Literal(_) => input.clone(),
            _ => unimplemented!("Unsupported input type"),
        };
        let from =
            Some(self.bind_table_or_source(PG_CATALOG_SCHEMA_NAME, PG_USER_TABLE_NAME, None)?);
        let where_clause = Some(ExprImpl::FunctionCall(Box::new(FunctionCall::new(
            Type::Equal,
            vec![
                input,
                ExprImpl::InputRef(Box::new(InputRef::new(PG_USER_ID_INDEX, DataType::Int32))),
            ],
        )?)));

        Ok(BoundSelect {
            distinct: false,
            select_items,
            aliases: vec![None],
            from,
            where_clause,
            group_by: vec![],
            having: None,
            schema,
        })
    }

    pub fn iter_bound_columns<'a>(
        column_binding: impl Iterator<Item = &'a ColumnBinding>,
    ) -> (Vec<ExprImpl>, Vec<Option<String>>) {
        column_binding
            .map(|c| {
                (
                    InputRef::new(c.index, c.field.data_type.clone()).into(),
                    Some(c.field.name.clone()),
                )
            })
            .unzip()
    }

    pub fn iter_column_groups(&self) -> (Vec<ExprImpl>, Vec<Option<String>>) {
        self.context
            .column_group_context
            .groups
            .values()
            .rev() // ensure that the outermost col group gets put first in the list
            .map(|g| {
                if let Some(col) = &g.non_nullable_column {
                    let c = &self.context.columns[*col];
                    (
                        InputRef::new(c.index, c.field.data_type.clone()).into(),
                        Some(c.field.name.clone()),
                    )
                } else {
                    let mut input_idxes = g.indices.iter().collect::<Vec<_>>();
                    input_idxes.sort();
                    let inputs = input_idxes
                        .into_iter()
                        .map(|index| {
                            let column = &self.context.columns[*index];
                            InputRef::new(column.index, column.field.data_type.clone()).into()
                        })
                        .collect::<Vec<_>>();
                    let c = &self.context.columns[*g.indices.iter().next().unwrap()];
                    (
                        FunctionCall::new(ExprType::Coalesce, inputs)
                            .expect("Failure binding COALESCE function call")
                            .into(),
                        Some(c.field.name.clone()),
                    )
                }
            })
            .unzip()
    }

    fn require_bool_clause(expr: &Option<ExprImpl>, clause: &str) -> Result<()> {
        if let Some(expr) = expr {
            let return_type = expr.return_type();
            if return_type != DataType::Boolean {
                return Err(ErrorCode::InternalError(format!(
                    "argument of {} must be boolean, not type {:?}",
                    clause, return_type
                ))
                .into());
            }
        }
        Ok(())
    }
}

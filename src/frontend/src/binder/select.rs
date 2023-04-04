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

use std::fmt::Debug;

use itertools::Itertools;
use risingwave_common::catalog::{Field, Schema, PG_CATALOG_SCHEMA_NAME, RW_CATALOG_SCHEMA_NAME};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_sqlparser::ast::{DataType as AstDataType, Distinct, Expr, Select, SelectItem};

use super::bind_context::{Clause, ColumnBinding};
use super::statement::RewriteExprsRecursive;
use super::UNNAMED_COLUMN;
use crate::binder::{Binder, Relation};
use crate::catalog::check_valid_column_name;
use crate::catalog::system_catalog::pg_catalog::{
    PG_USER_ID_INDEX, PG_USER_NAME_INDEX, PG_USER_TABLE_NAME,
};
use crate::catalog::system_catalog::rw_catalog::{
    RW_TABLE_ID_INDEX, RW_TABLE_STATS_KEY_SIZE_INDEX, RW_TABLE_STATS_TABLE_NAME,
    RW_TABLE_STATS_VALUE_SIZE_INDEX,
};
use crate::expr::{
    CorrelatedId, CorrelatedInputRef, Depth, Expr as _, ExprImpl, ExprType, FunctionCall, InputRef,
    Literal,
};

#[derive(Debug, Clone)]
pub struct BoundSelect {
    pub distinct: BoundDistinct,
    pub select_items: Vec<ExprImpl>,
    pub aliases: Vec<Option<String>>,
    pub from: Option<Relation>,
    pub where_clause: Option<ExprImpl>,
    pub group_by: Vec<ExprImpl>,
    pub having: Option<ExprImpl>,
    schema: Schema,
}

impl RewriteExprsRecursive for BoundSelect {
    fn rewrite_exprs_recursive(&mut self, rewriter: &mut impl crate::expr::ExprRewriter) {
        self.distinct.rewrite_exprs_recursive(rewriter);

        let new_select_items = std::mem::take(&mut self.select_items)
            .into_iter()
            .map(|expr| rewriter.rewrite_expr(expr))
            .collect::<Vec<_>>();
        self.select_items = new_select_items;

        if let Some(from) = &mut self.from {
            from.rewrite_exprs_recursive(rewriter);
        }

        self.where_clause =
            std::mem::take(&mut self.where_clause).map(|expr| rewriter.rewrite_expr(expr));

        let new_group_by = std::mem::take(&mut self.group_by)
            .into_iter()
            .map(|expr| rewriter.rewrite_expr(expr))
            .collect::<Vec<_>>();
        self.group_by = new_group_by;

        self.having = std::mem::take(&mut self.having).map(|expr| rewriter.rewrite_expr(expr));
    }
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
            .chain(self.having.iter_mut())
    }

    pub fn is_correlated(&self, depth: Depth) -> bool {
        self.exprs()
            .any(|expr| expr.has_correlated_input_ref_by_depth(depth))
            || match self.from.as_ref() {
                Some(relation) => relation.is_correlated(depth),
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

#[derive(Debug, Clone)]
pub enum BoundDistinct {
    All,
    Distinct,
    DistinctOn(Vec<ExprImpl>),
}

impl RewriteExprsRecursive for BoundDistinct {
    fn rewrite_exprs_recursive(&mut self, rewriter: &mut impl crate::expr::ExprRewriter) {
        if let Self::DistinctOn(exprs) = self {
            let new_exprs = std::mem::take(exprs)
                .into_iter()
                .map(|expr| rewriter.rewrite_expr(expr))
                .collect::<Vec<_>>();
            exprs.extend(new_exprs);
        }
    }
}

impl BoundDistinct {
    pub const fn is_all(&self) -> bool {
        matches!(self, Self::All)
    }

    pub const fn is_distinct(&self) -> bool {
        matches!(self, Self::Distinct)
    }
}

impl Binder {
    pub(super) fn bind_select(&mut self, select: Select) -> Result<BoundSelect> {
        // Bind FROM clause.
        let from = self.bind_vec_table_with_joins(select.from)?;

        // Bind SELECT clause.
        let (select_items, aliases) = self.bind_select_list(select.projection)?;

        // Bind DISTINCT ON.
        let distinct = self.bind_distinct_on(select.distinct)?;

        // Bind WHERE clause.
        self.context.clause = Some(Clause::Where);
        let selection = select
            .selection
            .map(|expr| {
                self.bind_expr(expr)
                    .and_then(|expr| expr.enforce_bool_clause("WHERE"))
            })
            .transpose()?;
        self.context.clause = None;

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
        let having = select
            .having
            .map(|expr| {
                self.bind_expr(expr)
                    .and_then(|expr| expr.enforce_bool_clause("HAVING"))
            })
            .transpose()?;
        self.context.clause = None;

        // Store field from `ExprImpl` to support binding `field_desc` in `subquery`.
        let fields = select_items
            .iter()
            .zip_eq_fast(aliases.iter())
            .map(|(s, a)| {
                let name = a.clone().unwrap_or_else(|| UNNAMED_COLUMN.to_string());
                Ok(Field::with_name(s.return_type(), name))
            })
            .collect::<Result<Vec<Field>>>()?;

        Ok(BoundSelect {
            distinct,
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
                    let alias = derive_alias(&expr);
                    let bound = self.bind_expr(expr)?;
                    select_list.push(bound);
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
                SelectItem::ExprQualifiedWildcard(expr, prefix) => {
                    let (exprs, names) = self.bind_wildcard_field_column(expr, prefix)?;
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

    pub fn bind_returning_list(
        &mut self,
        returning_items: Vec<SelectItem>,
    ) -> Result<(Vec<ExprImpl>, Vec<Field>)> {
        let (returning_list, aliases) = self.bind_select_list(returning_items)?;
        if returning_list
            .iter()
            .any(|expr| expr.has_agg_call() || expr.has_window_function())
        {
            return Err(RwError::from(ErrorCode::BindError(
                "should not have agg/window in the `RETURNING` list".to_string(),
            )));
        }

        let fields = returning_list
            .iter()
            .zip_eq_fast(aliases.iter())
            .map(|(s, a)| {
                let name = a.clone().unwrap_or_else(|| UNNAMED_COLUMN.to_string());
                Ok::<Field, RwError>(Field::with_name(s.return_type(), name))
            })
            .try_collect()?;
        Ok((returning_list, fields))
    }

    /// `bind_get_user_by_id_select` binds a select statement that returns a single user name by id,
    /// this is used for function `pg_catalog.get_user_by_id()`.
    pub fn bind_get_user_by_id_select(&mut self, input: &ExprImpl) -> Result<BoundSelect> {
        let select_items = vec![InputRef::new(PG_USER_NAME_INDEX, DataType::Varchar).into()];
        let schema = Schema {
            fields: vec![Field::with_name(
                DataType::Varchar,
                UNNAMED_COLUMN.to_string(),
            )],
        };
        let input = match input {
            ExprImpl::InputRef(input_ref) => {
                CorrelatedInputRef::new(input_ref.index(), input_ref.return_type(), 1).into()
            }
            ExprImpl::CorrelatedInputRef(col_input_ref) => CorrelatedInputRef::new(
                col_input_ref.index(),
                col_input_ref.return_type(),
                col_input_ref.depth() + 1,
            )
            .into(),
            ExprImpl::Literal(_) => input.clone(),
            _ => return Err(ErrorCode::BindError("Unsupported input type".to_string()).into()),
        };
        let from = Some(self.bind_relation_by_name_inner(
            Some(PG_CATALOG_SCHEMA_NAME),
            PG_USER_TABLE_NAME,
            None,
            false,
        )?);
        let where_clause = Some(
            FunctionCall::new(
                ExprType::Equal,
                vec![
                    input,
                    InputRef::new(PG_USER_ID_INDEX, DataType::Int32).into(),
                ],
            )?
            .into(),
        );

        Ok(BoundSelect {
            distinct: BoundDistinct::All,
            select_items,
            aliases: vec![None],
            from,
            where_clause,
            group_by: vec![],
            having: None,
            schema,
        })
    }

    pub fn bind_get_table_size_by_id_select(&mut self, input: &ExprImpl) -> Result<BoundSelect> {
        let table_name = input.as_literal().unwrap();
        let data = table_name.get_data().as_ref().unwrap().as_utf8();

        let table = self.bind_table(None, &data, None)?;
        let table_id = table.table_id;
        let input = ExprImpl::Literal(Box::new(Literal::new(
            Some(ScalarImpl::Int32(table_id.table_id as i32)),
            DataType::Int32,
        )));
        let key_value_size_sum = FunctionCall::new(
            ExprType::Add,
            vec![
                InputRef::new(RW_TABLE_STATS_KEY_SIZE_INDEX, DataType::Int64).into(),
                InputRef::new(RW_TABLE_STATS_VALUE_SIZE_INDEX, DataType::Int64).into(),
            ],
        )?
        .into();
        let select_items = vec![key_value_size_sum];

        // Is this the schema for the input to the query or the output of the query?
        let schema = Schema {
            fields: vec![Field::with_name(
                DataType::Int64,
                UNNAMED_COLUMN.to_string(),
            )],
        };
        // define the output schema
        let from = Some(self.bind_relation_by_name_inner(
            Some(RW_CATALOG_SCHEMA_NAME),
            RW_TABLE_STATS_TABLE_NAME,
            None,
            false,
        )?);

        // Define the where clause
        let where_clause = Some(
            FunctionCall::new(
                ExprType::Equal,
                vec![
                    input,
                    InputRef::new(RW_TABLE_ID_INDEX, DataType::Int32).into(),
                ],
            )?
            .into(),
        );

        Ok(BoundSelect {
            distinct: BoundDistinct::All,
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

    fn bind_distinct_on(&mut self, distinct: Distinct) -> Result<BoundDistinct> {
        Ok(match distinct {
            Distinct::All => BoundDistinct::All,
            Distinct::Distinct => BoundDistinct::Distinct,
            Distinct::DistinctOn(exprs) => {
                let mut bound_exprs = vec![];
                for expr in exprs {
                    bound_exprs.push(self.bind_expr(expr)?);
                }
                BoundDistinct::DistinctOn(bound_exprs)
            }
        })
    }
}

fn derive_alias(expr: &Expr) -> Option<String> {
    match expr.clone() {
        Expr::Identifier(ident) => Some(ident.real_value()),
        Expr::CompoundIdentifier(idents) => idents.last().map(|ident| ident.real_value()),
        Expr::FieldIdentifier(_, idents) => idents.last().map(|ident| ident.real_value()),
        Expr::Function(func) => Some(func.name.real_value()),
        Expr::Extract { .. } => Some("extract".to_string()),
        Expr::Case { .. } => Some("case".to_string()),
        Expr::Cast { expr, data_type } => {
            derive_alias(&expr).or_else(|| data_type_to_alias(&data_type))
        }
        Expr::TypedString { data_type, .. } => data_type_to_alias(&data_type),
        Expr::Value(risingwave_sqlparser::ast::Value::Interval { .. }) => {
            Some("interval".to_string())
        }
        Expr::Row(_) => Some("row".to_string()),
        Expr::Array(_) => Some("array".to_string()),
        Expr::ArrayIndex { obj, index: _ } => derive_alias(&obj),
        _ => None,
    }
}

fn data_type_to_alias(data_type: &AstDataType) -> Option<String> {
    let alias = match data_type {
        AstDataType::Char(_) => "bpchar".to_string(),
        AstDataType::Varchar => "varchar".to_string(),
        AstDataType::Uuid => "uuid".to_string(),
        AstDataType::Decimal(_, _) => "numeric".to_string(),
        AstDataType::Real | AstDataType::Float(Some(1..=24)) => "float4".to_string(),
        AstDataType::Double | AstDataType::Float(Some(25..=53) | None) => "float8".to_string(),
        AstDataType::Float(Some(0 | 54..)) => unreachable!(),
        AstDataType::SmallInt => "int2".to_string(),
        AstDataType::Int => "int4".to_string(),
        AstDataType::BigInt => "int8".to_string(),
        AstDataType::Boolean => "bool".to_string(),
        AstDataType::Date => "date".to_string(),
        AstDataType::Time(tz) => format!("time{}", if *tz { "z" } else { "" }),
        AstDataType::Timestamp(tz) => {
            format!("timestamp{}", if *tz { "tz" } else { "" })
        }
        AstDataType::Interval => "interval".to_string(),
        AstDataType::Regclass => "regclass".to_string(),
        AstDataType::Text => "text".to_string(),
        AstDataType::Bytea => "bytea".to_string(),
        AstDataType::Array(ty) => return data_type_to_alias(ty),
        AstDataType::Custom(ty) => format!("{}", ty),
        AstDataType::Struct(_) => {
            // Note: Postgres doesn't have anonymous structs
            return None;
        }
    };

    Some(alias)
}

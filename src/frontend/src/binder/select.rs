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
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{Expr, Ident, Select, SelectItem};

use super::bind_context::{Clause, ColumnBinding};
use super::UNNAMED_COLUMN;
use crate::binder::{Binder, Relation};
use crate::catalog::check_valid_column_name;
use crate::expr::{Expr as _, ExprImpl, InputRef};

#[derive(Debug)]
pub struct BoundSelect {
    pub distinct: bool,
    pub select_items: Vec<ExprImpl>,
    pub aliases: Vec<Option<String>>,
    pub from: Option<Relation>,
    pub where_clause: Option<ExprImpl>,
    pub group_by: Vec<ExprImpl>,
}

impl BoundSelect {
    /// The names returned by this [`BoundSelect`].
    pub fn names(&self) -> Vec<String> {
        self.aliases
            .iter()
            .cloned()
            .map(|alias| alias.unwrap_or_else(|| UNNAMED_COLUMN.to_string()))
            .collect()
    }

    /// The types returned by this [`BoundSelect`].
    pub fn data_types(&self) -> Vec<DataType> {
        self.select_items
            .iter()
            .map(|item| item.return_type())
            .collect()
    }

    pub fn is_correlated(&self) -> bool {
        self.select_items
            .iter()
            .chain(self.group_by.iter())
            .chain(self.where_clause.iter())
            .any(|expr| expr.has_correlated_input_ref())
    }
}

impl Binder {
    pub(super) fn bind_select(&mut self, select: Select) -> Result<BoundSelect> {
        // Bind FROM clause.
        let from = self.bind_vec_table_with_joins(select.from)?;

        // Bind WHERE clause.
        self.context.clause = Some(Clause::Where);
        let selection = select
            .selection
            .map(|expr| self.bind_expr(expr))
            .transpose()?;
        self.context.clause = None;

        if let Some(selection) = &selection {
            let return_type = selection.return_type();
            if return_type != DataType::Boolean {
                return Err(ErrorCode::InternalError(format!(
                    "argument of WHERE must be boolean, not type {:?}",
                    return_type
                ))
                .into());
            }
        }

        // Bind GROUP BY clause.
        let group_by = select
            .group_by
            .into_iter()
            .map(|expr| self.bind_expr(expr))
            .try_collect()?;

        // Bind SELECT clause.
        let (select_items, aliases) = self.bind_project(select.projection)?;

        Ok(BoundSelect {
            distinct: select.distinct,
            select_items,
            aliases,
            from,
            where_clause: selection,
            group_by,
        })
    }

    pub fn bind_project(
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
                            (self.bind_expr(expr)?, Some(ident.value.clone()))
                        }
                        Expr::CompoundIdentifier(idents) => (
                            self.bind_expr(expr)?,
                            idents.last().map(|ident| ident.value.clone()),
                        ),
                        Expr::FieldIdentifier(field_expr, idents) => {
                            self.bind_single_field_column(*field_expr.clone(), idents)?
                        }
                        _ => (self.bind_expr(expr)?, None),
                    };
                    select_list.push(select_expr);
                    aliases.push(alias);
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    check_valid_column_name(&alias.value)?;

                    let expr = self.bind_expr(expr)?;
                    select_list.push(expr);
                    aliases.push(Some(alias.value));
                }
                SelectItem::QualifiedWildcard(obj_name) => {
                    let table_name = &obj_name.0.last().unwrap().value;
                    let (begin, end) = self.context.range_of.get(table_name).ok_or_else(|| {
                        ErrorCode::ItemNotFound(format!("relation \"{}\"", table_name))
                    })?;
                    let (exprs, names) = Self::bind_columns_iter(
                        self.context.columns[*begin..*end]
                            .iter()
                            .filter(|c| !c.desc.is_nested),
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
                    let (exprs, names) = Self::bind_columns_iter(
                        self.context.columns[..]
                            .iter()
                            .filter(|c| !c.desc.is_nested)
                            .filter(|c| !c.is_hidden),
                    );
                    select_list.extend(exprs);
                    aliases.extend(names);
                }
            }
        }
        Ok((select_list, aliases))
    }

    /// This function will accept three expr type: `CompoundIdentifier`,`Identifier`,`Cast(Todo)`,
    /// will extract idents from expr and concat it with `ids` to get full `field_name`.
    /// Will return `table_name` and `field_name` or `field_name`.
    pub fn extract_table_and_field(
        &mut self,
        expr: Expr,
        ids: Vec<Ident>,
    ) -> Result<(Option<String>, String)> {
        match expr {
            // For CompoundIdentifier, we will use first ident as `table_name` and second ident as
            // first part of `column_name` and then concat with `ids`.
            Expr::CompoundIdentifier(idents) => {
                let (table_name, column): (&String, &String) = match &idents[..] {
                    [table, column] => (&table.value, &column.value),
                    _ => {
                        return Err(ErrorCode::InternalError(format!(
                            "Too many idents: {:?}",
                            idents
                        ))
                        .into());
                    }
                };
                Ok((
                    Some(table_name.clone()),
                    Self::concat_ident_names(column.clone(), &ids),
                ))
            }
            // For Identifier, we will first use the ident as first part of `column_name`.
            // If this `column_name` is exist and have only one index, return None
            // and full field name.
            // If not we will use the ident as table name and return concat `ids` as `field_name`.
            // For example, if a table name v3 have a column name v3 which
            // have a field name v3. Select (v3).v3 from v3 will return the field value instead
            // of column value.
            Expr::Identifier(ident) => {
                match self.context.indexs_of.get(&ident.value) {
                    Some(indexs) => {
                        if indexs.len() == 1 {
                            return Ok((None, Self::concat_ident_names(ident.value, &ids)));
                        }
                    }
                    None => {}
                }
                Ok((
                    Some(ident.value),
                    Self::concat_ident_names(ids[0].value.clone(), &ids[1..]),
                ))
            }
            Expr::Cast { .. } => {
                todo!()
            }
            _ => unreachable!(),
        }
    }

    /// Bind wildcard field column, e.g. `(table.v1).*`,
    /// will return vector of `InputRef` and full `field_name` as alias.
    pub fn bind_wildcard_field_column(
        &mut self,
        expr: Expr,
        idents: &[Ident],
    ) -> Result<(Vec<ExprImpl>, Vec<Option<String>>)> {
        // Extract `table_name` and `field_name` from expr and idents.
        let (table, field_name) = self.extract_table_and_field(expr, idents.to_vec())?;
        let desc = self
            .context
            .get_column_binding(table.clone(), &field_name)?
            .desc
            .clone();
        let bindings: Vec<&ColumnBinding> = desc
            .field_descs
            .iter()
            .map(|f| {
                self.context
                    .get_column_binding(table.clone(), &(field_name.clone() + "." + &f.name))
            })
            .collect::<Result<_>>()?;
        Ok(Self::bind_columns_iter(bindings.into_iter()))
    }

    /// Bind single field column, e.g. `(table.v1).v2`,
    /// will return `InputRef` and full `field_name` as alias.
    pub fn bind_single_field_column(
        &mut self,
        expr: Expr,
        idents: &[Ident],
    ) -> Result<(ExprImpl, Option<String>)> {
        // Extract `table_name` and `field_name` from expr and idents.
        let (table, field_name) = self.extract_table_and_field(expr, idents.to_vec())?;
        let binding = self.context.get_column_binding(table, &field_name)?;
        Ok((
            InputRef::new(binding.index, binding.desc.data_type.clone()).into(),
            Some(field_name),
        ))
    }

    /// Concat `column_name` with idents value.
    pub fn concat_ident_names(mut column_name: String, idents: &[Ident]) -> String {
        for name in idents {
            column_name = column_name + "." + &name.value;
        }
        column_name
    }

    /// Bind columns from `&column_binding` iterator.
    pub fn bind_columns_iter<'a>(
        column_binding: impl Iterator<Item = &'a ColumnBinding>,
    ) -> (Vec<ExprImpl>, Vec<Option<String>>) {
        column_binding
            .map(|c| {
                (
                    InputRef::new(c.index, c.desc.data_type.clone()).into(),
                    Some(c.desc.name.clone()),
                )
            })
            .unzip()
    }
}

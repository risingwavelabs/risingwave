use std::fmt::Debug;

use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{Expr, Select, SelectItem};

use super::bind_context::{Clause, ColumnBinding};
use super::UNNAMED_COLUMN;
use crate::binder::{Binder, TableRef};
use crate::expr::{Expr as _, ExprImpl, InputRef};

#[derive(Debug)]
pub struct BoundSelect {
    pub distinct: bool,
    pub select_items: Vec<ExprImpl>,
    pub aliases: Vec<Option<String>>,
    pub from: Option<TableRef>,
    pub selection: Option<ExprImpl>,
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
}

impl Binder {
    pub(super) fn bind_select(&mut self, select: Select) -> Result<BoundSelect> {
        let from = self.bind_vec_table_with_joins(select.from)?;

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
        let (select_items, aliases) = self.bind_project(select.projection)?;
        Ok(BoundSelect {
            distinct: select.distinct,
            select_items,
            aliases,
            from,
            selection,
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
                    let alias = match &expr {
                        Expr::Identifier(ident) => Some(ident.value.clone()),
                        Expr::CompoundIdentifier(idents) => {
                            idents.last().map(|ident| ident.value.clone())
                        }
                        _ => None,
                    };
                    let expr = self.bind_expr(expr)?;
                    select_list.push(expr);
                    aliases.push(alias);
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let expr = self.bind_expr(expr)?;
                    select_list.push(expr);
                    aliases.push(Some(alias.value));
                }
                SelectItem::QualifiedWildcard(obj_name) => {
                    let table_name = &obj_name.0.last().unwrap().value;
                    let (begin, end) = self.context.range_of.get(table_name).ok_or_else(|| {
                        ErrorCode::ItemNotFound(format!("relation \"{}\"", table_name))
                    })?;
                    let (exprs, names) = Self::bind_columns(&self.context.columns[*begin..*end])?;
                    select_list.extend(exprs);
                    aliases.extend(names);
                }
                SelectItem::Wildcard => {
                    let (exprs, names) = Self::bind_columns(&self.context.columns[..])?;
                    select_list.extend(exprs);
                    aliases.extend(names);
                }
            }
        }
        Ok((select_list, aliases))
    }

    pub fn bind_columns(columns: &[ColumnBinding]) -> Result<(Vec<ExprImpl>, Vec<Option<String>>)> {
        let bound_columns = columns
            .iter()
            .map(|column| {
                (
                    InputRef::new(column.index, column.data_type.clone()).into(),
                    Some(column.column_name.clone()),
                )
            })
            .unzip();
        Ok(bound_columns)
    }
}

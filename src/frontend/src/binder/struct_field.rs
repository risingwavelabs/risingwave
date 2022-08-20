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

use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::{DataType, Scalar};
use risingwave_sqlparser::ast::{Expr, Ident};

use crate::binder::Binder;
use crate::expr::{Expr as RwExpr, ExprImpl, ExprType, FunctionCall, InputRef, Literal};

impl Binder {
    /// This function will accept three expr type: `CompoundIdentifier`,`Identifier`,`Cast(Todo)`
    /// We will extract ident from `expr` to get the `column_binding`.
    /// Will return `column_binding` and field `idents`.
    fn bind_field_access(&mut self, expr: Expr, ids: Vec<Ident>) -> Result<(ExprImpl, Vec<Ident>)> {
        let (column_ident, ids) = match &expr {
            // For CompoundIdentifier, we will use the first ident as table name and the second
            // ident as column name.
            Expr::CompoundIdentifier(idents) => {
                let (table_name, column_name) = match &idents[..] {
                    [table, column] => (table.real_value(), column.real_value()),
                    _ => {
                        bail!("Too many idents: {:?}", idents);
                    }
                };
                (Some((Some(table_name), column_name)), ids)
            }
            // For Identifier, we will first use the ident as column name to get `column_desc`.
            // If column name does not exist, we will use the ident as table name.
            // The reason is that in pgsql, if table name v3 have a column name v3, which
            // has a field name v3. `SELECT (v3).v3 FROM v3` will return the field value instead
            // of column value.
            Expr::Identifier(ident) => match self.context.indices_of.get(&ident.real_value()) {
                Some(indices) => {
                    if indices.len() == 1 {
                        (Some((None, ident.real_value())), ids)
                    } else {
                        // When there are multiple tables having the same column, the table name
                        // must be explicitly specified.
                        let column_name = ids[0].real_value();
                        (
                            Some((Some(ident.real_value()), column_name)),
                            ids[1..].to_vec(),
                        )
                    }
                }
                None => {
                    let table_name = ident.real_value();
                    let column_name = ids[0].real_value();
                    (Some((Some(table_name), column_name)), ids[1..].to_vec())
                }
            },
            _ => (None, ids),
        };
        if let Some((table_name, column_name)) = column_ident {
            let index = self
                .context
                .get_column_binding_index(&table_name, &column_name)?;
            let binding = &self.context.columns[index];
            let expr = InputRef::new(binding.index, binding.field.data_type.clone()).into();
            Ok((expr, ids))
        } else {
            match expr {
                Expr::Cast { expr, data_type } => {
                    let cast = self.bind_cast(*expr, data_type)?;
                    Ok((cast, ids))
                }
                _ => unreachable!(),
            }
        }
    }

    /// Bind wildcard field column, e.g. `(table.v1).*`.
    /// Will return a vector of `Field(expr, int)` expressions and aliases.
    pub fn bind_wildcard_field_column(
        &mut self,
        expr: Expr,
        ids: &[Ident],
    ) -> Result<(Vec<ExprImpl>, Vec<Option<String>>)> {
        let (expr, idents) = self.bind_field_access(expr, ids.to_vec())?;
        let fields = Self::bind_field("".to_string(), expr, &idents, true)?;
        let exprs = fields.iter().map(|(e, _)| e.clone()).collect_vec();
        let names = fields.into_iter().map(|(_, s)| Some(s)).collect_vec();
        Ok((exprs, names))
    }

    /// Bind single field column, e.g. `(table.v1).v2`.
    /// Will return `Field(expr, int)` expression and the corresponding alias.
    /// `int` in the signagure of `Field` represents the field index.
    pub fn bind_single_field_column(&mut self, expr: Expr, ids: &[Ident]) -> Result<ExprImpl> {
        let (expr, idents) = self.bind_field_access(expr, ids.to_vec())?;
        let exprs = Self::bind_field("".to_string(), expr, &idents, false)?;
        Ok(exprs[0].clone().0)
    }

    /// Bind field in recursive way. It could return a couple Field expressions
    /// if it ends with a wildcard.
    fn bind_field(
        field_name: String,
        expr: ExprImpl,
        idents: &[Ident],
        wildcard: bool,
    ) -> Result<Vec<(ExprImpl, String)>> {
        match idents.get(0) {
            Some(ident) => {
                let field_name = ident.real_value();
                let (field_type, field_index) = find_field(expr.return_type(), ident.real_value())?;
                let expr = FunctionCall::new_unchecked(
                    ExprType::Field,
                    vec![
                        expr,
                        Literal::new(
                            Some((field_index as i32).to_scalar_value()),
                            DataType::Int32,
                        )
                        .into(),
                    ],
                    field_type,
                )
                .into();
                Self::bind_field(field_name, expr, &idents[1..], wildcard)
            }
            None => {
                if wildcard {
                    Self::bind_wildcard_field(expr)
                } else {
                    Ok(vec![(expr, field_name)])
                }
            }
        }
    }

    /// Will fail if it's an atomic value.
    /// Rewrite (expr:Struct).* to [Field(expr, 0), Field(expr, 1), ... Field(expr, n)].
    fn bind_wildcard_field(expr: ExprImpl) -> Result<Vec<(ExprImpl, String)>> {
        let input = expr.return_type();
        if let DataType::Struct(t) = input {
            Ok(t.fields
                .iter()
                .enumerate()
                .map(|(i, f)| {
                    (
                        FunctionCall::new_unchecked(
                            ExprType::Field,
                            vec![
                                expr.clone(),
                                Literal::new(Some((i as i32).to_scalar_value()), DataType::Int32)
                                    .into(),
                            ],
                            f.clone(),
                        )
                        .into(),
                        t.field_names[i].clone(),
                    )
                })
                .collect_vec())
        } else {
            Err(ErrorCode::BindError(format!("type \"{}\" is not composite", input)).into())
        }
    }
}

fn find_field(input: DataType, field_name: String) -> Result<(DataType, usize)> {
    if let DataType::Struct(t) = input {
        if let Some((pos, _)) = t.field_names.iter().find_position(|s| **s == field_name) {
            Ok((t.fields[pos].clone(), pos))
        } else {
            Err(ErrorCode::BindError(format!(
                "column \"{}\" not found in struct type",
                field_name
            ))
            .into())
        }
    } else {
        Err(ErrorCode::BindError(format!(
            "column notation .{} applied to type {}, which is not a composite type",
            field_name, input
        ))
        .into())
    }
}

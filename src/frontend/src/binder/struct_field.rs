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

use itertools::Itertools;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::{DataType, Scalar};
use risingwave_sqlparser::ast::{Expr, Ident};

use crate::binder::Binder;
use crate::expr::{Expr as RwExpr, ExprImpl, ExprType, FunctionCall, Literal};

impl Binder {
    /// Extracts and binds struct column from `expr`.
    /// Returns the bound column and the remaining fields.
    ///
    /// Specifically,
    /// - `(table).struct_col.fields` -> `(bound_struct_col, fields)`
    /// - Otherwise, `expr` corresponds to a column. `(expr).fields` -> `(bound_expr, fields)`
    fn extract_struct_column(
        &mut self,
        expr: Expr,
        field_idents: Vec<Ident>,
    ) -> Result<(ExprImpl, Vec<Ident>)> {
        let d = self.bind_expr(expr)?;
        Ok((d, field_idents))
    }

    /// Binds wildcard field column, e.g. `(table.v1).*` or `(table).v1.*`.
    ///
    /// Returns a vector of `Field(expr, int)` expressions and aliases.
    pub fn bind_wildcard_field_column(
        &mut self,
        expr: Expr,
        prefix: Vec<Ident>,
    ) -> Result<(Vec<ExprImpl>, Vec<Option<String>>)> {
        let (expr, idents) = self.extract_struct_column(expr, prefix)?;
        let fields = Self::bind_field("".to_string(), expr, &idents, true)?;
        let (exprs, names) = fields.into_iter().map(|(e, s)| (e, Some(s))).unzip();
        Ok((exprs, names))
    }

    /// Binds single field column, e.g. `(table.v1).v2` or `(table).v1.v2`.
    ///
    /// Returns a `Field(expr, int)` expression.
    pub fn bind_single_field_column(&mut self, expr: Expr, idents: &[Ident]) -> Result<ExprImpl> {
        let (expr, idents) = self.extract_struct_column(expr, idents.to_vec())?;
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
            Ok(t.iter()
                .enumerate()
                .map(|(i, (name, ty))| {
                    (
                        FunctionCall::new_unchecked(
                            ExprType::Field,
                            vec![
                                expr.clone(),
                                Literal::new(Some((i as i32).to_scalar_value()), DataType::Int32)
                                    .into(),
                            ],
                            ty.clone(),
                        )
                        .into(),
                        name.to_string(),
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
        if let Some((pos, (_, ty))) = t.iter().find_position(|(name, _)| name == &field_name) {
            Ok((ty.clone(), pos))
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

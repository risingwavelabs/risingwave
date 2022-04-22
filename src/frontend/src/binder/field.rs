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
use risingwave_common::catalog::ColumnDesc;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::{DataType, Scalar};
use risingwave_sqlparser::ast::{Expr, Ident};

use crate::binder::bind_context::ColumnBinding;
use crate::binder::Binder;
use crate::expr::{ExprImpl, ExprType, FunctionCall, InputRef, Literal};

impl Binder {
    /// This function will accept three expr type: `CompoundIdentifier`,`Identifier`,`Cast(Todo)`
    /// We will extract ident from `expr` to get the `column_binding`.
    /// Will return `column_binding` and field `idents`.
    fn extract_binding_and_idents(
        &mut self,
        expr: Expr,
        ids: Vec<Ident>,
    ) -> Result<(&ColumnBinding, Vec<Ident>)> {
        match expr {
            // For CompoundIdentifier, we will use first ident as table name and second ident as
            // column name to get `column_desc`.
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
                let index = self
                    .context
                    .get_column_binding_index(Some(table_name), column)?;
                Ok((&self.context.columns[index], ids))
            }
            // For Identifier, we will first use the ident as
            // column name to get `column_desc`.
            // If column name not exist, we will use the ident as table name.
            // The reason is that in pgsql, for table name v3 have a column name v3 which
            // have a field name v3. Select (v3).v3 from v3 will return the field value instead
            // of column value.
            Expr::Identifier(ident) => match self.context.indexs_of.get(&ident.value) {
                Some(indexs) => {
                    if indexs.len() == 1 {
                        let index = self.context.get_column_binding_index(None, &ident.value)?;
                        Ok((&self.context.columns[index], ids))
                    } else {
                        let column = &ids[0].value;
                        let index = self
                            .context
                            .get_column_binding_index(Some(&ident.value), column)?;
                        Ok((&self.context.columns[index], ids[1..].to_vec()))
                    }
                }
                None => {
                    let column = &ids[0].value;
                    let index = self
                        .context
                        .get_column_binding_index(Some(&ident.value), column)?;
                    Ok((&self.context.columns[index], ids[1..].to_vec()))
                }
            },
            Expr::Cast { .. } => {
                todo!()
            }
            _ => unreachable!(),
        }
    }

    /// Bind wildcard field column, e.g. `(table.v1).*`.
    /// Will return vector of Field type `FunctionCall` and alias.
    pub fn bind_wildcard_field_column(
        &mut self,
        expr: Expr,
        ids: &[Ident],
    ) -> Result<(Vec<ExprImpl>, Vec<Option<String>>)> {
        let (binding, idents) = self.extract_binding_and_idents(expr, ids.to_vec())?;
        let (exprs, column) = Self::bind_field(
            InputRef::new(binding.index, binding.desc.data_type.clone()).into(),
            &idents,
            binding.desc.clone(),
            true,
        )?;
        Ok((
            exprs,
            column
                .field_descs
                .iter()
                .map(|f| Some(f.name.clone()))
                .collect_vec(),
        ))
    }

    /// Bind single field column, e.g. `(table.v1).v2`.
    /// Will return Field type `FunctionCall` and alias.
    pub fn bind_single_field_column(&mut self, expr: Expr, ids: &[Ident]) -> Result<ExprImpl> {
        let (binding, idents) = self.extract_binding_and_idents(expr, ids.to_vec())?;
        let (exprs, _) = Self::bind_field(
            InputRef::new(binding.index, binding.desc.data_type.clone()).into(),
            &idents,
            binding.desc.clone(),
            false,
        )?;
        Ok(exprs[0].clone())
    }

    /// Bind field in recursive way, each field in the binding path will store as `literal` in
    /// exprs and return.
    /// `col` describes what `expr` contains.
    fn bind_field(
        expr: ExprImpl,
        idents: &[Ident],
        desc: ColumnDesc,
        wildcard: bool,
    ) -> Result<(Vec<ExprImpl>, ColumnDesc)> {
        match idents.get(0) {
            Some(ident) => {
                let (field, field_index) = desc.field(&ident.value)?;
                let expr = FunctionCall::new_with_return_type(
                    ExprType::Field,
                    vec![
                        expr,
                        Literal::new(Some(field_index.to_scalar_value()), DataType::Int32).into(),
                    ],
                    field.data_type.clone(),
                )
                .into();
                Self::bind_field(expr, &idents[1..], field, wildcard)
            }
            None => {
                if wildcard {
                    Self::bind_wildcard_field(expr, desc)
                } else {
                    Ok((vec![expr], desc))
                }
            }
        }
    }

    /// Will fail if it's an atomic value.
    /// Rewrite (expr:Struct).* to [Field(expr, 0), Field(expr, 1), ... Field(expr, n)].
    fn bind_wildcard_field(
        expr: ExprImpl,
        desc: ColumnDesc,
    ) -> Result<(Vec<ExprImpl>, ColumnDesc)> {
        if let DataType::Struct { .. } = desc.data_type {
            Ok((
                desc.field_descs
                    .iter()
                    .enumerate()
                    .map(|(i, f)| {
                        FunctionCall::new_with_return_type(
                            ExprType::Field,
                            vec![
                                expr.clone(),
                                Literal::new(Some((i as i32).to_scalar_value()), DataType::Int32)
                                    .into(),
                            ],
                            f.data_type.clone(),
                        )
                        .into()
                    })
                    .collect_vec(),
                desc,
            ))
        } else {
            Err(
                ErrorCode::BindError(format!("The field {} is not the nested column", desc.name))
                    .into(),
            )
        }
    }
}

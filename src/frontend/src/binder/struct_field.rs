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
use risingwave_common::catalog::Field;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::{DataType, Scalar, ScalarImpl};
use risingwave_expr::expr::AggKind;
use risingwave_sqlparser::ast::{Expr, Ident};

use crate::binder::bind_context::ColumnBinding;
use crate::binder::Binder;
use crate::expr::{
    AggCall, Expr as _, ExprImpl, ExprType, ExprVisitor, FunctionCall, InputRef, Literal,
};

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
    /// Will return a vector of `Field(expr, int)` expressions and aliases.
    pub fn bind_wildcard_field_column(
        &mut self,
        expr: Expr,
        ids: &[Ident],
    ) -> Result<(Vec<ExprImpl>, Vec<Option<String>>)> {
        let (binding, idents) = self.extract_binding_and_idents(expr, ids.to_vec())?;
        let (exprs, column) = Self::bind_field(
            InputRef::new(binding.index, binding.field.data_type.clone()).into(),
            &idents,
            binding.field.clone(),
            true,
        )?;
        Ok((
            exprs,
            column
                .sub_fields
                .iter()
                .map(|f| Some(f.name.clone()))
                .collect_vec(),
        ))
    }

    /// Bind single field column, e.g. `(table.v1).v2`.
    /// Will return `Field(expr, int)` expression and the corresponding alias.
    /// `int` in the signagure of `Field` represents the field index.
    pub fn bind_single_field_column(&mut self, expr: Expr, ids: &[Ident]) -> Result<ExprImpl> {
        let (binding, idents) = self.extract_binding_and_idents(expr, ids.to_vec())?;
        let (exprs, _) = Self::bind_field(
            InputRef::new(binding.index, binding.field.data_type.clone()).into(),
            &idents,
            binding.field.clone(),
            false,
        )?;
        Ok(exprs[0].clone())
    }

    /// Bind field in recursive way.
    fn bind_field(
        expr: ExprImpl,
        idents: &[Ident],
        field: Field,
        wildcard: bool,
    ) -> Result<(Vec<ExprImpl>, Field)> {
        match idents.get(0) {
            Some(ident) => {
                let (field, field_index) = field.sub_field(&ident.value)?;
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
                    field.data_type.clone(),
                )
                .into();
                Self::bind_field(expr, &idents[1..], field, wildcard)
            }
            None => {
                if wildcard {
                    Self::bind_wildcard_field(expr, field)
                } else {
                    Ok((vec![expr], field))
                }
            }
        }
    }

    /// Will fail if it's an atomic value.
    /// Rewrite (expr:Struct).* to [Field(expr, 0), Field(expr, 1), ... Field(expr, n)].
    fn bind_wildcard_field(expr: ExprImpl, field: Field) -> Result<(Vec<ExprImpl>, Field)> {
        if let DataType::Struct { .. } = field.data_type {
            Ok((
                field
                    .sub_fields
                    .iter()
                    .enumerate()
                    .map(|(i, f)| {
                        FunctionCall::new_unchecked(
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
                field,
            ))
        } else {
            Err(ErrorCode::BindError(format!(
                "The field \"{}\" is not a nested column",
                field.name
            ))
            .into())
        }
    }

    /// Return the schema of an expr, represented by a [`Field`].
    /// `name` is the alias of the expr.
    pub fn expr_to_field(&self, item: &ExprImpl, name: String) -> Result<Field> {
        if let DataType::Struct { .. } = item.return_type() {
            // Derive the schema of a struct including its fields name.
            // NOTE: The implementation assumes that only Min/Max/Field/InputRef/Struct ExprImpl
            // will return a STRUCT type. Because we assumes the first argument (if in a function
            // form) must be a STRUCT type.
            let is_struct_function = match item {
                ExprImpl::InputRef(_) => true,
                ExprImpl::FunctionCall(function) => {
                    matches!(function.get_expr_type(), ExprType::Field | ExprType::Row)
                }
                ExprImpl::AggCall(agg) => {
                    matches!(agg.agg_kind(), AggKind::Max | AggKind::Min)
                }
                _ => false,
            };
            if !is_struct_function {
                return Err(ErrorCode::BindError(format!(
                    "The expr must be Min/Max/Field/InputRef: {:?}",
                    item
                ))
                .into());
            }
            let mut visitor = GetFieldDesc::new(self.context.columns.clone());
            visitor.visit_expr(item);
            match visitor.collect()? {
                None => Ok(Field::with_name(item.return_type(), name)),
                Some(column) => Ok(Field::with_struct(
                    item.return_type(),
                    name,
                    column.sub_fields,
                    column.type_name,
                )),
            }
        } else {
            Ok(Field::with_name(item.return_type(), name))
        }
    }
}

/// Collect `field_desc` from bindings and expression.
struct GetFieldDesc {
    field: Option<Field>,
    err: Option<ErrorCode>,
    column_bindings: Vec<ColumnBinding>,
}

impl ExprVisitor for GetFieldDesc {
    /// Only check the first input because now we only accept nested column
    /// as first index.
    fn visit_agg_call(&mut self, agg_call: &AggCall) {
        match agg_call.inputs().get(0) {
            Some(input) => {
                self.visit_expr(input);
            }
            None => {}
        }
    }

    fn visit_input_ref(&mut self, expr: &InputRef) {
        self.field = Some(self.column_bindings[expr.index].field.clone());
    }

    /// The `func_call` only have two kinds which are `Field{InputRef,Literal(i32)}` or
    /// `Field{Field,Literal(i32)}`. So we only need to check the first input to get
    /// `column_desc` from bindings. And then we get the `field_desc` by second input.
    fn visit_function_call(&mut self, func_call: &FunctionCall) {
        if func_call.get_expr_type() == ExprType::Row {
            return;
        }
        match func_call.inputs().get(0) {
            Some(ExprImpl::FunctionCall(function)) => {
                self.visit_function_call(function);
            }
            Some(ExprImpl::InputRef(input)) => {
                self.visit_input_ref(input);
            }
            _ => {
                self.err = Some(
                    ErrorCode::BindError(format!(
                        "The first input of Field function must be FunctionCall or InputRef, now is {:?}",
                        func_call
                    )
                ));
                return;
            }
        }
        let column = match &self.field {
            None => {
                return;
            }
            Some(column) => column,
        };
        let expr = &func_call.inputs()[1];
        if let ExprImpl::Literal(literal) = expr {
            match literal.get_data() {
                Some(ScalarImpl::Int32(i)) => match column.sub_fields.get(*i as usize) {
                    Some(desc) => {
                        self.field = Some(desc.clone());
                    }
                    None => {
                        self.err = Some(ErrorCode::BindError(format!(
                            "Index {} out of field_desc bound {}",
                            *i,
                            column.sub_fields.len()
                        )));
                    }
                },
                _ => {
                    self.err = Some(ErrorCode::BindError(format!(
                        "The Literal in Field Function only accept i32 type, now is {:?}",
                        literal.return_type()
                    )));
                }
            }
        } else {
            self.err = Some(ErrorCode::BindError(format!(
                "The second input of Field function must be Literal, now is {:?}",
                expr
            )));
        }
    }
}

impl GetFieldDesc {
    /// Creates a `GetFieldDesc` with columns.
    fn new(columns: Vec<ColumnBinding>) -> Self {
        GetFieldDesc {
            field: None,
            err: None,
            column_bindings: columns,
        }
    }

    /// Returns the `field_desc` by the `GetFieldDesc`.
    fn collect(self) -> Result<Option<Field>> {
        match self.err {
            Some(err) => Err(err.into()),
            None => Ok(self.field),
        }
    }
}

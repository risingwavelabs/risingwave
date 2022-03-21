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
//
use itertools::zip_eq;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{BinaryOperator, DataType as AstDataType, Expr, UnaryOperator, TrimWhereField};

use crate::binder::Binder;
use crate::expr::{Expr as _, ExprImpl, ExprType, FunctionCall};

mod binary_op;
mod column;
mod function;
pub mod value;

impl Binder {
    pub(super) fn bind_expr(&mut self, expr: Expr) -> Result<ExprImpl> {
        match expr {
            Expr::IsNull(expr) => Ok(ExprImpl::FunctionCall(Box::new(
                self.bind_is_operator(ExprType::IsNull, *expr)?,
            ))),
            Expr::IsNotNull(expr) => Ok(ExprImpl::FunctionCall(Box::new(
                self.bind_is_operator(ExprType::IsNotNull, *expr)?,
            ))),
            Expr::IsTrue(expr) => Ok(ExprImpl::FunctionCall(Box::new(
                self.bind_is_operator(ExprType::IsTrue, *expr)?,
            ))),
            Expr::IsNotTrue(expr) => Ok(ExprImpl::FunctionCall(Box::new(
                self.bind_is_operator(ExprType::IsNotTrue, *expr)?,
            ))),
            Expr::IsFalse(expr) => Ok(ExprImpl::FunctionCall(Box::new(
                self.bind_is_operator(ExprType::IsFalse, *expr)?,
            ))),
            Expr::IsNotFalse(expr) => Ok(ExprImpl::FunctionCall(Box::new(
                self.bind_is_operator(ExprType::IsNotFalse, *expr)?,
            ))),
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => Ok(ExprImpl::FunctionCall(Box::new(self.bind_case(
                operand,
                conditions,
                results,
                else_result,
            )?))),
            Expr::Trim { expr, trim_where } => Ok(ExprImpl::FunctionCall(Box::new(
                self.bind_trim(*expr,trim_where)?,
            ))),
            Expr::Identifier(ident) => self.bind_column(&[ident]),
            Expr::CompoundIdentifier(idents) => self.bind_column(&idents),
            Expr::Value(v) => Ok(ExprImpl::Literal(Box::new(self.bind_value(v)?))),
            Expr::BinaryOp { left, op, right } => Ok(ExprImpl::FunctionCall(Box::new(
                self.bind_binary_op(*left, op, *right)?,
            ))),
            Expr::UnaryOp { op, expr } => Ok(ExprImpl::FunctionCall(Box::new(
                self.bind_unary_expr(op, *expr)?,
            ))),
            Expr::Nested(expr) => self.bind_expr(*expr),
            Expr::Cast { expr, data_type } => Ok(ExprImpl::FunctionCall(Box::new(
                self.bind_cast(*expr, data_type)?,
            ))),
            Expr::Function(f) => Ok(self.bind_function(f)?),
            _ => Err(ErrorCode::NotImplementedError(format!("{:?}", expr)).into()),
        }
    }

    pub(super) fn bind_unary_expr(
        &mut self,
        op: UnaryOperator,
        expr: Expr,
    ) -> Result<FunctionCall> {
        let func_type = match op {
            UnaryOperator::Minus => ExprType::Neg,
            UnaryOperator::Not => ExprType::Not,
            _ => {
                return Err(ErrorCode::NotImplementedError(format!(
                    "unsupported expression: {:?}",
                    op
                ))
                .into())
            }
        };
        let expr = self.bind_expr(expr)?;
        let return_type = expr.return_type();
        FunctionCall::new(func_type, vec![expr]).ok_or_else(|| {
            ErrorCode::NotImplementedError(format!("{:?} {:?}", op, return_type)).into()
        })
    }
    pub(super) fn bind_trim(
        &mut self,
        expr: Expr,
        // ([BOTH | LEADING | TRAILING], <expr>)
        trim_where: Option<(TrimWhereField, Box<Expr>)>,
    ) -> Result<FunctionCall> {
        let mut inputs = vec![self.bind_expr(expr)?];
        let func_type = match trim_where {
            Some(t) => {
                inputs.push(self.bind_expr(*t.1)?);
                match t.0 {
                    TrimWhereField::Both => ExprType::Trim,
                    TrimWhereField::Leading => ExprType::Ltrim,
                    TrimWhereField::Trailing => ExprType::Rtrim,
                }
            }
            None => ExprType::Trim,
        };
        Ok(FunctionCall::new_with_return_type(
            func_type,
            inputs,
            DataType::Varchar,
        ))
    }

    pub(super) fn bind_case(
        &mut self,
        operand: Option<Box<Expr>>,
        conditions: Vec<Expr>,
        results: Vec<Expr>,
        else_result: Option<Box<Expr>>,
    ) -> Result<FunctionCall> {
        let mut inputs = Vec::new();
        let results_expr: Vec<ExprImpl> = results
            .into_iter()
            .map(|expr| self.bind_expr(expr))
            .collect::<Result<_>>()?;
        let else_result_expr = else_result.map(|expr| self.bind_expr(*expr)).transpose()?;
        let mut return_type = results_expr.get(0).unwrap().return_type();
        for i in 1..results_expr.len() {
            return_type =
                Self::find_compat(return_type, results_expr.get(i).unwrap().return_type())?;
        }
        if let Some(expr) = &else_result_expr {
            return_type = Binder::find_compat(return_type, expr.return_type())?;
        }
        for (condition, result) in zip_eq(conditions, results_expr) {
            let condition = match operand {
                Some(ref t) => Expr::BinaryOp {
                    left: t.clone(),
                    op: BinaryOperator::Eq,
                    right: Box::new(condition),
                },
                None => condition,
            };
            inputs.push(self.bind_expr(condition)?);
            inputs.push(Binder::ensure_type(result, return_type.clone()));
        }
        if let Some(expr) = else_result_expr {
            inputs.push(Binder::ensure_type(expr, return_type.clone()));
        }
        Ok(FunctionCall::new_with_return_type(
            ExprType::Case,
            inputs,
            return_type,
        ))
    }

    pub(super) fn bind_is_operator(
        &mut self,
        func_type: ExprType,
        expr: Expr,
    ) -> Result<FunctionCall> {
        let expr = self.bind_expr(expr)?;
        FunctionCall::new(func_type, vec![expr])
            .ok_or_else(|| ErrorCode::NotImplementedError(format!("{:?}", &func_type)).into())
    }

    pub(super) fn bind_cast(&mut self, expr: Expr, data_type: AstDataType) -> Result<FunctionCall> {
        Ok(FunctionCall::new_with_return_type(
            ExprType::Cast,
            vec![self.bind_expr(expr)?],
            bind_data_type(&data_type)?,
        ))
    }
}

pub fn bind_data_type(data_type: &AstDataType) -> Result<DataType> {
    let data_type = match data_type {
        AstDataType::SmallInt(_) => DataType::Int16,
        AstDataType::Int(_) => DataType::Int32,
        AstDataType::BigInt(_) => DataType::Int64,
        AstDataType::Float(_) => DataType::Float64,
        AstDataType::Double => DataType::Float64,
        AstDataType::String => DataType::Varchar,
        AstDataType::Boolean => DataType::Boolean,
        AstDataType::Char(_) => DataType::Char,
        AstDataType::Varchar(_) => DataType::Varchar,
        AstDataType::Decimal(_, _) => DataType::Decimal,
        AstDataType::Date => DataType::Date,
        AstDataType::Time => DataType::Time,
        AstDataType::Timestamp => DataType::Timestamp,
        AstDataType::Interval => DataType::Interval,
        AstDataType::Real => DataType::Float32,
        _ => {
            return Err(ErrorCode::NotImplementedError(format!(
                "unsupported data type: {:?}",
                data_type
            ))
            .into())
        }
    };
    Ok(data_type)
}

use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{BinaryOperator, DataType as AstDataType, Expr, UnaryOperator};

use crate::binder::Binder;
use crate::expr::{Expr as _, ExprImpl, ExprType, FunctionCall};

mod binary_op;
mod column;
mod function;
mod value;

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

    pub(super) fn bind_case(
        &mut self,
        operand: Option<Box<Expr>>,
        conditions: Vec<Expr>,
        results: Vec<Expr>,
        else_result: Option<Box<Expr>>,
    ) -> Result<FunctionCall> {
        let mut inputs = Vec::new();
        // suppose lengths of conditions and results are the same
        if conditions.len() != results.len() {
            todo!();
        }
        let results_expr: Vec<ExprImpl> = results
            .into_iter()
            .map(|expr| self.bind_expr(expr).unwrap())
            .collect();
        let mut return_type = Binder::find_proper_type(&results_expr)?;
        return_type = match else_result.clone() {
            Some(t) => {
                let _return_type = self.bind_expr(*t)?.return_type();
                Binder::find_compat(return_type, _return_type)?
            }
            None => return_type,
        };
        for i in 0..conditions.len() {
            let condition = conditions.get(i).unwrap();
            let _condition = match operand {
                Some(ref t) => Expr::BinaryOp {
                    left: t.clone(),
                    op: BinaryOperator::Eq,
                    right: Box::new(condition.clone()),
                },
                None => condition.clone(),
            };
            inputs.push(self.bind_expr(_condition)?);
            inputs.push(Binder::ensure_type(
                results_expr.get(i).unwrap().clone(),
                return_type.clone(),
            ));
        }
        if let Some(expr) = else_result {
            inputs.push(Binder::ensure_type(
                self.bind_expr(*expr)?,
                return_type.clone(),
            ));
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
        Ok(FunctionCall::new(func_type, vec![expr]).unwrap())
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

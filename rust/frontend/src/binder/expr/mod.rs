use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::expr::AggKind;
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{
    DataType as AstDataType, Expr as AstExpr, Function, FunctionArg, FunctionArgExpr, UnaryOperator,
};

use crate::binder::Binder;
use crate::expr::{AggCall, Expr, ExprImpl, ExprType, FunctionCall};

mod binary_op;
mod column;
mod value;

impl Binder {
    pub(super) fn bind_expr(&mut self, expr: AstExpr) -> Result<ExprImpl> {
        match expr {
            AstExpr::Identifier(ident) => self.bind_column(&[ident]),
            AstExpr::CompoundIdentifier(idents) => self.bind_column(&idents),
            AstExpr::Value(v) => Ok(ExprImpl::Literal(Box::new(self.bind_value(v)?))),
            AstExpr::BinaryOp { left, op, right } => Ok(ExprImpl::FunctionCall(Box::new(
                self.bind_binary_op(*left, op, *right)?,
            ))),
            AstExpr::UnaryOp { op, expr } => Ok(ExprImpl::FunctionCall(Box::new(
                self.bind_unary_expr(op, *expr)?,
            ))),
            AstExpr::Nested(expr) => self.bind_expr(*expr),
            AstExpr::Cast { expr, data_type } => Ok(ExprImpl::FunctionCall(Box::new(
                self.bind_cast(*expr, data_type)?,
            ))),
            AstExpr::Function(f) => Ok(self.bind_function(f)?),
            _ => Err(ErrorCode::NotImplementedError(format!("{:?}", expr)).into()),
        }
    }

    pub(super) fn bind_unary_expr(
        &mut self,
        op: UnaryOperator,
        expr: AstExpr,
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

    pub(super) fn bind_cast(
        &mut self,
        expr: AstExpr,
        data_type: AstDataType,
    ) -> Result<FunctionCall> {
        Ok(FunctionCall::new_with_return_type(
            ExprType::Cast,
            vec![self.bind_expr(expr)?],
            self.bind_data_type(data_type)?,
        ))
    }

    pub(super) fn bind_data_type(&self, data_type: AstDataType) -> Result<DataType> {
        Ok(match data_type {
            AstDataType::SmallInt(_) => DataType::Int16,
            AstDataType::Int(_) => DataType::Int32,
            AstDataType::BigInt(_) => DataType::Int64,
            AstDataType::Float(_) => DataType::Float32,
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
            _ => {
                return Err(ErrorCode::NotImplementedError(format!(
                    "unsupported data type: {:?}",
                    data_type
                ))
                .into())
            }
        })
    }

    pub(super) fn bind_function(&mut self, f: Function) -> Result<ExprImpl> {
        let inputs = f
            .args
            .into_iter()
            .map(|arg| self.bind_function_arg(arg))
            .collect::<Result<Vec<ExprImpl>>>()?;

        if f.name.0.len() == 1 {
            let function_name = f.name.0.get(0).unwrap();
            let agg_kind = match function_name.value.as_str() {
                "count" => Some(AggKind::Count),
                "sum" => Some(AggKind::Sum),
                "min" => Some(AggKind::Min),
                "max" => Some(AggKind::Max),
                _ => None,
            };
            match agg_kind {
                Some(kind) => return Ok(ExprImpl::AggCall(Box::new(AggCall::new(kind, inputs)?))),
                None => {}
            };
            let function_type = match function_name.value.as_str() {
                "substr" => ExprType::Substr,
                _ => {
                    return Err(ErrorCode::NotImplementedError(format!(
                        "unsupported function: {:?}",
                        function_name
                    ))
                    .into())
                }
            };
            Ok(ExprImpl::FunctionCall(Box::new(
                FunctionCall::new(function_type, inputs).unwrap(),
            )))
        } else {
            Err(
                ErrorCode::NotImplementedError(format!("unsupported function: {:?}", f.name))
                    .into(),
            )
        }
    }

    pub(super) fn bind_function_expr_arg(&mut self, arg_expr: FunctionArgExpr) -> Result<ExprImpl> {
        match arg_expr {
            FunctionArgExpr::Expr(expr) => self.bind_expr(expr),
            FunctionArgExpr::QualifiedWildcard(_) => todo!(),
            FunctionArgExpr::Wildcard => todo!(),
        }
    }

    pub(super) fn bind_function_arg(&mut self, arg: FunctionArg) -> Result<ExprImpl> {
        match arg {
            FunctionArg::Unnamed(expr) => self.bind_function_expr_arg(expr),
            FunctionArg::Named { .. } => todo!(),
        }
    }
}

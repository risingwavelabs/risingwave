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
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;
use risingwave_expr::expr::AggKind;
use risingwave_sqlparser::ast::{Function, FunctionArg, FunctionArgExpr};

use crate::binder::bind_context::Clause;
use crate::binder::Binder;
use crate::expr::{AggCall, Expr, ExprImpl, ExprType, FunctionCall, Literal};

impl Binder {
    pub(super) fn bind_function(&mut self, f: Function) -> Result<ExprImpl> {
        let mut inputs = f
            .args
            .into_iter()
            .map(|arg| self.bind_function_arg(arg))
            .flatten_ok()
            .try_collect()?;

        if f.name.0.len() == 1 {
            let function_name = f.name.0.get(0).unwrap().value.as_str();
            let function_name = function_name.to_lowercase();
            let agg_kind = match function_name.as_str() {
                "count" => Some(AggKind::Count),
                "sum" => Some(AggKind::Sum),
                "min" => Some(AggKind::Min),
                "max" => Some(AggKind::Max),
                "avg" => Some(AggKind::Avg),
                _ => None,
            };
            if let Some(kind) = agg_kind {
                self.ensure_aggregate_allowed()?;
                return Ok(ExprImpl::AggCall(Box::new(AggCall::new(
                    kind, inputs, f.distinct,
                )?)));
            }
            let function_type = match function_name.as_str() {
                "substr" => ExprType::Substr,
                "length" => ExprType::Length,
                "upper" => ExprType::Upper,
                "lower" => ExprType::Lower,
                "trim" => ExprType::Trim,
                "replace" => ExprType::Replace,
                "position" => ExprType::Position,
                "ltrim" => ExprType::Ltrim,
                "rtrim" => ExprType::Rtrim,
                "nullif" => {
                    inputs = Self::rewrite_nullif_to_case_when(inputs)?;
                    ExprType::Case
                }
                "concat_ws" => ExprType::ConcatWs,
                "coalesce" => ExprType::Coalesce,
                "round" => {
                    inputs = Self::rewrite_round_args(inputs);
                    if inputs.len() >= 2 {
                        ExprType::RoundDigit
                    } else {
                        ExprType::Round
                    }
                }
                "ceil" => {
                    inputs = Self::rewrite_round_args(inputs);
                    ExprType::Ceil
                }
                "floor" => {
                    inputs = Self::rewrite_round_args(inputs);
                    ExprType::Floor
                }
                "abs" => ExprType::Abs,
                "booleq" => {
                    inputs = Self::rewrite_two_bool_inputs(inputs)?;
                    ExprType::Equal
                }
                "boolne" => {
                    inputs = Self::rewrite_two_bool_inputs(inputs)?;
                    ExprType::NotEqual
                }
                _ => {
                    return Err(ErrorCode::NotImplemented(
                        format!("unsupported function: {:?}", function_name),
                        112.into(),
                    )
                    .into());
                }
            };
            Ok(FunctionCall::new(function_type, inputs)?.into())
        } else {
            Err(ErrorCode::NotImplemented(
                format!("unsupported function: {:?}", f.name),
                112.into(),
            )
            .into())
        }
    }

    /// Make sure inputs only have 2 value and rewrite the arguments.
    /// Nullif(expr1,expr2) -> Case(Equal(expr1 = expr2),null,expr1).
    fn rewrite_nullif_to_case_when(inputs: Vec<ExprImpl>) -> Result<Vec<ExprImpl>> {
        if inputs.len() != 2 {
            Err(ErrorCode::BindError("Nullif function must contain 2 arguments".to_string()).into())
        } else {
            let inputs = vec![
                FunctionCall::new(ExprType::Equal, inputs.clone())?.into(),
                Literal::new(None, inputs[0].return_type()).into(),
                inputs[0].clone(),
            ];
            Ok(inputs)
        }
    }

    /// Rewrite the arguments to be consistent with the `round, ceil, floor` signature:
    /// Round:
    /// - round(Decimal, Int32) -> Decimal
    /// - round(Decimal) -> Decimal
    /// - round(Float64) -> Float64
    /// - Extend: round(Int16, Int32, Int64, Float32) -> Decimal
    ///
    /// Ceil:
    /// - ceil(Decimal) -> Decimal
    /// - ceil(Float) -> Float64
    /// - Extend: ceil(Int16, Int32, Int64, Float32) -> Decimal
    ///
    /// Floor:
    /// - floor(Decimal) -> Decimal
    /// - floor(Float) -> Float64
    /// - Extend: floor(Int16, Int32, Int64, Float32) -> Decimal
    fn rewrite_round_args(mut inputs: Vec<ExprImpl>) -> Vec<ExprImpl> {
        if inputs.len() == 1 {
            let input = inputs.pop().unwrap();
            match input.return_type() {
                risingwave_common::types::DataType::Float64 => vec![input],
                _ => vec![input
                    .clone()
                    .cast_implicit(DataType::Decimal)
                    .unwrap_or(input)],
            }
        } else if inputs.len() == 2 {
            let digits = inputs.pop().unwrap();
            let input = inputs.pop().unwrap();
            vec![
                input
                    .clone()
                    .cast_implicit(DataType::Decimal)
                    .unwrap_or(input),
                digits
                    .clone()
                    .cast_implicit(DataType::Int32)
                    .unwrap_or(digits),
            ]
        } else {
            inputs
        }
    }

    fn rewrite_two_bool_inputs(mut inputs: Vec<ExprImpl>) -> Result<Vec<ExprImpl>> {
        if inputs.len() != 2 {
            return Err(
                ErrorCode::BindError("function must contain only 2 arguments".to_string()).into(),
            );
        }
        let left = inputs.pop().unwrap();
        let right = inputs.pop().unwrap();
        Ok(vec![
            left.cast_implicit(DataType::Boolean)?,
            right.cast_implicit(DataType::Boolean)?,
        ])
    }

    fn ensure_aggregate_allowed(&self) -> Result<()> {
        if let Some(clause) = self.context.clause {
            if clause == Clause::Values || clause == Clause::Where {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "aggregate functions are not allowed in {}",
                    clause
                ))
                .into());
            }
        }
        Ok(())
    }

    pub(in crate::binder) fn bind_function_expr_arg(
        &mut self,
        arg_expr: FunctionArgExpr,
    ) -> Result<Vec<ExprImpl>> {
        match arg_expr {
            FunctionArgExpr::Expr(expr) => Ok(vec![self.bind_expr(expr)?]),
            FunctionArgExpr::QualifiedWildcard(_) => todo!(),
            FunctionArgExpr::ExprQualifiedWildcard(_, _) => todo!(),
            FunctionArgExpr::Wildcard => Ok(vec![]),
        }
    }

    pub(in crate::binder) fn bind_function_arg(
        &mut self,
        arg: FunctionArg,
    ) -> Result<Vec<ExprImpl>> {
        match arg {
            FunctionArg::Unnamed(expr) => self.bind_function_expr_arg(expr),
            FunctionArg::Named { .. } => todo!(),
        }
    }
}

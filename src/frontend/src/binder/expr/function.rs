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

use std::iter::once;

use itertools::Itertools;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;
use risingwave_expr::expr::AggKind;
use risingwave_sqlparser::ast::{Function, FunctionArg, FunctionArgExpr};

use crate::binder::bind_context::Clause;
use crate::binder::Binder;
use crate::expr::{
    AggCall, AggOrderBy, AggOrderByExpr, Expr, ExprImpl, ExprType, FunctionCall, Literal,
};
use crate::utils::Condition;

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
                "string_agg" => Some(AggKind::StringAgg),
                "single_value" => Some(AggKind::SingleValue),
                "approx_count_distinct" => Some(AggKind::ApproxCountDistinct),
                _ => None,
            };
            if let Some(kind) = agg_kind {
                self.ensure_aggregate_allowed()?;
                let filter = match f.filter {
                    Some(filter) => {
                        let expr = self.bind_expr(*filter)?;
                        if expr.return_type() != DataType::Boolean {
                            return Err(ErrorCode::InvalidInputSyntax(format!(
                                "the type of filter clause should be boolean, but found {:?}",
                                expr.return_type()
                            ))
                            .into());
                        }
                        if expr.has_subquery() {
                            return Err(ErrorCode::InvalidInputSyntax(
                                "subquery in filter clause is not supported".to_string(),
                            )
                            .into());
                        }
                        if expr.has_agg_call() {
                            return Err(ErrorCode::InvalidInputSyntax(
                                "aggregation function in filter clause is not supported"
                                    .to_string(),
                            )
                            .into());
                        }
                        Condition::with_expr(expr)
                    }
                    None => Condition::true_cond(),
                };
                // TODO(yuchao): handle DISTINCT and ORDER BY appear at the same time
                let order_by = AggOrderBy::new(
                    f.order_by
                        .into_iter()
                        .map(|e| -> Result<AggOrderByExpr> {
                            Ok(AggOrderByExpr {
                                expr: self.bind_expr(e.expr)?,
                                asc: e.asc,
                                nulls_first: e.nulls_first,
                            })
                        })
                        .try_collect()?,
                );
                log::warn!("[rc] order_by: {:?}", order_by);
                return Ok(ExprImpl::AggCall(Box::new(AggCall::new(
                    kind, inputs, f.distinct, order_by, filter,
                )?)));
            } else if f.filter.is_some() {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "filter clause is only allowed in aggregation functions, but `{}` is not an aggregation function", function_name
                )
                )
                .into());
            }
            let function_type = match function_name.as_str() {
                // comparison
                "booleq" => {
                    inputs = Self::rewrite_two_bool_inputs(inputs)?;
                    ExprType::Equal
                }
                "boolne" => {
                    inputs = Self::rewrite_two_bool_inputs(inputs)?;
                    ExprType::NotEqual
                }
                // conditional
                "coalesce" => ExprType::Coalesce,
                "nullif" => {
                    inputs = Self::rewrite_nullif_to_case_when(inputs)?;
                    ExprType::Case
                }
                // mathematical
                "round" => {
                    if inputs.len() >= 2 {
                        ExprType::RoundDigit
                    } else {
                        ExprType::Round
                    }
                }
                "ceil" => ExprType::Ceil,
                "floor" => ExprType::Floor,
                "abs" => ExprType::Abs,
                // string
                "substr" => ExprType::Substr,
                "length" => ExprType::Length,
                "upper" => ExprType::Upper,
                "lower" => ExprType::Lower,
                "trim" => ExprType::Trim,
                "replace" => ExprType::Replace,
                "overlay" => ExprType::Overlay,
                "position" => ExprType::Position,
                "ltrim" => ExprType::Ltrim,
                "rtrim" => ExprType::Rtrim,
                "md5" => ExprType::Md5,
                "to_char" => ExprType::ToChar,
                "concat" => {
                    inputs = Self::rewrite_concat_to_concat_ws(inputs)?;
                    ExprType::ConcatWs
                }
                "concat_ws" => ExprType::ConcatWs,
                "split_part" => ExprType::SplitPart,
                "char_length" => ExprType::CharLength,
                "character_length" => ExprType::CharLength,
                "repeat" => ExprType::Repeat,
                "ascii" => ExprType::Ascii,
                "octet_length" => ExprType::OctetLength,
                "bit_length" => ExprType::BitLength,
                "regexp_match" => ExprType::RegexpMatch,
                // special
                "pg_typeof" if inputs.len() == 1 => {
                    let input = &inputs[0];
                    let v = match input.is_unknown() {
                        true => "unknown".into(),
                        false => input.return_type().to_string(),
                    };
                    return Ok(ExprImpl::literal_varchar(v));
                }
                "current_database" if inputs.is_empty() => {
                    return Ok(ExprImpl::literal_varchar(self.db_name.clone()));
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

    fn rewrite_concat_to_concat_ws(inputs: Vec<ExprImpl>) -> Result<Vec<ExprImpl>> {
        if inputs.is_empty() {
            Err(ErrorCode::BindError(
                "Function `Concat` takes at least 1 arguments (0 given)".to_string(),
            )
            .into())
        } else {
            let inputs = once(ExprImpl::literal_varchar("".to_string()))
                .chain(inputs)
                .collect();
            Ok(inputs)
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

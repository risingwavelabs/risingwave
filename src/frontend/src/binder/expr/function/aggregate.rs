// Copyright 2025 RisingWave Labs
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
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::{bail, bail_not_implemented};
use risingwave_expr::aggregate::{AggType, PbAggKind, agg_types};
use risingwave_sqlparser::ast::{self, FunctionArgExpr};

use crate::Binder;
use crate::binder::Clause;
use crate::error::{ErrorCode, Result};
use crate::expr::{AggCall, ExprImpl, Literal, OrderBy};
use crate::utils::Condition;

impl Binder {
    fn ensure_aggregate_allowed(&self) -> Result<()> {
        if let Some(clause) = self.context.clause {
            match clause {
                Clause::Where
                | Clause::Values
                | Clause::From
                | Clause::GeneratedColumn
                | Clause::Insert
                | Clause::JoinOn => {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "aggregate functions are not allowed in {}",
                        clause
                    ))
                    .into());
                }
                Clause::Having | Clause::Filter | Clause::GroupBy => {}
            }
        }
        Ok(())
    }

    pub(super) fn bind_aggregate_function(
        &mut self,
        agg_type: AggType,
        distinct: bool,
        args: Vec<ExprImpl>,
        order_by: Vec<ast::OrderByExpr>,
        within_group: Option<Box<ast::OrderByExpr>>,
        filter: Option<Box<ast::Expr>>,
    ) -> Result<ExprImpl> {
        self.ensure_aggregate_allowed()?;

        let (direct_args, args, order_by) = if matches!(agg_type, agg_types::ordered_set!()) {
            self.bind_ordered_set_agg(&agg_type, distinct, args, order_by, within_group)?
        } else {
            self.bind_normal_agg(&agg_type, distinct, args, order_by, within_group)?
        };

        let filter = match filter {
            Some(filter) => {
                let mut clause = Some(Clause::Filter);
                std::mem::swap(&mut self.context.clause, &mut clause);
                let expr = self
                    .bind_expr_inner(*filter)
                    .and_then(|expr| expr.enforce_bool_clause("FILTER"))?;
                self.context.clause = clause;
                if expr.has_subquery() {
                    bail_not_implemented!("subquery in filter clause");
                }
                if expr.has_agg_call() {
                    bail_not_implemented!("aggregation function in filter clause");
                }
                if expr.has_table_function() {
                    bail_not_implemented!("table function in filter clause");
                }
                Condition::with_expr(expr)
            }
            None => Condition::true_cond(),
        };

        Ok(ExprImpl::AggCall(Box::new(AggCall::new(
            agg_type,
            args,
            distinct,
            order_by,
            filter,
            direct_args,
        )?)))
    }

    fn bind_ordered_set_agg(
        &mut self,
        kind: &AggType,
        distinct: bool,
        args: Vec<ExprImpl>,
        order_by: Vec<ast::OrderByExpr>,
        within_group: Option<Box<ast::OrderByExpr>>,
    ) -> Result<(Vec<Literal>, Vec<ExprImpl>, OrderBy)> {
        // Syntax:
        // aggregate_name ( [ expression [ , ... ] ] ) WITHIN GROUP ( order_by_clause ) [ FILTER
        // ( WHERE filter_clause ) ]

        assert!(matches!(kind, agg_types::ordered_set!()));

        if !order_by.is_empty() {
            return Err(ErrorCode::InvalidInputSyntax(format!(
                "`ORDER BY` is not allowed for ordered-set aggregation `{}`",
                kind
            ))
            .into());
        }
        if distinct {
            return Err(ErrorCode::InvalidInputSyntax(format!(
                "`DISTINCT` is not allowed for ordered-set aggregation `{}`",
                kind
            ))
            .into());
        }

        let within_group = *within_group.ok_or_else(|| {
            ErrorCode::InvalidInputSyntax(format!(
                "`WITHIN GROUP` is expected for ordered-set aggregation `{}`",
                kind
            ))
        })?;

        let mut direct_args = args;
        let mut args =
            self.bind_function_expr_arg(FunctionArgExpr::Expr(within_group.expr.clone()))?;
        let order_by = OrderBy::new(vec![self.bind_order_by_expr(within_group)?]);

        // check signature and do implicit cast
        match (kind, direct_args.len(), args.as_mut_slice()) {
            (AggType::Builtin(PbAggKind::PercentileCont | PbAggKind::PercentileDisc), 1, [arg]) => {
                let fraction = &mut direct_args[0];
                decimal_to_float64(fraction, kind)?;
                if matches!(&kind, AggType::Builtin(PbAggKind::PercentileCont)) {
                    arg.cast_implicit_mut(DataType::Float64).map_err(|_| {
                        ErrorCode::InvalidInputSyntax(format!(
                            "arg in `{}` must be castable to float64",
                            kind
                        ))
                    })?;
                }
            }
            (AggType::Builtin(PbAggKind::Mode), 0, [_arg]) => {}
            (AggType::Builtin(PbAggKind::ApproxPercentile), 1..=2, [_percentile_col]) => {
                let percentile = &mut direct_args[0];
                decimal_to_float64(percentile, kind)?;
                match direct_args.len() {
                    2 => {
                        let relative_error = &mut direct_args[1];
                        decimal_to_float64(relative_error, kind)?;
                        if let Some(relative_error) = relative_error.as_literal()
                            && let Some(relative_error) = relative_error.get_data()
                        {
                            let relative_error = relative_error.as_float64().0;
                            if relative_error <= 0.0 || relative_error >= 1.0 {
                                bail!(
                                    "relative_error={} does not satisfy 0.0 < relative_error < 1.0",
                                    relative_error,
                                )
                            }
                        }
                    }
                    1 => {
                        let relative_error: ExprImpl = Literal::new(
                            ScalarImpl::Float64(0.01.into()).into(),
                            DataType::Float64,
                        )
                        .into();
                        direct_args.push(relative_error);
                    }
                    _ => {
                        return Err(ErrorCode::InvalidInputSyntax(
                            "invalid direct args for approx_percentile aggregation".to_owned(),
                        )
                        .into());
                    }
                }
            }
            _ => {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "invalid direct args or within group argument for `{}` aggregation",
                    kind
                ))
                .into());
            }
        }

        Ok((
            direct_args
                .into_iter()
                .map(|arg| *arg.into_literal().unwrap())
                .collect(),
            args,
            order_by,
        ))
    }

    fn bind_normal_agg(
        &mut self,
        kind: &AggType,
        distinct: bool,
        args: Vec<ExprImpl>,
        order_by: Vec<ast::OrderByExpr>,
        within_group: Option<Box<ast::OrderByExpr>>,
    ) -> Result<(Vec<Literal>, Vec<ExprImpl>, OrderBy)> {
        // Syntax:
        // aggregate_name (expression [ , ... ] [ order_by_clause ] ) [ FILTER ( WHERE
        //   filter_clause ) ]
        // aggregate_name (ALL expression [ , ... ] [ order_by_clause ] ) [ FILTER ( WHERE
        //   filter_clause ) ]
        // aggregate_name (DISTINCT expression [ , ... ] [ order_by_clause ] ) [ FILTER ( WHERE
        //   filter_clause ) ]
        // aggregate_name ( * ) [ FILTER ( WHERE filter_clause ) ]

        assert!(!matches!(kind, agg_types::ordered_set!()));

        if within_group.is_some() {
            return Err(ErrorCode::InvalidInputSyntax(format!(
                "`WITHIN GROUP` is not allowed for non-ordered-set aggregation `{}`",
                kind
            ))
            .into());
        }

        let order_by = OrderBy::new(
            order_by
                .into_iter()
                .map(|e| self.bind_order_by_expr(e))
                .try_collect()?,
        );

        if distinct {
            if matches!(
                kind,
                AggType::Builtin(PbAggKind::ApproxCountDistinct)
                    | AggType::Builtin(PbAggKind::ApproxPercentile)
            ) {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "DISTINCT is not allowed for approximate aggregation `{}`",
                    kind
                ))
                .into());
            }

            if args.is_empty() {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "DISTINCT is not allowed for aggregate function `{}` without args",
                    kind
                ))
                .into());
            }

            // restrict arguments[1..] to be constant because we don't support multiple distinct key
            // indices for now
            if args.iter().skip(1).any(|arg| arg.as_literal().is_none()) {
                bail_not_implemented!(
                    "non-constant arguments other than the first one for DISTINCT aggregation is not supported now"
                );
            }

            // restrict ORDER BY to align with PG, which says:
            // > If DISTINCT is specified in addition to an order_by_clause, then all the ORDER BY
            // > expressions must match regular arguments of the aggregate; that is, you cannot sort
            // > on an expression that is not included in the DISTINCT list.
            if !order_by.sort_exprs.iter().all(|e| args.contains(&e.expr)) {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "ORDER BY expressions must match regular arguments of the aggregate for `{}` when DISTINCT is provided",
                    kind
                ))
                .into());
            }
        }

        Ok((vec![], args, order_by))
    }
}

fn decimal_to_float64(decimal_expr: &mut ExprImpl, kind: &AggType) -> Result<()> {
    if decimal_expr.cast_implicit_mut(DataType::Float64).is_err() {
        return Err(ErrorCode::InvalidInputSyntax(format!(
            "direct arg in `{}` must be castable to float64",
            kind
        ))
        .into());
    }

    let Some(Ok(fraction_datum)) = decimal_expr.try_fold_const() else {
        bail_not_implemented!(
            issue = 14079,
            "variable as direct argument of ordered-set aggregate",
        );
    };

    if let Some(ref fraction_value) = fraction_datum
        && !(0.0..=1.0).contains(&fraction_value.as_float64().0)
    {
        return Err(ErrorCode::InvalidInputSyntax(format!(
            "direct arg in `{}` must between 0.0 and 1.0",
            kind
        ))
        .into());
    }
    // note that the fraction can be NULL
    *decimal_expr = Literal::new(fraction_datum, DataType::Float64).into();
    Ok(())
}

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

use fixedbitset::FixedBitSet;
use risingwave_common::types::DataType;
use risingwave_expr::window_function::WindowFuncKind;

use super::{BoxedRule, Rule};
use crate::PlanRef;
use crate::expr::{
    Expr, ExprImpl, ExprRewriter, ExprType, FunctionCall, Literal, collect_input_refs,
};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{LogicalFilter, LogicalTopN, PlanTreeNodeUnary};
use crate::optimizer::property::Order;
use crate::planner::LIMIT_ALL_COUNT;
use crate::utils::Condition;

/// Transforms the following pattern to group `TopN` (No Ranking Output).
///
/// ```sql
/// -- project - filter - over window
/// SELECT .. FROM
///   (SELECT .., ROW_NUMBER() OVER(PARTITION BY .. ORDER BY ..) rank FROM ..)
/// WHERE rank [ < | <= | > | >= | = ] ..;
/// ```
///
/// Transforms the following pattern to `OverWindow` + group `TopN` (Ranking Output).
/// The `TopN` decreases the number of rows to be processed by the `OverWindow`.
///
/// ```sql
/// -- filter - over window
/// SELECT .., ROW_NUMBER() OVER(PARTITION BY .. ORDER BY ..) rank
/// FROM ..
/// WHERE rank [ < | <= | > | >= | = ] ..;
/// ```
///
/// Also optimizes filter arithmetic expressions in the `Project <- Filter <- OverWindow` pattern,
/// such as simplifying `(row_number - 1) = 0` to `row_number = 1`.
pub struct OverWindowToTopNRule;

impl OverWindowToTopNRule {
    pub fn create() -> BoxedRule {
        Box::new(OverWindowToTopNRule)
    }
}

impl Rule for OverWindowToTopNRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let ctx = plan.ctx();
        let (project, plan) = {
            if let Some(project) = plan.as_logical_project() {
                (Some(project), project.input())
            } else {
                (None, plan)
            }
        };
        let filter = plan.as_logical_filter()?;
        let plan = filter.input();
        // The filter is directly on top of the over window after predicate pushdown.
        let over_window = plan.as_logical_over_window()?;

        // First try to simplify filter arithmetic expressions
        let filter = if let Some(simplified) = self.simplify_filter_arithmetic(filter) {
            simplified
        } else {
            filter.clone()
        };

        if over_window.window_functions().len() != 1 {
            // Queries with multiple window function calls are not supported yet.
            return None;
        }
        let window_func = &over_window.window_functions()[0];
        if !window_func.kind.is_numbering() {
            // Only rank functions can be converted to TopN.
            return None;
        }

        let output_len = over_window.schema().len();
        let window_func_pos = output_len - 1;

        let with_ties = match window_func.kind {
            // Only `ROW_NUMBER` and `RANK` can be optimized to TopN now.
            WindowFuncKind::RowNumber => false,
            WindowFuncKind::Rank => true,
            WindowFuncKind::DenseRank => {
                ctx.warn_to_user("`dense_rank` is not supported in Top-N pattern, will fallback to inefficient implementation");
                return None;
            }
            _ => unreachable!("window functions other than rank functions should not reach here"),
        };

        let (rank_pred, other_pred) = {
            let predicate = filter.predicate();
            let mut rank_col = FixedBitSet::with_capacity(output_len);
            rank_col.set(window_func_pos, true);
            predicate.clone().split_disjoint(&rank_col)
        };

        let (limit, offset) = handle_rank_preds(&rank_pred.conjunctions, window_func_pos)?;

        if offset > 0 && with_ties {
            tracing::warn!("Failed to optimize with ties and offset");
            ctx.warn_to_user("group topN with ties and offset is not supported, see https://www.risingwave.dev/docs/current/sql-pattern-topn/ for more information");
            return None;
        }

        let topn: PlanRef = LogicalTopN::new(
            over_window.input(),
            limit,
            offset,
            with_ties,
            Order {
                column_orders: window_func.order_by.to_vec(),
            },
            window_func.partition_by.iter().map(|i| i.index).collect(),
        )
        .into();
        let filter = LogicalFilter::create(topn, other_pred);

        let plan = if let Some(project) = project {
            let referred_cols = collect_input_refs(output_len, project.exprs());
            if !referred_cols.contains(window_func_pos) {
                // No Ranking Output
                project.clone_with_input(filter).into()
            } else {
                // Ranking Output, with project
                project
                    .clone_with_input(over_window.clone_with_input(filter).into())
                    .into()
            }
        } else {
            // Ranking Output, without project
            ctx.warn_to_user("It can be inefficient to output ranking number in Top-N, see https://www.risingwave.dev/docs/current/sql-pattern-topn/ for more information");
            over_window.clone_with_input(filter).into()
        };
        Some(plan)
    }
}

impl OverWindowToTopNRule {
    /// Simplify arithmetic expressions in filter conditions before TopN optimization
    /// For example: `(row_number - 1) = 0` -> `row_number = 1`
    fn simplify_filter_arithmetic(&self, filter: &LogicalFilter) -> Option<LogicalFilter> {
        let new_predicate = self.simplify_filter_arithmetic_condition(filter.predicate())?;
        Some(LogicalFilter::new(filter.input(), new_predicate))
    }

    /// Simplify arithmetic expressions in the filter condition
    fn simplify_filter_arithmetic_condition(&self, predicate: &Condition) -> Option<Condition> {
        let expr = predicate.as_expr_unless_true()?;
        let mut rewriter = FilterArithmeticRewriter {};
        let new_expr = rewriter.rewrite_expr(expr.clone());

        if new_expr != expr {
            Some(Condition::with_expr(new_expr))
        } else {
            None
        }
    }
}

/// Filter arithmetic simplification rewriter: simplifies `(col op const) = const2` to `col = (const2 reverse_op const)`
struct FilterArithmeticRewriter {}

impl ExprRewriter for FilterArithmeticRewriter {
    fn rewrite_function_call(&mut self, func_call: FunctionCall) -> ExprImpl {
        use ExprType::{
            Equal, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, NotEqual,
        };

        // Check if this is a comparison operation
        match func_call.func_type() {
            Equal | NotEqual | LessThan | LessThanOrEqual | GreaterThan | GreaterThanOrEqual => {
                let inputs = func_call.inputs();
                if inputs.len() == 2 {
                    // Check if left operand is an arithmetic expression and right operand is a constant
                    if let ExprImpl::FunctionCall(left_func) = &inputs[0]
                        && inputs[1].is_const()
                            && let Some(simplified) = self.simplify_arithmetic_comparison(
                                left_func,
                                &inputs[1],
                                func_call.func_type(),
                            ) {
                                return simplified;
                            }
                }
            }
            _ => {}
        }

        // Recursively handle sub-expressions
        let (func_type, inputs, ret_type) = func_call.decompose();
        let new_inputs: Vec<_> = inputs
            .into_iter()
            .map(|input| self.rewrite_expr(input))
            .collect();

        FunctionCall::new_unchecked(func_type, new_inputs, ret_type).into()
    }
}

impl FilterArithmeticRewriter {
    /// Simplify arithmetic comparison: `(col op const1) comp const2` -> `col comp (const2 reverse_op const1)`
    fn simplify_arithmetic_comparison(
        &self,
        arithmetic_func: &FunctionCall,
        comparison_const: &ExprImpl,
        comparison_op: ExprType,
    ) -> Option<ExprImpl> {
        use ExprType::{Add, Subtract};

        // Check arithmetic operation
        match arithmetic_func.func_type() {
            Add | Subtract => {
                let inputs = arithmetic_func.inputs();
                if inputs.len() == 2 {
                    // Find column reference and constant
                    let (column_ref, arith_const, reverse_op) = if inputs[1].is_const() {
                        // col op const
                        let reverse_op = match arithmetic_func.func_type() {
                            Add => Subtract,
                            Subtract => Add,
                            _ => unreachable!(),
                        };
                        (&inputs[0], &inputs[1], reverse_op)
                    } else if inputs[0].is_const() && arithmetic_func.func_type() == Add {
                        // const + col
                        (&inputs[1], &inputs[0], Subtract)
                    } else {
                        return None;
                    };

                    // Calculate new constant value
                    if let Ok(new_const_func) = FunctionCall::new(
                        reverse_op,
                        vec![comparison_const.clone(), arith_const.clone()],
                    ) {
                        let new_const_expr: ExprImpl = new_const_func.into();
                        // Try constant folding
                        if let Some(Ok(Some(folded_value))) = new_const_expr.try_fold_const() {
                            let new_const =
                                Literal::new(Some(folded_value), new_const_expr.return_type())
                                    .into();

                            // Construct new comparison expression
                            if let Ok(new_comparison) = FunctionCall::new(
                                comparison_op,
                                vec![column_ref.clone(), new_const],
                            ) {
                                return Some(new_comparison.into());
                            }
                        }
                    }
                }
            }
            _ => {}
        }

        None
    }
}

/// Returns `None` if the conditions are too complex or invalid. `Some((limit, offset))` otherwise.
fn handle_rank_preds(rank_preds: &[ExprImpl], window_func_pos: usize) -> Option<(u64, u64)> {
    if rank_preds.is_empty() {
        return None;
    }

    // rank >= lb
    let mut lb: Option<i64> = None;
    // rank <= ub
    let mut ub: Option<i64> = None;
    // rank == eq
    let mut eq: Option<i64> = None;

    for cond in rank_preds {
        if let Some((input_ref, cmp, v)) = cond.as_comparison_const() {
            assert_eq!(input_ref.index, window_func_pos);
            let v = v.cast_implicit(DataType::Int64).ok()?.fold_const().ok()??;
            let v = *v.as_int64();
            match cmp {
                ExprType::LessThanOrEqual => ub = ub.map_or(Some(v), |ub| Some(ub.min(v))),
                ExprType::LessThan => ub = ub.map_or(Some(v - 1), |ub| Some(ub.min(v - 1))),
                ExprType::GreaterThan => lb = lb.map_or(Some(v + 1), |lb| Some(lb.max(v + 1))),
                ExprType::GreaterThanOrEqual => lb = lb.map_or(Some(v), |lb| Some(lb.max(v))),
                _ => unreachable!(),
            }
        } else if let Some((input_ref, v)) = cond.as_eq_const() {
            assert_eq!(input_ref.index, window_func_pos);
            let v = v.cast_implicit(DataType::Int64).ok()?.fold_const().ok()??;
            let v = *v.as_int64();
            if let Some(eq) = eq
                && eq != v
            {
                tracing::warn!(
                    "Failed to optimize rank predicate with conflicting equal conditions."
                );
                return None;
            }
            eq = Some(v)
        } else {
            // TODO: support between and in
            tracing::warn!("Failed to optimize complex rank predicate {:?}", cond);
            return None;
        }
    }

    // Note: rank functions start from 1
    if let Some(eq) = eq {
        if eq < 1 {
            tracing::warn!(
                "Failed to optimize rank predicate with invalid predicate rank={}.",
                eq
            );
            return None;
        }
        let lb = lb.unwrap_or(i64::MIN);
        let ub = ub.unwrap_or(i64::MAX);
        if !(lb <= eq && eq <= ub) {
            tracing::warn!("Failed to optimize rank predicate with conflicting bounds.");
            return None;
        }
        Some((1, (eq - 1) as u64))
    } else {
        match (lb, ub) {
            (Some(lb), Some(ub)) => Some(((ub - lb + 1).max(0) as u64, (lb - 1).max(0) as u64)),
            (Some(lb), None) => Some((LIMIT_ALL_COUNT, (lb - 1).max(0) as u64)),
            (None, Some(ub)) => Some((ub.max(0) as u64, 0)),
            (None, None) => unreachable!(),
        }
    }
}

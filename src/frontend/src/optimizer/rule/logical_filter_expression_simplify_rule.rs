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

use std::collections::HashSet;

use fixedbitset::FixedBitSet;
use risingwave_common::types::{DataType, ScalarImpl};

use crate::expr::{Expr, ExprImpl, ExprRewriter, ExprType, FunctionCall};
use crate::optimizer::PlanRef;
use crate::optimizer::plan_expr_visitor::strong::Strong;
use crate::optimizer::plan_node::{ExprRewritable, LogicalFilter, PlanTreeNodeUnary};
use crate::optimizer::rule::{BoxedRule, Rule};
use crate::utils::Condition;

/// Specially for the predicate and sub-nodes under `LogicalFilter`
/// note that we will rewrite everything for the `LogicalFilter`
/// include `input` and `predicate`
pub struct LogicalFilterExpressionSimplifyRule {}
impl Rule for LogicalFilterExpressionSimplifyRule {
    /// The pattern we aim to optimize, e.g.,
    /// 1. (NOT (e)) OR (e) => True
    /// 2. (NOT (e)) AND (e) => False
    ///
    /// NOTE: `e` should only contain at most a single column
    /// otherwise we will not conduct the optimization
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let filter: &LogicalFilter = plan.as_logical_filter()?;

        // rewrite the entire condition first
        // i.e., the specific optimization that will apply to the entire condition
        // e.g., `e and not(e)`
        let predicate = ConditionRewriter::rewrite(filter.predicate().clone());

        // construct the new filter after predicate rewriting
        let filter = LogicalFilter::create(filter.input(), predicate);

        // then rewrite single expression via `rewrite_exprs`
        let mut expr_rewriter = ExpressionSimplifyRewriter {};
        Some(filter.rewrite_exprs(&mut expr_rewriter))
    }
}

impl LogicalFilterExpressionSimplifyRule {
    pub fn create() -> BoxedRule {
        Box::new(LogicalFilterExpressionSimplifyRule {})
    }
}

/// `True` => the return value of the input `func_type` will *definitely* not be null
/// `False` => vice versa
fn is_not_null(func_type: ExprType) -> bool {
    func_type == ExprType::IsNull
        || func_type == ExprType::IsNotNull
        || func_type == ExprType::IsTrue
        || func_type == ExprType::IsFalse
        || func_type == ExprType::IsNotTrue
        || func_type == ExprType::IsNotFalse
}

/// Simply extract every possible `InputRef` out from the input `expr`
fn extract_column(expr: ExprImpl, columns: &mut Vec<ExprImpl>) {
    match expr.clone() {
        ExprImpl::FunctionCall(func_call) => {
            // the functions that *never* return null will be ignored
            if is_not_null(func_call.func_type()) {
                return;
            }
            for sub_expr in func_call.inputs() {
                extract_column(sub_expr.clone(), columns);
            }
        }
        ExprImpl::InputRef(_) => {
            if !columns.contains(&expr) {
                // only add the column if not exists
                columns.push(expr);
            }
        }
        _ => (),
    }
}

/// If ever `Not (e)` and `(e)` appear together
/// First return value indicates if the optimizable pattern exist
/// Second return value indicates if the term `e` should be converted to either `IsNotNull` or `IsNull`
/// If so, it will contain the actual wrapper `ExprImpl` for that; otherwise it will be `None`
fn check_optimizable_pattern(e1: ExprImpl, e2: ExprImpl) -> (bool, Option<ExprImpl>) {
    /// Try wrapping inner *column* with `IsNotNull`
    fn try_wrap_inner_expression(expr: ExprImpl) -> Option<ExprImpl> {
        let mut columns = vec![];

        extract_column(expr, &mut columns);

        assert!(columns.len() <= 1, "should only contain a single column");

        if columns.is_empty() {
            return None;
        }

        // From `c1` to `IsNotNull(c1)`
        let Ok(expr) = FunctionCall::new(ExprType::IsNotNull, vec![columns[0].clone()]) else {
            return None;
        };

        Some(expr.into())
    }

    // Due to constant folding, we only need to consider `FunctionCall` here (presumably)
    let ExprImpl::FunctionCall(e1_func) = e1.clone() else {
        return (false, None);
    };
    let ExprImpl::FunctionCall(e2_func) = e2.clone() else {
        return (false, None);
    };

    // No chance to optimize
    if e1_func.func_type() != ExprType::Not && e2_func.func_type() != ExprType::Not {
        return (false, None);
    }

    if e1_func.func_type() != ExprType::Not {
        // (e1) [op] (Not (e2))
        if e2_func.inputs().len() != 1 {
            // `not` should only have a single operand, which is `e2` in this case
            return (false, None);
        }
        (
            e1 == e2_func.inputs()[0].clone(),
            try_wrap_inner_expression(e1),
        )
    } else {
        // (Not (e1)) [op] (e2)
        if e1_func.inputs().len() != 1 {
            return (false, None);
        }
        (
            e2 == e1_func.inputs()[0].clone(),
            try_wrap_inner_expression(e2),
        )
    }
}

/// 1. True or (...) | (...) or True => True
/// 2. False and (...) | (...) and False => False
///
/// NOTE: the `True` and `False` here not only represent a single `ExprImpl::Literal`
/// but represent every `ExprImpl` that can be *evaluated* to `ScalarImpl::Bool`
/// during optimization phase as well
fn check_special_pattern(e1: ExprImpl, e2: ExprImpl, op: ExprType) -> Option<bool> {
    fn check_special_pattern_inner(e: ExprImpl, op: ExprType) -> Option<bool> {
        let Some(Ok(Some(scalar))) = e.try_fold_const() else {
            return None;
        };
        match op {
            ExprType::Or => {
                if scalar == ScalarImpl::Bool(true) {
                    Some(true)
                } else {
                    None
                }
            }
            ExprType::And => {
                if scalar == ScalarImpl::Bool(false) {
                    Some(false)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    if e1.is_const()
        && let Some(res) = check_special_pattern_inner(e1, op) {
            return Some(res);
        }

    if e2.is_literal()
        && let Some(res) = check_special_pattern_inner(e2, op) {
            return Some(res);
        }

    None
}

pub struct ExpressionSimplifyRewriter {}

impl ExpressionSimplifyRewriter {
    /// Should only be used in `predicate_pushdown_inner`
    pub fn rewrite_cond(&mut self, expr: ExprImpl) -> ExprImpl {
        self.rewrite_expr(expr)
    }
}

impl ExprRewriter for ExpressionSimplifyRewriter {
    fn rewrite_expr(&mut self, expr: ExprImpl) -> ExprImpl {
        // Check if the input expression is *definitely* null
        let mut columns = vec![];
        extract_column(expr.clone(), &mut columns);

        // NOTE: we do NOT optimize cases that involve multiple columns
        // for detailed reference: <https://github.com/risingwavelabs/risingwave/pull/15275#issuecomment-1975783856>
        if columns.len() > 1 {
            return expr;
        }

        // Eliminate the case where the current expression
        // will definitely return null by using `Strong::is_null`
        if !columns.is_empty() {
            let ExprImpl::InputRef(input_ref) = columns[0].clone() else {
                return expr;
            };
            let index = input_ref.index();
            let fixedbitset = FixedBitSet::with_capacity(index);
            if Strong::is_null(&expr, fixedbitset) {
                return ExprImpl::literal_bool(false);
            }
        }

        let ExprImpl::FunctionCall(func_call) = expr.clone() else {
            return expr;
        };
        if func_call.func_type() != ExprType::Or && func_call.func_type() != ExprType::And {
            return expr;
        }
        assert_eq!(func_call.return_type(), DataType::Boolean);
        // Sanity check, the inputs should only contain two branches
        if func_call.inputs().len() != 2 {
            return expr;
        }

        let inputs = func_call.inputs();
        let e1 = inputs[0].clone();
        let e2 = inputs[1].clone();

        // Eliminate special pattern
        if let Some(res) = check_special_pattern(e1.clone(), e2.clone(), func_call.func_type()) {
            return ExprImpl::literal_bool(res);
        }

        let (optimizable_flag, column) = check_optimizable_pattern(e1, e2);
        if optimizable_flag {
            match func_call.func_type() {
                ExprType::Or => {
                    if let Some(column) = column {
                        // IsNotNull(col)
                        column
                    } else {
                        ExprImpl::literal_bool(true)
                    }
                }
                // `AND` will always be false, no matter the underlying columns are null or not
                // i.e., for `(Not (e)) AND (e)`, since this is filter simplification,
                // whether `e` is null or not does NOT matter
                ExprType::And => ExprImpl::literal_bool(false),
                _ => expr,
            }
        } else {
            expr
        }
    }
}

/// The condition rewriter *only* used by `LogicalScan` under `LogicalFilter`
/// we do not directly rewrite in `condition.rs` (i.e., the `simplify`) since
/// this should only apply to the predicate, otherwise it may be *semantically*
/// wrong especially when `null` is involved.
///
/// consider the following example, e.g.,
/// ```sql
///     create table t1 (c1 INT nullable);
///     select c1 from t1 where not(c1 > 1) and (c1 > 1);
/// ```
///
/// we can easily replace this predicate with `Condition::false_cond`,
/// since both `null` and `false` will be *filtered* out
/// from a `where clause / filter`'s perspective.
struct ConditionRewriter {}
impl ConditionRewriter {
    pub fn rewrite(condition: Condition) -> Condition {
        let conjunctions = condition.conjunctions.clone();
        let mut set = HashSet::new();
        let mut not_set = HashSet::new();
        for expr in conjunctions {
            let e;
            let mut not_flag = false;
            if let ExprImpl::FunctionCall(func_call) = expr.clone() {
                // Not(e)
                if func_call.func_type() == ExprType::Not && func_call.inputs().len() == 1 {
                    e = func_call.inputs()[0].clone();
                    not_set.insert(e.clone());
                    not_flag = true;
                } else {
                    e = expr;
                }
            } else {
                e = expr;
            }
            if (!not_flag && not_set.contains(&e)) || (not_flag && set.contains(&e)) {
                return Condition::false_cond();
            } else if !not_flag {
                set.insert(e);
            }
        }
        condition
    }
}

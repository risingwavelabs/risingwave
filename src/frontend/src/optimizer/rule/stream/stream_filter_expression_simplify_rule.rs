// Copyright 2024 RisingWave Labs
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
use risingwave_connector::source::DataType;

use crate::expr::{
    Expr, ExprImpl, ExprRewriter, FunctionCall,
};
use crate::expr::ExprType;
use crate::optimizer::plan_expr_visitor::strong::Strong;
use crate::optimizer::plan_node::{ExprRewritable, LogicalFilter, LogicalShare, PlanTreeNodeUnary};
use crate::optimizer::rule::{Rule, BoxedRule};
use crate::optimizer::PlanRef;

pub struct StreamFilterExpressionSimplifyRule {}
impl Rule for StreamFilterExpressionSimplifyRule {
    /// The pattern we aim to optimize, e.g.,
    /// 1. (NOT (e)) OR (e) => True | (NOT (e)) AND (e) => False
    /// TODO(Zihao): 2. (NOT (e1) AND NOT (e2)) OR (e1 OR e2) => True
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let filter: &LogicalFilter = plan.as_logical_filter()?;
        let mut rewriter = StreamFilterExpressionSimplifyRewriter {};
        let logical_share_plan = filter.input();
        let share: &LogicalShare = logical_share_plan.as_logical_share()?;
        let input = share.input().rewrite_exprs(&mut rewriter);
        let share = LogicalShare::new(input);
        Some(LogicalFilter::create(share.into(), filter.predicate().clone()))
    }
}

impl StreamFilterExpressionSimplifyRule {
    pub fn create() -> BoxedRule {
        Box::new(StreamFilterExpressionSimplifyRule {})
    }
}

fn is_null_or_not_null(func_type: ExprType) -> bool {
    func_type == ExprType::IsNull || func_type == ExprType::IsNotNull
}

/// Simply extract every possible `InputRef` out from the input `expr`
fn extract_column(expr: ExprImpl, columns: &mut Vec<ExprImpl>) {
    match expr {
        ExprImpl::FunctionCall(func_call) => {
            // `IsNotNull( ... )` or `IsNull( ... )` will be ignored
            if is_null_or_not_null(func_call.func_type()) {
                return;
            }
            for sub_expr in func_call.inputs() {
                extract_column(sub_expr.clone(), columns);
            }
        }
        ExprImpl::InputRef(_) => {
            columns.push(expr);
        }
        _ => (),
    }
}

/// If ever `Not (e)` and `(e)` appear together
/// First return value indicates if the optimizable pattern exist
/// Second return value indicates if the term `e` is either `IsNotNull` or `IsNull`
/// If so, it will contain the actual wrapper `ExprImpl` for that; otherwise it will be `None`
fn check_pattern(e1: ExprImpl, e2: ExprImpl) -> (bool, Option<ExprImpl>) {
    /// Try wrapping inner expression with `IsNotNull`
    /// Note: only columns (i.e., `InputRef`) will be extracted and connected via `AND`
    fn try_wrap_inner_expression(expr: ExprImpl) -> Option<ExprImpl> {
        let mut columns = vec![];

        extract_column(expr, &mut columns);
        if columns.is_empty() {
            return None;
        }

        let mut inputs: Vec<ExprImpl> = vec![];
        // From [`c1`, `c2`, ... , `cn`] to [`IsNotNull(c1)`, ... , `IsNotNull(cn)`]
        for column in columns {
            let Ok(expr) = FunctionCall::new(ExprType::IsNotNull, vec![column]) else {
                return None;
            };
            inputs.push(expr.into());
        }

        // Connect them with `AND` if multiple columns are involved
        // i.e., AND [`IsNotNull(c1)`, ... , `IsNotNull(cn)`]
        if inputs.len() > 1 {
            let Ok(expr) = FunctionCall::new(ExprType::And, inputs) else {
                return None;
            };
            Some(expr.into())
        } else {
            Some(inputs[0].clone())
        }
    }

    // Due to constant folding, we only need to consider `FunctionCall` here (presumably)
    let ExprImpl::FunctionCall(e1_func) = e1.clone() else {
        return (false, None);
    };
    let ExprImpl::FunctionCall(e2_func) = e2.clone() else {
        return (false, None);
    };
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

struct StreamFilterExpressionSimplifyRewriter {}
impl ExprRewriter for StreamFilterExpressionSimplifyRewriter {
    fn rewrite_expr(&mut self, expr: ExprImpl) -> ExprImpl {
        // Check if the input expression is *definitely* null
        let mut columns = vec![];
        extract_column(expr.clone(), &mut columns);
        let max_col_index = columns
            .iter()
            .map(|e| {
                let ExprImpl::InputRef(input_ref) = e else {
                    return 0;
                };
                input_ref.index()
            })
            .max()
            .unwrap_or(0);
        let fixedbitset = FixedBitSet::with_capacity(max_col_index);
        if Strong::is_null(&expr, fixedbitset) {
            return ExprImpl::literal_bool(false);
        }

        let ExprImpl::FunctionCall(func_call) = expr.clone() else {
            return expr;
        };
        if func_call.func_type() != ExprType::Or && func_call.func_type() != ExprType::And {
            return expr;
        }
        assert_eq!(func_call.return_type(), DataType::Boolean);
        // Currently just optimize the first rule
        if func_call.inputs().len() != 2 {
            return expr;
        }
        let inputs = func_call.inputs();
        let (optimizable_flag, columns) = check_pattern(inputs[0].clone(), inputs[1].clone());
        if optimizable_flag {
            match func_call.func_type() {
                ExprType::Or => {
                    if let Some(columns) = columns {
                        columns
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
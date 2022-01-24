use bit_set::BitSet;

use super::BoundExprImpl;
use crate::expr::ExprType;

fn to_conjunctions_inner(expr: BoundExprImpl, rets: &mut Vec<BoundExprImpl>) {
    match expr {
        BoundExprImpl::FunctionCall(func_call) if func_call.get_expr_type() == ExprType::And => {
            let (_, exprs) = func_call.decompose();
            for expr in exprs.into_iter() {
                to_conjunctions_inner(expr, rets);
            }
        }
        _ => rets.push(expr),
    }
}

/// give a bool expression, and transform it to Conjunctive form. e.g. given expression is
/// (expr1 AND expr2 AND expr3) the function will return Vec[expr1, expr2, expr3].
pub fn to_conjunctions(expr: BoundExprImpl) -> Vec<BoundExprImpl> {
    let mut rets = vec![];
    to_conjunctions_inner(expr, &mut rets);
    // TODO: extract common factors fromm the conjunctions with OR expression.
    // e.g: transform (a AND ((b AND c) OR (b AND d))) to (a AND b AND (c OR d))
    rets
}

/// give a expression, and get all columns in its input_ref expressions.
pub fn get_col_refs(_expr: &BoundExprImpl) -> BitSet {
    todo!()
}
/// give a expression, and check all columns in its input_ref expressions less than the input
/// column number.
pub fn assert_input_ref(expr: &BoundExprImpl, input_col_num: usize) {
    let cols = get_col_refs(expr);
    for col in cols.iter() {
        assert!(col < input_col_num);
    }
}

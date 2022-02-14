use fixedbitset::FixedBitSet;

use super::ExprImpl;
use crate::expr::ExprType;

fn to_conjunctions_inner(expr: ExprImpl, rets: &mut Vec<ExprImpl>) {
    match expr {
        ExprImpl::FunctionCall(func_call) if func_call.get_expr_type() == ExprType::And => {
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
pub fn to_conjunctions(expr: ExprImpl) -> Vec<ExprImpl> {
    let mut rets = vec![];
    to_conjunctions_inner(expr, &mut rets);
    // TODO: extract common factors fromm the conjunctions with OR expression.
    // e.g: transform (a AND ((b AND c) OR (b AND d))) to (a AND b AND (c OR d))
    rets
}

/// give a expression, and get all columns in its input_ref expressions.
/// **Panics** if **bit** is out of bounds of the FixedBitSet.
pub fn get_inputs_col_index(expr: &ExprImpl, cols: &mut FixedBitSet) {
    match expr {
        ExprImpl::FunctionCall(func_call) => {
            for expr in func_call.inputs() {
                get_inputs_col_index(expr, cols);
            }
        }
        ExprImpl::AggCall(agg_call) => {
            for expr in agg_call.inputs() {
                get_inputs_col_index(expr, cols);
            }
        }
        ExprImpl::InputRef(input_ref) => cols.insert(input_ref.index()),
        _ => {}
    }
}
/// give a expression, and check all columns in its input_ref expressions less than the input
/// column number.
pub fn assert_input_ref(expr: &ExprImpl, input_col_num: usize) {
    let mut cols = FixedBitSet::with_capacity(input_col_num);
    let _cols = get_inputs_col_index(expr, &mut cols);
}

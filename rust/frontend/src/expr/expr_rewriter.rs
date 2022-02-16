use super::{AggCall, Expr, ExprImpl, FunctionCall, InputRef, Literal};

pub trait ExprRewriter {
    fn rewrite_expr(&mut self, expr: ExprImpl) -> ExprImpl {
        match expr {
            ExprImpl::InputRef(inner) => self.rewrite_input_ref(*inner).bound_expr(),
            ExprImpl::Literal(inner) => self.rewrite_literal(*inner).bound_expr(),
            ExprImpl::FunctionCall(inner) => self.rewrite_function_call(*inner).bound_expr(),
            ExprImpl::AggCall(inner) => self.rewrite_agg_call(*inner).bound_expr(),
        }
    }
    fn rewrite_function_call(&mut self, func_call: FunctionCall) -> FunctionCall {
        let (func_type, inputs) = func_call.decompose();
        let inputs = inputs
            .into_iter()
            .map(|expr| self.rewrite_expr(expr))
            .collect();
        FunctionCall::new(func_type, inputs).unwrap()
    }
    fn rewrite_agg_call(&mut self, agg_call: AggCall) -> AggCall {
        let (func_type, inputs) = agg_call.decompose();
        let inputs = inputs
            .into_iter()
            .map(|expr| self.rewrite_expr(expr))
            .collect();
        AggCall::new(func_type, inputs).unwrap()
    }
    fn rewrite_literal(&mut self, literal: Literal) -> Literal {
        literal
    }
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> InputRef {
        input_ref
    }
}

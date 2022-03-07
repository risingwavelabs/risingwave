use super::{AggCall, ExprImpl, FunctionCall, InputRef, Literal};

pub trait ExprRewriter {
    fn rewrite_expr(&mut self, expr: ExprImpl) -> ExprImpl {
        match expr {
            ExprImpl::InputRef(inner) => self.rewrite_input_ref(*inner),
            ExprImpl::Literal(inner) => self.rewrite_literal(*inner),
            ExprImpl::FunctionCall(inner) => self.rewrite_function_call(*inner),
            ExprImpl::AggCall(inner) => self.rewrite_agg_call(*inner),
        }
    }
    fn rewrite_function_call(&mut self, func_call: FunctionCall) -> ExprImpl {
        let (func_type, inputs) = func_call.decompose();
        let inputs = inputs
            .into_iter()
            .map(|expr| self.rewrite_expr(expr))
            .collect();
        FunctionCall::new(func_type, inputs).unwrap().into()
    }
    fn rewrite_agg_call(&mut self, agg_call: AggCall) -> ExprImpl {
        let (func_type, inputs) = agg_call.decompose();
        let inputs = inputs
            .into_iter()
            .map(|expr| self.rewrite_expr(expr))
            .collect();
        AggCall::new(func_type, inputs).unwrap().into()
    }
    fn rewrite_literal(&mut self, literal: Literal) -> ExprImpl {
        literal.into()
    }
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        input_ref.into()
    }
}

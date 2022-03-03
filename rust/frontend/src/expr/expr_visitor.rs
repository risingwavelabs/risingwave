use super::{AggCall, ExprImpl, FunctionCall, InputRef, Literal};

pub trait ExprVisitor {
    fn visit_expr(&mut self, expr: &ExprImpl) {
        match expr {
            ExprImpl::InputRef(inner) => self.visit_input_ref(inner),
            ExprImpl::Literal(inner) => self.visit_literal(inner),
            ExprImpl::FunctionCall(inner) => self.visit_function_call(inner),
            ExprImpl::AggCall(inner) => self.visit_agg_call(inner),
        }
    }
    fn visit_function_call(&mut self, func_call: &FunctionCall) {
        func_call
            .inputs()
            .iter()
            .for_each(|expr| self.visit_expr(expr))
    }
    fn visit_agg_call(&mut self, agg_call: &AggCall) {
        agg_call
            .inputs()
            .iter()
            .for_each(|expr| self.visit_expr(expr))
    }
    fn visit_literal(&mut self, _: &Literal) {}
    fn visit_input_ref(&mut self, _: &InputRef) {}
}

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

use super::{
    AggCall, CorrelatedInputRef, ExprImpl, FunctionCall, InputRef, Literal, Subquery,
    TableFunction, WindowFunction,
};

/// with the same visit logic of `ExprVisitor`, but mutable.
pub trait ExprMutator {
    fn visit_expr(&mut self, expr: &mut ExprImpl) {
        match expr {
            ExprImpl::InputRef(inner) => self.visit_input_ref(inner),
            ExprImpl::Literal(inner) => self.visit_literal(inner),
            ExprImpl::FunctionCall(inner) => self.visit_function_call(inner),
            ExprImpl::AggCall(inner) => self.visit_agg_call(inner),
            ExprImpl::Subquery(inner) => self.visit_subquery(inner),
            ExprImpl::CorrelatedInputRef(inner) => self.visit_correlated_input_ref(inner),
            ExprImpl::TableFunction(inner) => self.visit_table_function(inner),
            ExprImpl::WindowFunction(inner) => self.visit_window_function(inner),
        }
    }
    fn visit_function_call(&mut self, func_call: &mut FunctionCall) {
        func_call
            .inputs_mut()
            .iter_mut()
            .for_each(|expr| self.visit_expr(expr))
    }
    fn visit_agg_call(&mut self, agg_call: &mut AggCall) {
        agg_call
            .inputs_mut()
            .iter_mut()
            .for_each(|expr| self.visit_expr(expr));
        agg_call.order_by_mut().visit_expr_mut(self);
        agg_call.filter_mut().visit_expr_mut(self);
    }
    fn visit_literal(&mut self, _: &mut Literal) {}
    fn visit_input_ref(&mut self, _: &mut InputRef) {}
    fn visit_subquery(&mut self, _: &mut Subquery) {}
    fn visit_correlated_input_ref(&mut self, _: &mut CorrelatedInputRef) {}
    fn visit_table_function(&mut self, func_call: &mut TableFunction) {
        func_call
            .args
            .iter_mut()
            .for_each(|expr| self.visit_expr(expr))
    }
    fn visit_window_function(&mut self, func_call: &mut WindowFunction) {
        func_call
            .args
            .iter_mut()
            .for_each(|expr| self.visit_expr(expr));
    }
}

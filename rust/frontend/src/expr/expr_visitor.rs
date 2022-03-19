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
//
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

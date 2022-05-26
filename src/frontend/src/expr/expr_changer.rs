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

use super::{AggCall, CorrelatedInputRef, ExprImpl, FunctionCall, InputRef, Literal, Subquery};

/// Traverse an expression tree.
///
/// Each method of the trait is a hook that can be overridden to customize the behavior when
/// traversing the corresponding type of node. By default, every method recursively changes the
/// subtree.
///
/// Note: The default implementation for `change_subquery` is a no-op, i.e., expressions inside
/// subqueries are not traversed.
pub trait ExprChanger {
    fn change_expr(&mut self, expr: &mut ExprImpl) {
        match expr {
            ExprImpl::InputRef(inner) => self.change_input_ref(inner),
            ExprImpl::Literal(inner) => self.change_literal(inner),
            ExprImpl::FunctionCall(inner) => self.change_function_call(inner),
            ExprImpl::AggCall(inner) => self.change_agg_call(inner),
            ExprImpl::Subquery(inner) => self.change_subquery(inner),
            ExprImpl::CorrelatedInputRef(inner) => self.change_correlated_input_ref(inner),
        }
    }
    fn change_function_call(&mut self, func_call: &mut FunctionCall) {
        func_call
            .inputs
            .iter_mut()
            .for_each(|expr| self.change_expr(expr))
    }
    fn change_agg_call(&mut self, agg_call: &mut AggCall) {
        agg_call
            .inputs
            .iter_mut()
            .for_each(|expr| self.change_expr(expr))
    }
    fn change_literal(&mut self, _: &mut Literal) {}
    fn change_input_ref(&mut self, _: &mut InputRef) {}
    fn change_subquery(&mut self, _: &mut Subquery) {}
    fn change_correlated_input_ref(&mut self, _: &mut CorrelatedInputRef) {}
}

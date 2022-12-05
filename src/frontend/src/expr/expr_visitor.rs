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

/// Traverse an expression tree.
///
/// Each method of the trait is a hook that can be overridden to customize the behavior when
/// traversing the corresponding type of node. By default, every method recursively visits the
/// subtree.
///
/// Note: The default implementation for `visit_subquery` is a no-op, i.e., expressions inside
/// subqueries are not traversed.
pub trait ExprVisitor<R: Default> {
    /// This merge function is used to reduce results of expr inputs.
    /// In order to always remind users to implement themselves, we don't provide an default
    /// implementation.
    fn merge(a: R, b: R) -> R;

    fn visit_expr(&mut self, expr: &ExprImpl) -> R {
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
    fn visit_function_call(&mut self, func_call: &FunctionCall) -> R {
        func_call
            .inputs()
            .iter()
            .map(|expr| self.visit_expr(expr))
            .reduce(Self::merge)
            .unwrap_or_default()
    }
    fn visit_agg_call(&mut self, agg_call: &AggCall) -> R {
        let mut r = agg_call
            .inputs()
            .iter()
            .map(|expr| self.visit_expr(expr))
            .reduce(Self::merge)
            .unwrap_or_default();
        r = Self::merge(r, agg_call.order_by().visit_expr(self));
        r = Self::merge(r, agg_call.filter().visit_expr(self));
        r
    }
    fn visit_literal(&mut self, _: &Literal) -> R {
        R::default()
    }
    fn visit_input_ref(&mut self, _: &InputRef) -> R {
        R::default()
    }
    fn visit_subquery(&mut self, _: &Subquery) -> R {
        R::default()
    }
    fn visit_correlated_input_ref(&mut self, _: &CorrelatedInputRef) -> R {
        R::default()
    }
    fn visit_table_function(&mut self, func_call: &TableFunction) -> R {
        func_call
            .args
            .iter()
            .map(|expr| self.visit_expr(expr))
            .reduce(Self::merge)
            .unwrap_or_default()
    }
    fn visit_window_function(&mut self, func_call: &WindowFunction) -> R {
        func_call
            .args
            .iter()
            .map(|expr| self.visit_expr(expr))
            .reduce(Self::merge)
            .unwrap_or_default()
    }
}

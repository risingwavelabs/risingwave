// Copyright 2023 RisingWave Labs
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

use super::{
    AggCall, CorrelatedInputRef, ExprImpl, FunctionCall, FunctionCallWithLambda, InputRef, Literal,
    Now, Parameter, Subquery, TableFunction, UserDefinedFunction, WindowFunction,
};

/// Traverse an expression tree.
///
/// Each method of the trait is a hook that can be overridden to customize the behavior when
/// traversing the corresponding type of node. By default, every method recursively visits the
/// subtree.
///
/// Note: The default implementation for `visit_subquery` is a no-op, i.e., expressions inside
/// subqueries are not traversed.
pub trait ExprVisitor: Sized {
    type Result: Default;

    /// This merge function is used to reduce results of expr inputs.
    /// In order to always remind users to implement themselves, we don't provide an default
    /// implementation.
    fn merge(a: Self::Result, b: Self::Result) -> Self::Result;

    fn visit_expr(&mut self, expr: &ExprImpl) -> Self::Result {
        match expr {
            ExprImpl::InputRef(inner) => self.visit_input_ref(inner),
            ExprImpl::Literal(inner) => self.visit_literal(inner),
            ExprImpl::FunctionCall(inner) => self.visit_function_call(inner),
            ExprImpl::FunctionCallWithLambda(inner) => self.visit_function_call_with_lambda(inner),
            ExprImpl::AggCall(inner) => self.visit_agg_call(inner),
            ExprImpl::Subquery(inner) => self.visit_subquery(inner),
            ExprImpl::CorrelatedInputRef(inner) => self.visit_correlated_input_ref(inner),
            ExprImpl::TableFunction(inner) => self.visit_table_function(inner),
            ExprImpl::WindowFunction(inner) => self.visit_window_function(inner),
            ExprImpl::UserDefinedFunction(inner) => self.visit_user_defined_function(inner),
            ExprImpl::Parameter(inner) => self.visit_parameter(inner),
            ExprImpl::Now(inner) => self.visit_now(inner),
        }
    }
    fn visit_function_call(&mut self, func_call: &FunctionCall) -> Self::Result {
        default_visit_function_call(self, func_call)
    }
    fn visit_function_call_with_lambda(
        &mut self,
        func_call: &FunctionCallWithLambda,
    ) -> Self::Result {
        self.visit_function_call(func_call.base())
    }
    fn visit_agg_call(&mut self, agg_call: &AggCall) -> Self::Result {
        let mut r = agg_call
            .args()
            .iter()
            .map(|expr| self.visit_expr(expr))
            .reduce(Self::merge)
            .unwrap_or_default();
        r = Self::merge(r, agg_call.order_by().visit_expr(self));
        r = Self::merge(r, agg_call.filter().visit_expr(self));
        r
    }
    fn visit_parameter(&mut self, _: &Parameter) -> Self::Result {
        Self::Result::default()
    }
    fn visit_literal(&mut self, _: &Literal) -> Self::Result {
        Self::Result::default()
    }
    fn visit_input_ref(&mut self, _: &InputRef) -> Self::Result {
        Self::Result::default()
    }
    fn visit_subquery(&mut self, _: &Subquery) -> Self::Result {
        Self::Result::default()
    }
    fn visit_correlated_input_ref(&mut self, _: &CorrelatedInputRef) -> Self::Result {
        Self::Result::default()
    }
    fn visit_table_function(&mut self, func_call: &TableFunction) -> Self::Result {
        func_call
            .args
            .iter()
            .map(|expr| self.visit_expr(expr))
            .reduce(Self::merge)
            .unwrap_or_default()
    }
    fn visit_window_function(&mut self, func_call: &WindowFunction) -> Self::Result {
        func_call
            .args
            .iter()
            .map(|expr| self.visit_expr(expr))
            .reduce(Self::merge)
            .unwrap_or_default()
    }
    fn visit_user_defined_function(&mut self, func_call: &UserDefinedFunction) -> Self::Result {
        func_call
            .args
            .iter()
            .map(|expr| self.visit_expr(expr))
            .reduce(Self::merge)
            .unwrap_or_default()
    }
    fn visit_now(&mut self, _: &Now) -> Self::Result {
        Self::Result::default()
    }
}

/// The default implementation of [`ExprVisitor::visit_function_call`].
//
/// Provide this function as an alternative to the feature [calling_default_trait_methods][calling_default_trait_methods].
/// When the override implementation wants to call the default implementation, this method can be used.
///
/// [calling_default_trait_methods]: https://github.com/rust-lang/rfcs/pull/3329
pub fn default_visit_function_call<V: ExprVisitor>(
    visitor: &mut V,
    func_call: &FunctionCall,
) -> V::Result {
    func_call
        .inputs()
        .iter()
        .map(|expr| visitor.visit_expr(expr))
        .reduce(V::merge)
        .unwrap_or_default()
}

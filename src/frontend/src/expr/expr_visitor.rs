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

use itertools::Itertools;

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
pub trait ExprVisitor {
    type Result: Default;

    /// This merge function is used to reduce results of expr inputs.
    /// In order to always remind users to implement themselves, we don't provide an default
    /// implementation.
    fn merge(&self, a: Self::Result, b: Self::Result) -> Self::Result;

    #[allow(clippy::type_complexity)]
    fn gen_merge_fn(&self) -> Box<dyn FnMut(Self::Result, Self::Result) -> Self::Result + '_> {
        Box::new(|a: Self::Result, b: Self::Result| -> Self::Result { self.merge(a, b) })
    }

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
        let vec = func_call
            .inputs()
            .iter()
            .map(|expr| self.visit_expr(expr))
            .collect_vec();
        vec.into_iter()
            .reduce(self.gen_merge_fn())
            .unwrap_or_default()
    }
    fn visit_function_call_with_lambda(
        &mut self,
        func_call: &FunctionCallWithLambda,
    ) -> Self::Result {
        self.visit_function_call(func_call.base())
    }
    fn visit_agg_call(&mut self, agg_call: &AggCall) -> Self::Result {
        let vec = agg_call
            .args()
            .iter()
            .map(|expr| self.visit_expr(expr))
            .collect_vec();
        let mut r = vec
            .into_iter()
            .reduce(self.gen_merge_fn())
            .unwrap_or_default();
        let agg_call_order_by_result = agg_call.order_by().visit_expr(self);
        r = self.merge(r, agg_call_order_by_result);
        let agg_call_filter_result = agg_call.filter().visit_expr(self);
        r = self.merge(r, agg_call_filter_result);
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
        let vec = func_call
            .args
            .iter()
            .map(|expr| self.visit_expr(expr))
            .collect_vec();

        vec.into_iter()
            .reduce(self.gen_merge_fn())
            .unwrap_or_default()
    }
    fn visit_window_function(&mut self, func_call: &WindowFunction) -> Self::Result {
        let vec = func_call
            .args
            .iter()
            .map(|expr| self.visit_expr(expr))
            .collect_vec();

        vec.into_iter()
            .reduce(self.gen_merge_fn())
            .unwrap_or_default()
    }
    fn visit_user_defined_function(&mut self, func_call: &UserDefinedFunction) -> Self::Result {
        let vec = func_call
            .args
            .iter()
            .map(|expr| self.visit_expr(expr))
            .collect_vec();

        vec.into_iter()
            .reduce(self.gen_merge_fn())
            .unwrap_or_default()
    }
    fn visit_now(&mut self, _: &Now) -> Self::Result {
        Self::Result::default()
    }
}

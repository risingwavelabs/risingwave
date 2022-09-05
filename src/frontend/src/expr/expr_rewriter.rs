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

/// By default, `ExprRewriter` simply traverses the expression tree and leaves nodes unchanged.
/// Implementations can override a subset of methods and perform transformation on some particular
/// types of expression.
pub trait ExprRewriter {
    fn rewrite_expr(&mut self, expr: ExprImpl) -> ExprImpl {
        match expr {
            ExprImpl::InputRef(inner) => self.rewrite_input_ref(*inner),
            ExprImpl::Literal(inner) => self.rewrite_literal(*inner),
            ExprImpl::FunctionCall(inner) => self.rewrite_function_call(*inner),
            ExprImpl::AggCall(inner) => self.rewrite_agg_call(*inner),
            ExprImpl::Subquery(inner) => self.rewrite_subquery(*inner),
            ExprImpl::CorrelatedInputRef(inner) => self.rewrite_correlated_input_ref(*inner),
            ExprImpl::TableFunction(inner) => self.rewrite_table_function(*inner),
            ExprImpl::WindowFunction(inner) => self.rewrite_window_function(*inner),
        }
    }
    fn rewrite_function_call(&mut self, func_call: FunctionCall) -> ExprImpl {
        let (func_type, inputs, ret) = func_call.decompose();
        let inputs = inputs
            .into_iter()
            .map(|expr| self.rewrite_expr(expr))
            .collect();
        FunctionCall::new_unchecked(func_type, inputs, ret).into()
    }
    fn rewrite_agg_call(&mut self, agg_call: AggCall) -> ExprImpl {
        let (func_type, inputs, distinct, order_by, filter) = agg_call.decompose();
        let inputs = inputs
            .into_iter()
            .map(|expr| self.rewrite_expr(expr))
            .collect();
        let order_by = order_by.rewrite_expr(self);
        let filter = filter.rewrite_expr(self);
        AggCall::new(func_type, inputs, distinct, order_by, filter)
            .unwrap()
            .into()
    }
    fn rewrite_literal(&mut self, literal: Literal) -> ExprImpl {
        literal.into()
    }
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        input_ref.into()
    }
    fn rewrite_subquery(&mut self, subquery: Subquery) -> ExprImpl {
        subquery.into()
    }
    fn rewrite_correlated_input_ref(&mut self, input_ref: CorrelatedInputRef) -> ExprImpl {
        input_ref.into()
    }
    fn rewrite_table_function(&mut self, table_func: TableFunction) -> ExprImpl {
        let TableFunction {
            args,
            return_type,
            function_type,
        } = table_func;
        let args = args
            .into_iter()
            .map(|expr| self.rewrite_expr(expr))
            .collect();
        TableFunction {
            args,
            return_type,
            function_type,
        }
        .into()
    }
    fn rewrite_window_function(&mut self, window_func: WindowFunction) -> ExprImpl {
        let WindowFunction {
            args,
            return_type,
            function_type,
            partition_by,
            order_by,
        } = window_func;
        let args = args
            .into_iter()
            .map(|expr| self.rewrite_expr(expr))
            .collect();
        WindowFunction {
            args,
            return_type,
            function_type,
            partition_by,
            order_by,
        }
        .into()
    }
}

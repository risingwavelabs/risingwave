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

use risingwave_common::error::RwError;

use crate::expr::{Expr, ExprImpl, ExprRewriter, Literal};

pub(crate) struct ConstEvalRewriter {
    pub(crate) error: Option<RwError>,
}
impl ExprRewriter for ConstEvalRewriter {
    fn rewrite_expr(&mut self, expr: ExprImpl) -> ExprImpl {
        if self.error.is_some() {
            return expr;
        }
        if expr.is_const() {
            let data_type = expr.return_type();
            match expr.eval_row_const() {
                Ok(datum) => Literal::new(datum, data_type).into(),
                Err(e) => {
                    self.error = Some(e);
                    expr
                }
            }
        } else {
            match expr {
                ExprImpl::InputRef(inner) => self.rewrite_input_ref(*inner),
                ExprImpl::Literal(inner) => self.rewrite_literal(*inner),
                ExprImpl::FunctionCall(inner) => self.rewrite_function_call(*inner),
                ExprImpl::AggCall(inner) => self.rewrite_agg_call(*inner),
                ExprImpl::Subquery(inner) => self.rewrite_subquery(*inner),
                ExprImpl::CorrelatedInputRef(inner) => self.rewrite_correlated_input_ref(*inner),
                ExprImpl::TableFunction(inner) => self.rewrite_table_function(*inner),
                ExprImpl::WindowFunction(inner) => self.rewrite_window_function(*inner),
                ExprImpl::UserDefinedFunction(inner) => self.rewrite_user_defined_function(*inner),
                ExprImpl::Parameter(_) => unreachable!("Parameter should not appear here. It will be replaced by a literal before this step."),
            }
        }
    }
}

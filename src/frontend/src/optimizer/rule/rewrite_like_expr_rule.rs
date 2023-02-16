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

use core::slice::memchr::memchr;
use std::cmp::min;
use std::str::from_utf8;

use risingwave_common::types::ScalarImpl;
use risingwave_connector::source::DataType;

use super::{BoxedRule, Rule};
use crate::expr::{Expr, ExprImpl, ExprRewriter, ExprType, ExprVisitor, FunctionCall, Literal};
use crate::optimizer::plan_node::{ExprRewritable, LogicalFilter};
use crate::optimizer::PlanRef;

pub struct RewriteLikeExprRule {}
impl Rule for RewriteLikeExprRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let filter: &LogicalFilter = plan.as_logical_filter()?;
        let mut has_like = HasLikeExprVisitor {};
        if filter
            .predicate()
            .conjunctions
            .iter()
            .any(|expr| has_like.visit_expr(expr))
        {
            let mut rewriter = LikeExprRewriter {};
            Some(filter.rewrite_exprs(&mut rewriter))
        } else {
            None
        }
    }
}

struct HasLikeExprVisitor {}

impl ExprVisitor<bool> for HasLikeExprVisitor {
    fn merge(a: bool, b: bool) -> bool {
        a | b
    }

    fn visit_function_call(&mut self, func_call: &FunctionCall) -> bool {
        if func_call.get_expr_type() == ExprType::Like
            && let (_, ExprImpl::InputRef(_), ExprImpl::Literal(_)) = func_call.clone().decompose_as_binary() {
            true
        } else {
            func_call
                .inputs()
                .iter()
                .map(|expr| self.visit_expr(expr))
                .reduce(Self::merge)
                .unwrap_or_default()
        }
    }
}

struct LikeExprRewriter {}

impl ExprRewriter for LikeExprRewriter {
    fn rewrite_function_call(&mut self, func_call: FunctionCall) -> ExprImpl {
        let (func_type, inputs, ret) = func_call.clone().decompose();
        let inputs: Vec<ExprImpl> = inputs
            .into_iter()
            .map(|expr| self.rewrite_expr(expr))
            .collect();
        if func_call.get_expr_type() == ExprType::Like
            && let (_, ExprImpl::InputRef(x), ExprImpl::Literal(y)) = func_call.decompose_as_binary()
            && matches!(y.return_type(), DataType::Varchar) {

            let data = y.get_data();
            if let Some(ScalarImpl::Utf8(data)) = data {
                let bytes = data.as_bytes();
                let len = bytes.len();
                let idx = match (memchr(b'%', bytes), memchr(b'_', bytes)) {
                    (Some(a), Some(b)) => min(a, b),
                    (Some(idx), None) => idx,
                    (None, Some(idx)) => idx,
                    (None, None) => {
                        return FunctionCall::new_unchecked(ExprType::Equal, inputs, ret).into()
                    }
                };

                if idx == 0 {
                    return FunctionCall::new_unchecked(ExprType::Equal, inputs, ret).into()
                }

                let (low, high) = {
                    let low = bytes[0..idx].to_owned();
                    if low[idx - 1] == 255 {
                        return FunctionCall::new_unchecked(func_type, inputs, ret).into();
                    }
                    let mut high = low.clone();
                    high[idx - 1] += 1;
                    match (from_utf8(&low), from_utf8(&high)) {
                        (Ok(low), Ok(high)) => (low.to_owned(), high.to_owned()),
                        _ => {
                            return FunctionCall::new_unchecked(func_type, inputs, ret).into();
                        }
                    }
                };

                let between = FunctionCall::new_unchecked(
                    ExprType::And,
                    vec![
                        FunctionCall::new_unchecked(
                            ExprType::GreaterThanOrEqual,
                            vec![
                                ExprImpl::InputRef(x.clone()),
                                ExprImpl::Literal(Literal::new(Some(ScalarImpl::Utf8(low.into())), DataType::Varchar).into())
                            ],
                            DataType::Boolean)
                            .into(),
                        FunctionCall::new_unchecked(
                            ExprType::LessThanOrEqual,
                            vec![
                                ExprImpl::InputRef(x),
                                ExprImpl::Literal(Literal::new(Some(ScalarImpl::Utf8(high.into())), DataType::Varchar).into())
                            ],
                            DataType::Boolean).into(),
                    ],
                    DataType::Boolean,
                );

                if idx == len - 1 {
                    between.into()
                } else {
                    FunctionCall::new_unchecked(
                        ExprType::And,
                        vec![
                            between.into(),
                            FunctionCall::new_unchecked(func_type, inputs, ret).into(),
                        ],
                        DataType::Boolean,
                    ).into()
                }
            } else {
                FunctionCall::new_unchecked(func_type, inputs, ret).into()
            }
        } else {
            FunctionCall::new_unchecked(func_type, inputs, ret).into()
        }
    }
}

impl RewriteLikeExprRule {
    pub fn create() -> BoxedRule {
        Box::new(RewriteLikeExprRule {})
    }
}

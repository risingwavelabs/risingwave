// Copyright 2023 Singularity Data
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

use risingwave_common::types::DataType;
use risingwave_pb::expr::expr_node::Type;
use risingwave_pb::plan_common::JoinType;

use crate::expr::{ExprRewriter, FunctionCall, InputRef};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{LogicalFilter, LogicalJoin, LogicalNow};
use crate::optimizer::rule::{BoxedRule, Rule};
use crate::optimizer::PlanRef;
use crate::utils::Condition;

/// Convert `LogicalFilter` with now in predicate to left-semi `LogicalJoin`
pub struct FilterWithNowToJoinRule {}
impl Rule for FilterWithNowToJoinRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let filter: &LogicalFilter = plan.as_logical_filter()?;
        // if filter.predicate().conjunctions.iter().(|)

        let lhs_len = filter.base.schema().len();

        let mut now_filters = vec![];
        let mut remainder = vec![];

        // If the `now` is not a valid dynamic filter expression,
        filter.predicate().conjunctions.iter().for_each(|expr| {
            let mut rewriter = NowAsInputRef::new(lhs_len);
            let expr = rewriter.rewrite_expr(expr.clone());
            if rewriter.rewritten {
                now_filters.push(expr);
            } else {
                remainder.push(expr);
            }
        });

        if now_filters.is_empty() {
            return None;
        }
        let mut new_plan = plan.inputs()[0].clone();

        for now_filter in now_filters {
            new_plan = LogicalJoin::new(
                new_plan,
                LogicalNow::new(plan.ctx()).into(),
                JoinType::LeftSemi,
                Condition {
                    conjunctions: vec![now_filter],
                },
            )
            .into()
        }

        if !remainder.is_empty() {
            new_plan = LogicalFilter::new(
                new_plan,
                Condition {
                    conjunctions: remainder,
                },
            )
            .into();
        }

        Some(new_plan)
    }
}

impl FilterWithNowToJoinRule {
    pub fn create() -> BoxedRule {
        Box::new(FilterWithNowToJoinRule {})
    }
}

struct NowAsInputRef {
    index: usize,
    rewritten: bool,
}
impl ExprRewriter for NowAsInputRef {
    fn rewrite_function_call(&mut self, func_call: FunctionCall) -> crate::expr::ExprImpl {
        let (func_type, inputs, ret) = func_call.decompose();
        let inputs = inputs
            .into_iter()
            .map(|expr| self.rewrite_expr(expr))
            .collect();
        match func_type {
            Type::Now => {
                self.rewritten = true;
                InputRef {
                    index: self.index,
                    data_type: DataType::Timestamptz,
                }
                .into()
            }
            _ => FunctionCall::new_unchecked(func_type, inputs, ret).into(),
        }
    }
}

impl NowAsInputRef {
    fn new(lhs_len: usize) -> Self {
        Self {
            index: lhs_len,
            rewritten: false,
        }
    }
}

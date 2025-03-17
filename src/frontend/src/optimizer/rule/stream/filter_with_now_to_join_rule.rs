// Copyright 2025 RisingWave Labs
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
use risingwave_pb::plan_common::JoinType;

use crate::expr::{ExprRewriter, FunctionCall, InputRef};
use crate::optimizer::PlanRef;
use crate::optimizer::plan_node::generic::{self, GenericPlanRef};
use crate::optimizer::plan_node::{LogicalFilter, LogicalJoin, LogicalNow};
use crate::optimizer::property::{analyze_monotonicity, monotonicity_variants};
use crate::optimizer::rule::{BoxedRule, Rule};
use crate::utils::Condition;

/// Convert `LogicalFilter` with now in predicate to left-semi `LogicalJoin`
/// Only applies to stream.
pub struct FilterWithNowToJoinRule {}
impl Rule for FilterWithNowToJoinRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let filter: &LogicalFilter = plan.as_logical_filter()?;

        let lhs_len = filter.base.schema().len();

        let mut now_filters = vec![];
        let mut remainder = vec![];

        // If the `now` is not a valid dynamic filter expression, we will not push it down.
        filter.predicate().conjunctions.iter().for_each(|expr| {
            if let Some((input_expr, cmp, now_expr)) = expr.as_now_comparison_cond() {
                // ensure that this expression is increasing
                use monotonicity_variants::*;
                if matches!(analyze_monotonicity(&now_expr), Inherent(NonDecreasing)) {
                    now_filters.push(
                        FunctionCall::new(cmp, vec![input_expr, now_expr])
                            .unwrap()
                            .into(),
                    );
                } else {
                    remainder.push(expr.clone());
                }
            } else {
                remainder.push(expr.clone());
            }
        });

        // Ignore no now filter
        if now_filters.is_empty() {
            return None;
        }
        let mut new_plan = plan.inputs()[0].clone();

        let mut rewriter = NowAsInputRef::new(lhs_len);
        for now_filter in now_filters {
            let now_filter = rewriter.rewrite_expr(now_filter);
            new_plan = LogicalJoin::new(
                new_plan,
                LogicalNow::new(generic::Now::update_current(plan.ctx())).into(),
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
}
impl ExprRewriter for NowAsInputRef {
    fn rewrite_now(&mut self, _: crate::expr::Now) -> crate::expr::ExprImpl {
        InputRef {
            index: self.index,
            data_type: DataType::Timestamptz,
        }
        .into()
    }
}

impl NowAsInputRef {
    fn new(lhs_len: usize) -> Self {
        Self { index: lhs_len }
    }
}

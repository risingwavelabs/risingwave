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

use risingwave_common::types::DataType;
use risingwave_pb::expr::expr_node::Type;
use risingwave_pb::plan_common::JoinType;

use crate::expr::{try_derive_watermark, ExprRewriter, FunctionCall, InputRef};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{LogicalFilter, LogicalJoin, LogicalNow};
use crate::optimizer::rule::{BoxedRule, Rule};
use crate::optimizer::PlanRef;
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

        let mut rewriter = NowAsInputRef::new(lhs_len);

        // If the `now` is not a valid dynamic filter expression, we will not push it down.
        filter.predicate().conjunctions.iter().for_each(|expr| {
            if let Some((input_expr, cmp, now_expr)) = expr.as_now_comparison_cond() {
                let now_expr = rewriter.rewrite_expr(now_expr);

                // as a sanity check, ensure that this expression will derive a watermark
                // on the output of the now executor
                debug_assert_eq!(try_derive_watermark(&now_expr), Some(lhs_len));

                now_filters.push(FunctionCall::new(cmp, vec![input_expr, now_expr]).unwrap());
            } else {
                remainder.push(expr.clone());
            }
        });

        // We want to put `input_expr >/>= now_expr` before `input_expr </<= now_expr` as the former
        // will introduce a watermark that can reduce state (since `now_expr` is monotonically
        // increasing)
        now_filters.sort_by_key(|l| rank_cmp(l.func_type()));

        // Ignore no now filter & forbid now filters that do not create a watermark
        if now_filters.is_empty()
            || !matches!(
                now_filters[0].func_type(),
                Type::GreaterThan | Type::GreaterThanOrEqual
            )
        {
            return None;
        }
        let mut new_plan = plan.inputs()[0].clone();

        for now_filter in now_filters {
            new_plan = LogicalJoin::new(
                new_plan,
                LogicalNow::new(plan.ctx()).into(),
                JoinType::LeftSemi,
                Condition {
                    conjunctions: vec![now_filter.into()],
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

fn rank_cmp(cmp: Type) -> u8 {
    match cmp {
        Type::GreaterThan | Type::GreaterThanOrEqual => 0,
        Type::LessThan | Type::LessThanOrEqual => 1,
        _ => 2,
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

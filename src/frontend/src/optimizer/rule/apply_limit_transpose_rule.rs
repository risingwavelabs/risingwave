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

use itertools::Itertools;
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_pb::plan_common::JoinType;

use super::{BoxedRule, Rule};
use crate::optimizer::PlanRef;
use crate::optimizer::plan_node::{
    LogicalApply, LogicalFilter, LogicalLimit, LogicalTopN, PlanTreeNodeUnary,
};
use crate::optimizer::property::Order;
use crate::utils::Condition;

/// Transpose `LogicalApply` and `LogicalLimit`.
///
/// Before:
///
/// ```text
///     LogicalApply
///    /            \
///  Domain      LogicalLimit
///                  |
///                Input
/// ```
///
/// After:
///
/// ```text
///      LogicalTopN
///          |
///     LogicalApply
///    /            \
///  Domain        Input
/// ```
pub struct ApplyLimitTransposeRule {}
impl Rule for ApplyLimitTransposeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply: &LogicalApply = plan.as_logical_apply()?;
        let (left, right, on, join_type, correlated_id, correlated_indices, max_one_row) =
            apply.clone().decompose();
        assert_eq!(join_type, JoinType::Inner);
        let logical_limit: &LogicalLimit = right.as_logical_limit()?;
        let limit_input = logical_limit.input();
        let limit = logical_limit.limit();
        let offset = logical_limit.offset();

        let apply_left_len = left.schema().len();

        if max_one_row {
            return None;
        }

        let new_apply = LogicalApply::create(
            left,
            limit_input,
            JoinType::Inner,
            Condition::true_cond(),
            correlated_id,
            correlated_indices,
            false,
        );

        let new_topn = {
            // use the first column as an order to provide determinism for streaming queries.
            let order = Order::new(vec![ColumnOrder::new(0, OrderType::ascending())]);
            LogicalTopN::new(
                new_apply,
                limit,
                offset,
                false,
                order,
                (0..apply_left_len).collect_vec(),
            )
        };

        let filter = LogicalFilter::create(new_topn.into(), on);
        Some(filter)
    }
}

impl ApplyLimitTransposeRule {
    pub fn create() -> BoxedRule {
        Box::new(ApplyLimitTransposeRule {})
    }
}

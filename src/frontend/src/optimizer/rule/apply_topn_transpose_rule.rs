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
use risingwave_pb::plan_common::JoinType;

use super::{BoxedRule, Rule};
use crate::optimizer::PlanRef;
use crate::optimizer::plan_node::{LogicalApply, LogicalFilter, LogicalTopN};
use crate::utils::Condition;

/// Transpose `LogicalApply` and `LogicalTopN`.
///
/// Before:
///
/// ```text
///     LogicalApply
///    /            \
///  Domain      LogicalTopN
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
pub struct ApplyTopNTransposeRule {}
impl Rule for ApplyTopNTransposeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply: &LogicalApply = plan.as_logical_apply()?;
        let (left, right, on, join_type, correlated_id, correlated_indices, max_one_row) =
            apply.clone().decompose();
        assert_eq!(join_type, JoinType::Inner);
        let topn: &LogicalTopN = right.as_logical_top_n()?;
        let (topn_input, limit, offset, with_ties, mut order, mut group_key) =
            topn.clone().decompose();

        let apply_left_len = left.schema().len();

        if max_one_row {
            return None;
        }

        let new_apply = LogicalApply::create(
            left,
            topn_input,
            JoinType::Inner,
            Condition::true_cond(),
            correlated_id,
            correlated_indices,
            false,
        );

        let new_topn = {
            // shift index of topn's `InputRef` with `apply_left_len`.
            order
                .column_orders
                .iter_mut()
                .for_each(|ord| ord.column_index += apply_left_len);
            group_key.iter_mut().for_each(|idx| *idx += apply_left_len);
            let new_group_key = (0..apply_left_len).chain(group_key).collect_vec();
            LogicalTopN::new(new_apply, limit, offset, with_ties, order, new_group_key)
        };

        let filter = LogicalFilter::create(new_topn.into(), on);
        Some(filter)
    }
}

impl ApplyTopNTransposeRule {
    pub fn create() -> BoxedRule {
        Box::new(ApplyTopNTransposeRule {})
    }
}

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

use risingwave_pb::plan_common::JoinType;

use super::{BoxedRule, Rule};
use crate::optimizer::plan_node::{LogicalApply, LogicalDedup, LogicalFilter, PlanTreeNodeUnary};
use crate::optimizer::PlanRef;
use crate::utils::Condition;

/// Transpose `LogicalApply` and `LogicalDedup`.
///
/// Before:
///
/// ```text
///     LogicalApply
///    /            \
///  Domain      LogicalDedup
///                  |
///                Input
/// ```
///
/// After:
///
/// ```text
///     LogicalDedup
///          |
///     LogicalApply
///    /            \
///  Domain        Input
/// ```
pub struct ApplyDedupTransposeRule {}
impl Rule for ApplyDedupTransposeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply: &LogicalApply = plan.as_logical_apply()?;
        let (left, right, on, join_type, correlated_id, correlated_indices, max_one_row) =
            apply.clone().decompose();
        assert_eq!(join_type, JoinType::Inner);
        let dedup: &LogicalDedup = right.as_logical_dedup()?;
        let dedup_cols = dedup.dedup_cols();
        let dedup_input = dedup.input();

        let apply_left_len = left.schema().len();

        if max_one_row {
            return None;
        }

        let new_apply = LogicalApply::new(
            left,
            dedup_input,
            JoinType::Inner,
            Condition::true_cond(),
            correlated_id,
            correlated_indices,
            false,
        )
        .into();

        let new_dedup = {
            let mut new_dedup_cols: Vec<usize> = (0..apply_left_len).collect();
            new_dedup_cols.extend(dedup_cols.iter().map(|key| key + apply_left_len));
            LogicalDedup::new(new_apply, new_dedup_cols).into()
        };

        let filter = LogicalFilter::create(new_dedup, on);
        Some(filter)
    }
}

impl ApplyDedupTransposeRule {
    pub fn create() -> BoxedRule {
        Box::new(ApplyDedupTransposeRule {})
    }
}

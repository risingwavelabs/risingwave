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

use risingwave_pb::plan_common::JoinType;

use super::{BoxedRule, Rule};
use crate::optimizer::plan_node::{LogicalAgg, LogicalApply, LogicalFilter, PlanTreeNodeUnary};
use crate::optimizer::PlanRef;
use crate::utils::Condition;

/// Push `LogicalApply` down `LogicalAgg`.
pub struct ApplyAggRule {}
impl Rule for ApplyAggRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply: &LogicalApply = plan.as_logical_apply()?;
        let (left, right, on, join_type, correlated_id, correlated_indices) =
            apply.clone().decompose();
        assert_eq!(join_type, JoinType::Inner);
        let agg = right.as_logical_agg()?;

        // Insert all the columns of `LogicalApply`'s left at the beginning of `LogicalAgg`.
        let apply_left_len = left.schema().len();
        let mut group_key: Vec<usize> = (0..apply_left_len).collect();
        let (mut agg_calls, agg_group_key, _) = agg.clone().decompose();
        group_key.extend(agg_group_key.into_iter().map(|key| key + apply_left_len));

        // Shift index of agg_calls' `InputRef` with `apply_left_len`.
        agg_calls.iter_mut().for_each(|agg_call| {
            agg_call.inputs.iter_mut().for_each(|input_ref| {
                input_ref.shift_with_offset(apply_left_len as isize);
            });
        });

        let new_apply = LogicalApply::create(
            left,
            agg.input(),
            join_type,
            Condition {
                conjunctions: vec![],
            },
            correlated_id,
            correlated_indices,
        );
        let new_agg: PlanRef = LogicalAgg::new(agg_calls, group_key, new_apply).into();

        // left apply's on condition for predicate push to deal with
        if !on.conjunctions.is_empty() {
            let filter = LogicalFilter::create(new_agg, on);
            Some(filter)
        } else {
            Some(new_agg)
        }
    }
}

impl ApplyAggRule {
    pub fn create() -> BoxedRule {
        Box::new(ApplyAggRule {})
    }
}

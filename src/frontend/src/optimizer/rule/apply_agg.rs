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
use crate::optimizer::plan_node::{LogicalAgg, PlanTreeNodeBinary};
use crate::optimizer::PlanRef;
use crate::utils::ColIndexMapping;

/// Push `LogicalApply` down `LogicalAgg`.
pub struct ApplyAggRule {}
impl Rule for ApplyAggRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply = plan.as_logical_apply()?;
        assert_eq!(apply.join_type(), JoinType::Inner);
        let right = apply.right();
        let agg = right.as_logical_agg()?;

        // Insert all the columns of `LogicalApply`'s left at the beginning of `LogicalAgg`.
        let apply_left_len = apply.left().schema().len();
        let mut group_key: Vec<usize> = (0..apply_left_len).collect();
        let (mut agg_calls, agg_group_key, input) = agg.clone().decompose();
        group_key.extend(agg_group_key.into_iter().map(|key| key + apply_left_len));

        // Shift index of agg_calls' `InputRef` with `apply_left_len`.
        let offset = apply_left_len as isize;
        let mut shift_index = ColIndexMapping::with_shift_offset(input.schema().len(), offset);
        agg_calls.iter_mut().for_each(|agg_call| {
            agg_call.inputs.iter_mut().for_each(|input_ref| {
                input_ref.shift_with_offset(offset);
            });
            agg_call
                .order_by_fields
                .iter_mut()
                .for_each(|o| o.input.shift_with_offset(offset));
            agg_call.filter = agg_call.filter.clone().rewrite_expr(&mut shift_index);
        });

        let new_apply = apply.clone_with_left_right(apply.left(), input);
        let new_agg = LogicalAgg::new(agg_calls, group_key, new_apply.into());
        Some(new_agg.into())
    }
}

impl ApplyAggRule {
    pub fn create() -> BoxedRule {
        Box::new(ApplyAggRule {})
    }
}

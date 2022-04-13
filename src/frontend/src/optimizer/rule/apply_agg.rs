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

use risingwave_pb::plan::JoinType;

use super::{BoxedRule, Rule};
use crate::optimizer::plan_node::{LogicalAgg, PlanTreeNodeBinary};
use crate::optimizer::PlanRef;

pub struct ApplyAggRule {}
impl Rule for ApplyAggRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply = plan.as_logical_apply()?;
        let right = apply.right();
        let logical_agg = right.as_logical_agg()?;
        let (agg_calls, agg_call_alias, mut group_keys, new_right) =
            logical_agg.clone().decompose();

        match apply.join_type() {
            JoinType::LeftOuter if group_keys.is_empty() => {
                // Convert count(*) to count(pk).
                // if let Some(count) = agg_calls.iter_mut().find(|agg_call| {
                //     matches!(agg_call.agg_kind, AggKind::Count) && agg_call.inputs.is_empty()
                // }) {
                //     count.inputs.push();
                // }

                // Use apply.left's primary key as group_keys.
                group_keys.extend(logical_agg.base.pk_indices.iter());

                let new_apply = apply.clone_with_left_right(apply.left(), new_right);
                let new_agg =
                    LogicalAgg::new(agg_calls, agg_call_alias, group_keys, new_apply.into());
                Some(new_agg.into())
            }
            _ => Some(plan),
        }
    }
}

impl ApplyAggRule {
    pub fn create() -> BoxedRule {
        Box::new(ApplyAggRule {})
    }
}

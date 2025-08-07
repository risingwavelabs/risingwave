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

use risingwave_expr::aggregate::PbAggKind;

use super::prelude::{PlanRef, *};
use crate::expr::InputRef;
use crate::optimizer::plan_node::generic::{Agg, GenericPlanRef};
use crate::optimizer::plan_node::*;
use crate::utils::{Condition, IndexSet};

/// Use functional dependencies to simplify aggregation's group by
/// Before:
/// group by = [a, b, c], where b -> [a, c]
/// After
/// group by b, `first_value`(a), `first_value`(c),
pub struct AggGroupBySimplifyRule {}
impl Rule<Logical> for AggGroupBySimplifyRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let agg: &LogicalAgg = plan.as_logical_agg()?;
        let (agg_calls, group_key, grouping_sets, agg_input, _two_phase) = agg.clone().decompose();
        if !grouping_sets.is_empty() {
            return None;
        }
        let functional_dependency = agg_input.functional_dependency();
        let group_key = group_key.to_vec();
        if !functional_dependency.is_key(&group_key) {
            return None;
        }
        let minimized_group_key = functional_dependency.minimize_key(&group_key);
        if minimized_group_key.len() < group_key.len() {
            let new_group_key = IndexSet::from(minimized_group_key);
            let new_group_key_len = new_group_key.len();
            let mut new_agg_calls = vec![];
            for &i in &group_key {
                if !new_group_key.contains(i) {
                    let data_type = agg_input.schema().fields[i].data_type();
                    new_agg_calls.push(PlanAggCall {
                        agg_type: PbAggKind::InternalLastSeenValue.into(),
                        return_type: data_type.clone(),
                        inputs: vec![InputRef::new(i, data_type)],
                        distinct: false,
                        order_by: vec![],
                        filter: Condition::true_cond(),
                        direct_args: vec![],
                    });
                }
            }
            new_agg_calls.extend(agg_calls);

            // Use project to align schema type
            let mut out_fields = vec![];
            let mut remained_group_key_offset = 0;
            let mut removed_group_key_offset = new_group_key_len;
            for &i in &group_key {
                if new_group_key.contains(i) {
                    out_fields.push(remained_group_key_offset);
                    remained_group_key_offset += 1;
                } else {
                    out_fields.push(removed_group_key_offset);
                    removed_group_key_offset += 1;
                }
            }
            for i in group_key.len()..agg.base.schema().len() {
                out_fields.push(i);
            }
            let new_agg = Agg::new(new_agg_calls, new_group_key, agg.input());

            Some(LogicalProject::with_out_col_idx(new_agg.into(), out_fields.into_iter()).into())
        } else {
            None
        }
    }
}

impl AggGroupBySimplifyRule {
    pub fn create() -> BoxedRule {
        Box::new(AggGroupBySimplifyRule {})
    }
}

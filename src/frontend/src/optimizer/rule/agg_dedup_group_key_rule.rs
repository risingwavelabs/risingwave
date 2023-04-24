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

use super::super::plan_node::*;
use super::{BoxedRule, Rule};
use crate::optimizer::plan_node::generic::Agg;

/// Dedup the group key of `LogicalAgg`.
/// Before:
///
/// ```text
///    LogicalAgg (group_key = [a,b,a], agg_calls)
///        |
///    InputPlan
/// ```
/// After:
///
/// ```text
///    LogicalProject (exprs = [a,b,a] | agg_calls)
///        |
///    LogicalAgg (group_key = [a,b], agg_calls)
///        |
///    InputPlan
/// ```
pub struct AggDedupGroupKeyRule {}
impl Rule for AggDedupGroupKeyRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let agg = plan.as_logical_agg()?;
        let Agg {
            agg_calls,
            group_key,
            input,
        } = agg.core().clone();
        let (deduped_group_key, group_key_mapping) = {
            let mut deduped_group_key = vec![];
            let mut group_key_mapping = vec![];
            for key in group_key.ones() {
                if let Some(idx) = deduped_group_key
                    .iter()
                    .position(|deduped_key| *deduped_key == key)
                {
                    group_key_mapping.push(idx);
                } else {
                    deduped_group_key.push(key);
                    group_key_mapping.push(deduped_group_key.len() - 1);
                }
            }
            (deduped_group_key, group_key_mapping)
        };
        if deduped_group_key.len() == group_key.count_ones(..) {
            return None;
        }
        let deduped_group_key_num = deduped_group_key.len();
        let agg_call_num = agg_calls.len();
        let new_agg = Agg::new(agg_calls, deduped_group_key.into_iter().collect(), input);
        let proj = LogicalProject::with_out_col_idx(
            new_agg.into(),
            group_key_mapping
                .into_iter()
                .chain(deduped_group_key_num..deduped_group_key_num + agg_call_num),
        );
        Some(proj.into())
    }
}

impl AggDedupGroupKeyRule {
    pub fn create() -> BoxedRule {
        Box::new(AggDedupGroupKeyRule {})
    }
}

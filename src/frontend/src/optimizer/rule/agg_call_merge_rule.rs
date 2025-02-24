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

use super::{BoxedRule, Rule};
use crate::PlanRef;
use crate::optimizer::plan_node::generic::Agg;
use crate::optimizer::plan_node::{LogicalProject, PlanTreeNodeUnary};

/// Merges duplicated aggregate function calls in `LogicalAgg`, and project them back to the desired schema.
pub struct AggCallMergeRule {}

impl Rule for AggCallMergeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let agg = plan.as_logical_agg()?;

        let calls = agg.agg_calls();
        let mut new_calls = Vec::with_capacity(calls.len());
        let mut out_fields = (0..agg.group_key().len()).collect::<Vec<_>>();
        out_fields.extend(calls.iter().map(|call| {
            let pos = new_calls.iter().position(|c| c == call).unwrap_or_else(|| {
                let pos = new_calls.len();
                new_calls.push(call.clone());
                pos
            });
            agg.group_key().len() + pos
        }));

        if calls.len() == new_calls.len() {
            // no change
            None
        } else {
            let new_agg = Agg::new(new_calls, agg.group_key().clone(), agg.input())
                .with_enable_two_phase(agg.core().two_phase_agg_enabled())
                .into();
            Some(LogicalProject::with_out_col_idx(new_agg, out_fields.into_iter()).into())
        }
    }
}

impl AggCallMergeRule {
    pub fn create() -> BoxedRule {
        Box::new(Self {})
    }
}

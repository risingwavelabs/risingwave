// Copyright 2026 RisingWave Labs
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

use crate::optimizer::PlanRef;
use crate::optimizer::plan_node::{Logical, LogicalPlanRef, LogicalScan};
use crate::optimizer::rule::{BoxedRule, Rule};

pub struct MvSelectionRule;

impl MvSelectionRule {
    pub fn create() -> BoxedRule<Logical> {
        Box::new(Self)
    }
}

impl Rule<Logical> for MvSelectionRule {
    fn apply(&self, plan: PlanRef<Logical>) -> Option<PlanRef<Logical>> {
        let ctx = plan.ctx();
        for candidate in ctx.batch_mview_candidates().iter() {
            if plan == candidate.plan {
                let output_len = plan.schema().len();
                if output_len > candidate.table.columns().len() {
                    continue;
                }
                let output_col_idx = (0..output_len).collect();
                let scan = LogicalScan::create(candidate.table.clone(), ctx.clone(), None)
                    .clone_with_output_indices(output_col_idx);
                let rewritten: LogicalPlanRef = scan.into();
                return Some(rewritten);
            }
        }
        None
    }
}

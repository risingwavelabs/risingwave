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

use super::{BoxedRule, Rule};
use crate::optimizer::plan_node::{LogicalFilter, PlanTreeNodeUnary};
use crate::optimizer::PlanRef;

pub struct FilterMergeRule {}
impl Rule for FilterMergeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let filter = plan.as_logical_filter()?;

        let input = filter.input();
        let child_filter = input.as_logical_filter()?;

        Some(LogicalFilter::create(
            child_filter.input(),
            filter
                .predicate()
                .clone()
                .and(child_filter.predicate().clone()),
        ))
    }
}

impl FilterMergeRule {
    pub fn create() -> BoxedRule {
        Box::new(FilterMergeRule {})
    }
}

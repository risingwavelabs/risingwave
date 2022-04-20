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
use crate::optimizer::plan_node::PlanTreeNodeUnary;
use crate::optimizer::PlanRef;

pub struct FilterRemove {}
impl Rule for FilterRemove {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let filter = plan.as_logical_filter()?;
        if filter.predicate().always_true() {
            Some(filter.input())
        } else {
            None
        }
    }
}

impl FilterRemove {
    pub fn create() -> BoxedRule {
        Box::new(FilterRemove {})
    }
}

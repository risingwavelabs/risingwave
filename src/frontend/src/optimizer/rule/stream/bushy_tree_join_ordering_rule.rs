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

use super::super::super::plan_node::*;
use super::super::Rule;
use crate::optimizer::rule::BoxedRule;

/// Reorders a multi join into a left deep join via the heuristic ordering
pub struct BushyTreeJoinOrderingRule {}

impl Rule for BushyTreeJoinOrderingRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let join = plan.as_logical_multi_join()?;
        match join.as_bushy_tree_join() {
            Ok(plan) => Some(plan),
            Err(e) => {
                eprintln!("{}", e);
                None
            }
        }
    }
}

impl BushyTreeJoinOrderingRule {
    pub fn create() -> BoxedRule {
        Box::new(BushyTreeJoinOrderingRule {})
    }
}

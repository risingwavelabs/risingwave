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

/// Reorders a multi join into a bushy tree shape join tree with a minimal height.
pub struct BushyTreeJoinOrderingRule {}

/// If inputs of a multi join reach the limit, fallback to a left deep tree, because the search
/// space could be too large to search.
const BUSHY_TREE_JOIN_UPPER_LIMIT: usize = 10;

/// To construct a bushy tree with a height lower than the left deep tree, we need tt least four
/// inputs.
const BUSHY_TREE_JOIN_LOWER_LIMIT: usize = 4;

impl Rule for BushyTreeJoinOrderingRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let join = plan.as_logical_multi_join()?;
        if join.inputs().len() >= BUSHY_TREE_JOIN_LOWER_LIMIT
            && join.inputs().len() <= BUSHY_TREE_JOIN_UPPER_LIMIT
        {
            match join.as_bushy_tree_join() {
                Ok(plan) => Some(plan),
                Err(e) => {
                    eprintln!("{}", e);
                    None
                }
            }
        } else {
            // Too many inputs, so fallback to a left deep tree.
            let join_ordering = join.heuristic_ordering().ok()?;
            let left_deep_join = join.as_reordered_left_deep_join(&join_ordering);
            Some(left_deep_join)
        }
    }
}

impl BushyTreeJoinOrderingRule {
    pub fn create() -> BoxedRule {
        Box::new(BushyTreeJoinOrderingRule {})
    }
}

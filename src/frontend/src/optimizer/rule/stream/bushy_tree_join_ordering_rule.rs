// Copyright 2024 RisingWave Labs
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

use thiserror_ext::AsReport;

use super::super::super::plan_node::*;
use super::super::Rule;
use crate::optimizer::rule::BoxedRule;
use crate::optimizer::Result;

/// Reorders a multi join into a bushy tree shape join tree with a minimal height.
pub struct BushyTreeJoinOrderingRule {}

/// If inputs of a multi join reach the limit, fallback to a left deep tree, because the search
/// space could be too large to search.
const BUSHY_TREE_JOIN_UPPER_LIMIT: usize = 10;

/// To construct a bushy tree with a height lower than the left deep tree, we need tt least four
/// inputs.
const BUSHY_TREE_JOIN_LOWER_LIMIT: usize = 4;

impl Rule for BushyTreeJoinOrderingRule {
    fn apply(&self, plan: PlanRef) -> Result<Option<PlanRef>> {
        let join = match plan.as_logical_multi_join() {
            Some(join) => join,
            None => return Ok(None),
        };

        if join.inputs().len() >= BUSHY_TREE_JOIN_LOWER_LIMIT
            && join.inputs().len() <= BUSHY_TREE_JOIN_UPPER_LIMIT
        {
            match join.as_bushy_tree_join() {
                Ok(plan) => Ok(Some(plan)),
                Err(e) => {
                    eprintln!("{}", e.as_report());
                    Ok(None)
                }
            }
        } else {
            // Too many inputs, so fallback to a left deep tree.
            let join_ordering = match join.heuristic_ordering() {
                Ok(join_ordering) => join_ordering,
                Err(_) => return Ok(None),
            };

            let left_deep_join = join.as_reordered_left_deep_join(&join_ordering);
            Ok(Some(left_deep_join))
        }
    }
}

impl BushyTreeJoinOrderingRule {
    pub fn create() -> BoxedRule {
        Box::new(BushyTreeJoinOrderingRule {})
    }
}

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

use risingwave_pb::plan_common::JoinType;

use super::{BoxedRule, Rule};
use crate::optimizer::PlanRef;
use crate::optimizer::plan_node::generic::Agg;
use crate::optimizer::plan_node::{LogicalExcept, LogicalJoin, PlanTreeNode};
use crate::optimizer::rule::IntersectToSemiJoinRule;

pub struct ExceptToAntiJoinRule {}
impl Rule for ExceptToAntiJoinRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let logical_except: &LogicalExcept = plan.as_logical_except()?;
        let all = logical_except.all();
        if all {
            return None;
        }

        let inputs = logical_except.inputs();
        let join = inputs
            .into_iter()
            .fold(None, |left, right| match left {
                None => Some(right),
                Some(left) => {
                    let on =
                        IntersectToSemiJoinRule::gen_null_safe_equal(left.clone(), right.clone());
                    Some(LogicalJoin::create(left, right, JoinType::LeftAnti, on))
                }
            })
            .unwrap();

        Some(Agg::new(vec![], (0..join.schema().len()).collect(), join).into())
    }
}

impl ExceptToAntiJoinRule {
    pub fn create() -> BoxedRule {
        Box::new(ExceptToAntiJoinRule {})
    }
}

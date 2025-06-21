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

use crate::PlanRef;
use crate::optimizer::plan_node::{
    LogicalJoin, PlanTreeNodeBinary, StreamExchange, StreamHashJoin,
};
use crate::optimizer::rule::{BoxedRule, Rule};

/// Separate consecutive stream hash joins by no-shuffle exchange
pub struct SeparateConsecutiveJoinRule {}

impl Rule for SeparateConsecutiveJoinRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let join = plan.as_stream_hash_join()?;
        let left_input = join.left();
        let right_input = join.right();

        let new_left = if left_input.as_stream_hash_join().is_some() {
            StreamExchange::new_no_shuffle(left_input).into()
        } else {
            left_input
        };

        let new_right = if right_input.as_stream_hash_join().is_some() {
            StreamExchange::new_no_shuffle(right_input).into()
        } else {
            right_input
        };

        let new_logical_join = LogicalJoin::new(
            new_left,
            new_right,
            join.join_type(),
            join.eq_join_predicate().all_cond(),
        );
        Some(
            StreamHashJoin::new(
                new_logical_join.core().clone(),
                join.eq_join_predicate().clone(),
            )
            .into(),
        )
    }
}

impl SeparateConsecutiveJoinRule {
    pub fn create() -> BoxedRule {
        Box::new(SeparateConsecutiveJoinRule {})
    }
}

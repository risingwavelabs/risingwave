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

use std::collections::HashMap;

use paste::paste;

use super::*;
use crate::utils::Condition;
use crate::{for_batch_plan_nodes, for_stream_plan_nodes};
/// The trait for predicate pushdown, only logical plan node will use it, though all plan node impl
/// it.
pub trait PredicatePushdown {
    /// Push predicate down for every logical plan node.
    ///
    /// There are three kinds of predicates:
    ///
    /// 1. those can't be pushed down. We just create a `LogicalFilter` for them above the current
    /// `PlanNode`. i.e.,
    ///   
    ///     ```ignore
    ///     LogicalFilter::create(self.clone().into(), predicate)
    ///     ```
    ///
    /// 2. those can be merged with current `PlanNode` (e.g. `LogicalJoin`). We just merge
    /// the predicates with the `Condition` of it.
    ///
    /// 3. those can be pushed down. We pass them to current `PlanNode`'s input.
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef;
}

macro_rules! ban_predicate_pushdown {
    ($( { $convention:ident, $name:ident }),*) => {
        paste!{
            $(impl PredicatePushdown for [<$convention $name>] {
                fn predicate_pushdown(&self, _predicate: Condition, _ctx: &mut PredicatePushdownContext) -> PlanRef {
                    unreachable!("predicate pushdown is only allowed on logical plan")
                }
            })*
        }
    }
}
for_batch_plan_nodes! {ban_predicate_pushdown}
for_stream_plan_nodes! {ban_predicate_pushdown}

#[inline]
pub fn gen_filter_and_pushdown<T: PlanTreeNodeUnary + PlanNode>(
    node: &T,
    filter_predicate: Condition,
    pushed_predicate: Condition,
    ctx: &mut PredicatePushdownContext,
) -> PlanRef {
    let new_input = node.input().predicate_pushdown(pushed_predicate, ctx);
    let new_node = node.clone_with_input(new_input);
    LogicalFilter::create(Rc::new(new_node), filter_predicate)
}

#[derive(Debug, Clone, Default)]
pub struct PredicatePushdownContext {
    share_predicate_map: HashMap<i32, Vec<Condition>>,
}

impl PredicatePushdownContext {
    pub fn add_predicate(&mut self, plan_node_id: PlanNodeId, predicate: Condition) -> usize {
        self.share_predicate_map
            .entry(plan_node_id.0)
            .and_modify(|e| e.push(predicate.clone()))
            .or_insert_with(|| vec![predicate])
            .len()
    }

    pub fn take_predicate(&mut self, plan_node_id: PlanNodeId) -> Option<Vec<Condition>> {
        self.share_predicate_map.remove(&plan_node_id.0)
    }
}

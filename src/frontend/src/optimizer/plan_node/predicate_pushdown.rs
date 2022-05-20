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

use paste::paste;

use super::*;
use crate::utils::Condition;
use crate::{for_batch_plan_nodes, for_stream_plan_nodes};
/// The trait for predicate pushdown, only logical plan node will use it, though all plan node impl
/// it.
pub trait PredicatePushdown {
    /// Push predicate down for every logical plan node.
    ///
    /// Sometimes, you will find it difficult or just want to delay the implementation of predicate
    /// pushdown, then you can use [`gen_filter_and_pushdown`] to generate a `LogicalFilter` above
    /// the node and do predicate pushdown for its input.
    /// Please note that `LogicalFilter::create` will NOT create a `LogicalFilter` if `predicate`
    /// is always true, so feel free to use the helper function.
    fn predicate_pushdown(&self, predicate: Condition) -> PlanRef;
}

macro_rules! impl_predicate_pushdown {
    ([], $( { $convention:ident, $name:ident }),*) => {
        paste!{
            $(impl PredicatePushdown for [<$convention $name>] {
                fn predicate_pushdown(&self, _predicate: Condition) -> PlanRef {
                    panic!("predicate pushdown is only allowed on logical plan")
                }
            })*
        }
    }
}
for_batch_plan_nodes! {impl_predicate_pushdown}
for_stream_plan_nodes! {impl_predicate_pushdown}

#[inline]
pub fn gen_filter_and_pushdown<T: PlanTreeNodeUnary + PlanNode>(
    node: &T,
    filter_predicate: Condition,
    pushed_predicate: Condition,
) -> PlanRef {
    let new_input = node.input().predicate_pushdown(pushed_predicate);
    let new_node = node.clone_with_input(new_input);
    LogicalFilter::create(Rc::new(new_node), filter_predicate)
}

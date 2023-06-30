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

use std::collections::{HashMap, HashSet};

use paste::paste;

use super::*;
use crate::optimizer::plan_visitor::ShareParentCounter;
use crate::optimizer::PlanVisitor;
use crate::{for_batch_plan_nodes, for_stream_plan_nodes};

/// The trait for column pruning, only logical plan node will use it, though all plan node impl it.
pub trait ColPrunable {
    /// Transform the plan node to only output the required columns ordered by index number.
    ///
    /// `required_cols` must be a subset of the range `0..self.schema().len()`.
    ///
    /// After calling `prune_col` on the children, their output schema may change, so
    /// the caller may need to transform its [`InputRef`](crate::expr::InputRef) using
    /// [`ColIndexMapping`](crate::utils::ColIndexMapping).
    ///
    /// When implementing this method for a node, it may require its children to produce additional
    /// columns besides `required_cols`. In this case, it may need to insert a
    /// [`LogicalProject`](super::LogicalProject) above to have a correct schema.
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef;
}

/// Implements [`ColPrunable`] for batch and streaming node.
macro_rules! impl_prune_col {
    ($( { $convention:ident, $name:ident }),*) => {
        paste!{
            $(impl ColPrunable for [<$convention $name>] {
                fn prune_col(&self, _required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
                    panic!("column pruning is only allowed on logical plan")
                }
            })*
        }
    }
}
for_batch_plan_nodes! { impl_prune_col }
for_stream_plan_nodes! { impl_prune_col }

#[derive(Debug, Clone)]
pub struct ColumnPruningContext {
    /// `share_required_cols_map` is used by the first round of column pruning to keep track of
    /// each parent required columns.
    share_required_cols_map: HashMap<PlanNodeId, Vec<Vec<usize>>>,
    /// Used to calculate how many parents the share operator has.
    share_parent_counter: ShareParentCounter,
    /// Share input cache used by the second round of column pruning.
    /// For a DAG plan, use only one round to prune column is not enough,
    /// because we need to change the schema of share operator
    /// and you don't know what is the final schema when the first parent try to prune column,
    /// so we need a second round to use the information collected by the first round.
    /// `share_cache` maps original share operator plan id to the new share operator and the column
    /// changed mapping which is actually the merged required columns calculated at the first
    /// round.
    share_cache: HashMap<PlanNodeId, (PlanRef, Vec<usize>)>,
    /// `share_visited` is used to track whether the share operator is visited, because we need to
    /// recursively call the `prune_col` of the new share operator to trigger the replacement.
    /// It is only used at the second round of the column pruning.
    share_visited: HashSet<PlanNodeId>,
}

impl ColumnPruningContext {
    pub fn new(root: PlanRef) -> Self {
        let mut share_parent_counter = ShareParentCounter::default();
        share_parent_counter.visit(root);
        Self {
            share_required_cols_map: Default::default(),
            share_parent_counter,
            share_cache: Default::default(),
            share_visited: Default::default(),
        }
    }

    pub fn get_parent_num(&self, share: &LogicalShare) -> usize {
        self.share_parent_counter.get_parent_num(share)
    }

    pub fn add_required_cols(
        &mut self,
        plan_node_id: PlanNodeId,
        required_cols: Vec<usize>,
    ) -> usize {
        self.share_required_cols_map
            .entry(plan_node_id)
            .and_modify(|e| e.push(required_cols.clone()))
            .or_insert_with(|| vec![required_cols])
            .len()
    }

    pub fn take_required_cols(&mut self, plan_node_id: PlanNodeId) -> Option<Vec<Vec<usize>>> {
        self.share_required_cols_map.remove(&plan_node_id)
    }

    pub fn add_share_cache(
        &mut self,
        plan_node_id: PlanNodeId,
        new_share: PlanRef,
        merged_required_columns: Vec<usize>,
    ) {
        self.share_cache
            .try_insert(plan_node_id, (new_share, merged_required_columns))
            .unwrap();
    }

    pub fn get_share_cache(&self, plan_node_id: PlanNodeId) -> Option<(PlanRef, Vec<usize>)> {
        self.share_cache.get(&plan_node_id).cloned()
    }

    pub fn need_second_round(&self) -> bool {
        !self.share_cache.is_empty()
    }

    pub fn visit_share_at_second_round(&mut self, plan_node_id: PlanNodeId) -> bool {
        self.share_visited.insert(plan_node_id)
    }
}

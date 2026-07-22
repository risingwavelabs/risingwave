// Copyright 2022 RisingWave Labs
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

use super::*;
use crate::optimizer::LogicalPlanRef as PlanRef;

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

#[derive(Clone, Debug)]
struct RequiredColumns(Vec<usize>);

impl ShareRequirement for RequiredColumns {
    fn merge(requirements: Vec<Self>) -> Self {
        Self(
            requirements
                .into_iter()
                .flat_map(|required| required.0)
                .sorted()
                .dedup()
                .collect(),
        )
    }
}

#[derive(Debug, Clone)]
pub struct ColumnPruningContext {
    dag: ShareDagContext<RequiredColumns>,
}

impl ColumnPruningContext {
    pub fn new(root: PlanRef) -> Self {
        Self {
            dag: ShareDagContext::new(root),
        }
    }

    pub(in crate::optimizer) fn is_running(&self) -> bool {
        self.dag.is_running()
    }

    pub(in crate::optimizer) fn phase(&self) -> ShareDagPhase {
        self.dag.phase()
    }

    pub(in crate::optimizer) fn get_parent_num(&self, share: &LogicalShare) -> usize {
        self.dag.parent_num(share)
    }

    pub(in crate::optimizer) fn add_required_cols(
        &mut self,
        share: &LogicalShare,
        required_cols: Vec<usize>,
    ) -> Option<Vec<usize>> {
        self.dag
            .record_requirement(share, RequiredColumns(required_cols))
            .map(|required| required.0)
    }

    pub(in crate::optimizer) fn share_mapping(&self, share: &LogicalShare) -> ColIndexMapping {
        share.ctx().logical_share_pending_mapping(share.share_id())
    }

    pub(in crate::optimizer) fn run(&mut self, root: PlanRef, required_cols: &[usize]) -> PlanRef {
        self.dag.reset(root.clone());
        let optimizer_ctx = root.ctx();
        let transaction = LogicalShareTableTransaction::new(optimizer_ctx.clone());
        optimizer_ctx.clear_logical_share_pending_mappings();

        self.dag.set_phase(ShareDagPhase::Collect);
        let collected = root.prune_col_inner(required_cols, self);
        let rebuild_order = self.dag.rebuild_order();
        if rebuild_order.is_empty() {
            self.dag.finish();
            transaction.commit();
            return collected;
        }

        self.dag.set_phase(ShareDagPhase::Rebuild);
        for share_id in rebuild_order {
            let original_input = self.dag.original_input(share_id);
            let required = self.dag.merged_requirement(share_id).0;
            let old_schema_len = optimizer_ctx
                .resolve_current_logical_share(share_id)
                .schema()
                .len();
            let rebuilt_input = original_input.prune_col(&required, self);
            let mapping = ColIndexMapping::with_remaining_columns(&required, old_schema_len);
            optimizer_ctx.update_logical_share_for_dag(share_id, rebuilt_input, mapping);
        }

        self.dag.set_phase(ShareDagPhase::Adapt);
        let result = root.prune_col(required_cols, self);
        optimizer_ctx.clear_logical_share_pending_mappings();
        self.dag.finish();
        transaction.commit();
        result
    }
}

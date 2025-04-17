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

use risingwave_common::catalog::TableVersionId;

use super::utils::impl_distill_by_unit;
use super::{
    BatchDelete, ColPrunable, ExprRewritable, Logical, LogicalProject, PlanBase, PlanRef,
    PlanTreeNodeUnary, PredicatePushdown, ToBatch, ToStream, gen_filter_and_pushdown, generic,
};
use crate::catalog::TableId;
use crate::error::Result;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::{
    ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext, ToStreamContext,
};
use crate::utils::{ColIndexMapping, Condition};

/// [`LogicalDelete`] iterates on input relation and delete the data from specified table.
///
/// It corresponds to the `DELETE` statements in SQL.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalDelete {
    pub base: PlanBase<Logical>,
    core: generic::Delete<PlanRef>,
}

impl From<generic::Delete<PlanRef>> for LogicalDelete {
    fn from(core: generic::Delete<PlanRef>) -> Self {
        let base = PlanBase::new_logical_with_core(&core);
        Self { base, core }
    }
}

impl LogicalDelete {
    #[must_use]
    pub fn table_id(&self) -> TableId {
        self.core.table_id
    }

    pub fn has_returning(&self) -> bool {
        self.core.returning
    }

    pub fn table_version_id(&self) -> TableVersionId {
        self.core.table_version_id
    }
}

impl PlanTreeNodeUnary for LogicalDelete {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        core.into()
    }
}

impl_plan_tree_node_for_unary! { LogicalDelete }
impl_distill_by_unit!(LogicalDelete, core, "LogicalDelete");

impl ColPrunable for LogicalDelete {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        let pruned_input = {
            let input = &self.core.input;
            let required_cols: Vec<_> = (0..input.schema().len()).collect();
            input.prune_col(&required_cols, ctx)
        };

        // No pruning.
        LogicalProject::with_out_col_idx(
            self.clone_with_input(pruned_input).into(),
            required_cols.iter().copied(),
        )
        .into()
    }
}

impl ExprRewritable for LogicalDelete {}

impl ExprVisitable for LogicalDelete {}

impl PredicatePushdown for LogicalDelete {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        gen_filter_and_pushdown(self, predicate, Condition::true_cond(), ctx)
    }
}

impl ToBatch for LogicalDelete {
    fn to_batch(&self) -> Result<PlanRef> {
        let new_input = self.input().to_batch()?;
        let mut core = self.core.clone();
        core.input = new_input;
        Ok(BatchDelete::new(core).into())
    }
}

impl ToStream for LogicalDelete {
    fn to_stream(&self, _ctx: &mut ToStreamContext) -> Result<PlanRef> {
        unreachable!("delete should always be converted to batch plan");
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        unreachable!("delete should always be converted to batch plan");
    }
}

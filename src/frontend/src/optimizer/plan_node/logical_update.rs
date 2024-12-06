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

use super::generic::GenericPlanRef;
use super::utils::impl_distill_by_unit;
use super::{
    gen_filter_and_pushdown, generic, BatchUpdate, ColPrunable, ExprRewritable, Logical,
    LogicalProject, PlanBase, PlanRef, PlanTreeNodeUnary, PredicatePushdown, ToBatch, ToStream,
};
use crate::error::Result;
use crate::expr::{ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::{
    ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext, ToStreamContext,
};
use crate::utils::{ColIndexMapping, Condition};

/// [`LogicalUpdate`] iterates on input relation, set some columns, and inject update records into
/// specified table.
///
/// It corresponds to the `UPDATE` statements in SQL.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalUpdate {
    pub base: PlanBase<Logical>,
    core: generic::Update<PlanRef>,
}

impl From<generic::Update<PlanRef>> for LogicalUpdate {
    fn from(core: generic::Update<PlanRef>) -> Self {
        let base = PlanBase::new_logical_with_core(&core);
        Self { base, core }
    }
}

impl PlanTreeNodeUnary for LogicalUpdate {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        core.into()
    }
}

impl_plan_tree_node_for_unary! { LogicalUpdate }
impl_distill_by_unit!(LogicalUpdate, core, "LogicalUpdate");

impl ExprRewritable for LogicalUpdate {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self::from(core).into()
    }
}

impl ExprVisitable for LogicalUpdate {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}

impl ColPrunable for LogicalUpdate {
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

impl PredicatePushdown for LogicalUpdate {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        gen_filter_and_pushdown(self, predicate, Condition::true_cond(), ctx)
    }
}

impl ToBatch for LogicalUpdate {
    fn to_batch(&self) -> Result<PlanRef> {
        let new_input = self.input().to_batch()?;
        let mut new_logical = self.core.clone();
        new_logical.input = new_input;
        Ok(BatchUpdate::new(new_logical, self.schema().clone()).into())
    }
}

impl ToStream for LogicalUpdate {
    fn to_stream(&self, _ctx: &mut ToStreamContext) -> Result<PlanRef> {
        unreachable!("update should always be converted to batch plan");
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        unreachable!("update should always be converted to batch plan");
    }
}

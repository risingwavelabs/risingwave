use std::rc::Weak;

use risingwave_common::bail_not_implemented;
use risingwave_common::util::column_index_mapping::ColIndexMapping;

use super::expr_visitable::ExprVisitable;
use super::utils::impl_distill_by_unit;
use super::{
    gen_filter_and_pushdown, generic, ColPrunable, ColumnPruningContext, ExprRewritable, Logical,
    LogicalProject, PlanBase, PredicatePushdown, PredicatePushdownContext, RewriteStreamContext,
    ToBatch, ToStream, ToStreamContext,
};
use crate::binder::ShareId;
use crate::error::Result;
use crate::utils::Condition;
use crate::PlanRef;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalCteRef {
    pub base: PlanBase<Logical>,
    core: generic::CteRef<PlanRef>,
}

impl LogicalCteRef {
    pub fn new(share_id: ShareId, base_plan: PlanRef, r#ref: Weak<PlanRef>) -> Self {
        let core = generic::CteRef::new(share_id, r#ref, base_plan);
        let base = PlanBase::new_logical_with_core(&core);
        Self { base, core }
    }

    pub fn create(share_id: ShareId, base_plan: PlanRef, r#ref: Weak<PlanRef>) -> PlanRef {
        Self::new(share_id, base_plan, r#ref).into()
    }
}

impl_plan_tree_node_for_leaf! {LogicalCteRef}

impl_distill_by_unit! {LogicalCteRef, core, "LogicalCteRef"}

impl ExprRewritable for LogicalCteRef {}

impl ExprVisitable for LogicalCteRef {}

impl ColPrunable for LogicalCteRef {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        LogicalProject::with_out_col_idx(self.clone().into(), required_cols.iter().copied()).into()
    }
}

impl PredicatePushdown for LogicalCteRef {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        todo!()
    }
}

impl ToBatch for LogicalCteRef {
    fn to_batch(&self) -> Result<PlanRef> {
        bail_not_implemented!(issue = 15135, "recursive CTE not supported")
    }
}

impl ToStream for LogicalCteRef {
    fn to_stream(&self, ctx: &mut ToStreamContext) -> Result<PlanRef> {
        bail_not_implemented!(issue = 15135, "recursive CTE not supported")
    }

    fn logical_rewrite_for_stream(
        &self,
        ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        bail_not_implemented!(issue = 15135, "recursive CTE not supported")
    }
}

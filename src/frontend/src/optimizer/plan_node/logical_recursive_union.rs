use itertools::Itertools;
use risingwave_common::bail_not_implemented;
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use smallvec::{smallvec, SmallVec};

use super::expr_visitable::ExprVisitable;
use super::utils::impl_distill_by_unit;
use super::{
    gen_filter_and_pushdown, generic, ColPrunable, ColumnPruningContext, ExprRewritable, Logical,
    PlanBase, PlanTreeNode, PredicatePushdown, PredicatePushdownContext, RewriteStreamContext,
    ToBatch, ToStream, ToStreamContext,
};
use crate::error::Result;
use crate::utils::Condition;
use crate::PlanRef;

/// `LogicalUnion` returns the union of the rows of its inputs.
/// If `all` is false, it needs to eliminate duplicates.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalRecursiveUnion {
    pub base: PlanBase<Logical>,
    core: generic::RecursiveUnion<PlanRef>,
}

impl LogicalRecursiveUnion {
    pub fn new(base_plan: PlanRef, recursive: PlanRef) -> Self {
        let core = generic::RecursiveUnion {
            base: base_plan,
            recursive,
        };
        let base = PlanBase::new_logical_with_core(&core);
        LogicalRecursiveUnion { base, core }
    }

    pub fn create(base_plan: PlanRef, recursive: PlanRef) -> PlanRef {
        Self::new(base_plan, recursive).into()
    }
}

impl PlanTreeNode for LogicalRecursiveUnion {
    fn inputs(&self) -> SmallVec<[PlanRef; 2]> {
        smallvec![self.core.base.clone(), self.core.recursive.clone()]
    }

    fn clone_with_inputs(&self, inputs: &[PlanRef]) -> PlanRef {
        let mut inputs = inputs.into_iter().cloned();
        Self::create(inputs.next().unwrap(), inputs.next().unwrap())
    }
}

impl_distill_by_unit!(LogicalRecursiveUnion, core, "LogicalRecursiveUnion");

impl ColPrunable for LogicalRecursiveUnion {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        let new_inputs = self
            .inputs()
            .iter()
            .map(|input| input.prune_col(required_cols, ctx))
            .collect_vec();
        self.clone_with_inputs(&new_inputs)
    }
}

impl ExprRewritable for LogicalRecursiveUnion {}

impl ExprVisitable for LogicalRecursiveUnion {}

impl PredicatePushdown for LogicalRecursiveUnion {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        todo!()
    }
}

impl ToBatch for LogicalRecursiveUnion {
    fn to_batch(&self) -> Result<PlanRef> {
        bail_not_implemented!(issue = 15135, "recursive CTE not supported")
    }
}

impl ToStream for LogicalRecursiveUnion {
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

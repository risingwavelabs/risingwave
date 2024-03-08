use crate::binder::BoundSetExpr;
use crate::error::Result;
use crate::optimizer::plan_node::LogicalRecursiveUnion;
use crate::{PlanRef, Planner};

impl Planner {
    pub(super) fn plan_recursive_union(
        &mut self,
        base: BoundSetExpr,
        recursive: BoundSetExpr,
    ) -> Result<PlanRef> {
        let base = self.plan_set_expr(base, vec![], &[])?;
        let recursive = self.plan_set_expr(recursive, vec![], &[])?;
        Ok(LogicalRecursiveUnion::create(base, recursive))
    }
}

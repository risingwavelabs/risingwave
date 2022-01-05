use std::fmt;

use risingwave_common::catalog::Schema;

use super::{LogicalProject, PlanRef, PlanTreeNodeUnary};
use crate::planner::property::{WithDistribution, WithOrder, WithSchema};

#[derive(Debug, Clone)]
pub struct BatchProject {
  logical: LogicalProject,
}
impl BatchProject {
  pub fn new(logical: LogicalProject) -> Self {
    BatchProject { logical }
  }
}
impl fmt::Display for BatchProject {
  fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
    todo!()
  }
}
impl PlanTreeNodeUnary for BatchProject {
  fn child(&self) -> PlanRef {
    self.logical.child()
  }
  fn clone_with_child(&self, child: PlanRef) -> Self {
    Self::new(self.logical.clone_with_child(child))
  }
}
impl_plan_tree_node_for_unary! {BatchProject}
impl WithOrder for BatchProject {}
impl WithDistribution for BatchProject {}
impl WithSchema for BatchProject {
  fn schema(&self) -> &Schema {
    self.logical.schema()
  }
}

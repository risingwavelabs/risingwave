use std::fmt;

use risingwave_common::catalog::Schema;

use super::{LogicalProject, PlanRef, PlanTreeNodeUnary};
use crate::optimizer::property::{WithDistribution, WithOrder, WithSchema};

#[derive(Debug, Clone)]
pub struct StreamProject {
  logical: LogicalProject,
}
impl fmt::Display for StreamProject {
  fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
    todo!()
  }
}
impl StreamProject {
  pub fn new(logical: LogicalProject) -> Self {
    StreamProject { logical }
  }
}
impl PlanTreeNodeUnary for StreamProject {
  fn child(&self) -> PlanRef {
    self.logical.child()
  }
  fn clone_with_child(&self, child: PlanRef) -> Self {
    Self::new(self.logical.clone_with_child(child))
  }
}
impl_plan_tree_node_for_unary! {StreamProject}
impl WithSchema for StreamProject {
  fn schema(&self) -> &Schema {
    self.logical.schema()
  }
}

impl WithDistribution for StreamProject {}
impl WithOrder for StreamProject {}

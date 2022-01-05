use std::fmt;

use risingwave_common::catalog::Schema;

use super::{PlanRef, PlanTreeNodeUnary};
use crate::planner::property::{WithDistribution, WithOrder, WithSchema};

#[derive(Debug, Clone)]
pub struct LogicalProject {
  // TODO(expr)
  child: PlanRef,
  schema: Schema,
}
impl LogicalProject {
  pub fn new(child: PlanRef) -> Self {
    let schema = Self::derive_schema(child.schema());
    LogicalProject { child, schema }
  }
  // TODO: give project expressions
  fn derive_schema(_child: &Schema) -> Schema {
    todo!()
  }
}
impl PlanTreeNodeUnary for LogicalProject {
  fn child(&self) -> PlanRef {
    self.child.clone()
  }
  fn clone_with_child(&self, child: PlanRef) -> Self {
    Self::new(child)
  }
}
impl_plan_tree_node_for_unary! {LogicalProject}
impl fmt::Display for LogicalProject {
  fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
    todo!()
  }
}
impl WithOrder for LogicalProject {}
impl WithDistribution for LogicalProject {}
impl WithSchema for LogicalProject {
  fn schema(&self) -> &Schema {
    &self.schema
  }
}

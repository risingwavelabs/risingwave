use std::fmt;

use risingwave_common::catalog::Schema;

use super::{PlanRef, PlanTreeNodeUnary};
use crate::optimizer::property::{WithDistribution, WithOrder, WithSchema};

#[derive(Debug, Clone)]
pub struct LogicalProject {
  // TODO(expr)
  input: PlanRef,
  schema: Schema,
}
impl LogicalProject {
  pub fn new(input: PlanRef) -> Self {
    let schema = Self::derive_schema(input.schema());
    LogicalProject { input, schema }
  }
  // TODO: give project expressions
  fn derive_schema(_input: &Schema) -> Schema {
    todo!()
  }
}
impl PlanTreeNodeUnary for LogicalProject {
  fn input(&self) -> PlanRef {
    self.input.clone()
  }
  fn clone_with_input(&self, input: PlanRef) -> Self {
    Self::new(input)
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

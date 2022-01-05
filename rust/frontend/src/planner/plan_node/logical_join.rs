use std::fmt;

use risingwave_common::catalog::Schema;

use super::{JoinPredicate, PlanRef, PlanTreeNodeBinary};
use crate::planner::property::{WithDistribution, WithOrder, WithSchema};

#[derive(Debug, Clone)]
pub struct LogicalJoin {
  // TODO:
  left: PlanRef,
  right: PlanRef,
  predicate: JoinPredicate,
  schema: Schema,
}
impl fmt::Display for LogicalJoin {
  fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
    todo!()
  }
}
impl LogicalJoin {
  // TODO(expr)
  pub fn new(left: PlanRef, right: PlanRef) -> Self {
    let schema = Self::derive_schema(left.schema(), right.schema());
    let predicate = JoinPredicate::new();
    LogicalJoin {
      left,
      right,
      schema,
      predicate,
    }
  }
  // TODO: give joinType and consider
  fn derive_schema(_left: &Schema, _right: &Schema) -> Schema {
    todo!()
  }
  pub fn get_predicate(&self) -> &JoinPredicate {
    &self.predicate
  }
}
impl PlanTreeNodeBinary for LogicalJoin {
  fn left(&self) -> PlanRef {
    self.left.clone()
  }

  fn right(&self) -> PlanRef {
    self.right.clone()
  }

  fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self {
    Self::new(left, right)
  }
}
impl_plan_tree_node_for_binary! {LogicalJoin}
impl WithOrder for LogicalJoin {}
impl WithDistribution for LogicalJoin {}
impl WithSchema for LogicalJoin {
  fn schema(&self) -> &Schema {
    &self.schema
  }
}

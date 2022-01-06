use std::fmt;

use risingwave_common::catalog::Schema;

use super::{PlanRef, PlanTreeNodeUnary};
use crate::optimizer::property::{Distribution, WithDistribution, WithOrder, WithSchema};

#[derive(Debug, Clone)]
pub struct StreamExchange {
  child: PlanRef,
  schema: Schema,
  dist: Distribution,
}
impl StreamExchange {
  pub fn new(child: PlanRef, dist: Distribution) -> Self {
    let schema = child.schema().clone();
    StreamExchange {
      child,
      schema,
      dist,
    }
  }
}
impl fmt::Display for StreamExchange {
  fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
    todo!()
  }
}
impl PlanTreeNodeUnary for StreamExchange {
  fn child(&self) -> PlanRef {
    self.child.clone()
  }
  fn clone_with_child(&self, child: PlanRef) -> Self {
    Self::new(child, self.distribution())
  }
}
impl_plan_tree_node_for_unary! {StreamExchange}
impl WithDistribution for StreamExchange {
  fn distribution(&self) -> Distribution {
    self.dist.clone()
  }
}
impl WithOrder for StreamExchange {}

impl WithSchema for StreamExchange {
  fn schema(&self) -> &Schema {
    &self.schema
  }
}

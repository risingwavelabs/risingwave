use std::fmt;

use risingwave_common::catalog::Schema;

use super::{PlanRef, PlanTreeNodeUnary};
use crate::planner::property::{Distribution, Order, WithDistribution, WithOrder, WithSchema};

#[derive(Debug, Clone)]
pub struct BatchSort {
  order: Order,
  child: PlanRef,
  schema: Schema,
  dist: Distribution,
}
impl BatchSort {
  pub fn new(child: PlanRef, order: Order) -> Self {
    let schema = child.schema().clone();
    let dist = child.distribution();
    BatchSort {
      child,
      order,
      schema,
      dist,
    }
  }
}
impl fmt::Display for BatchSort {
  fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
    todo!()
  }
}
impl PlanTreeNodeUnary for BatchSort {
  fn child(&self) -> PlanRef {
    self.child.clone()
  }
  fn clone_with_child(&self, child: PlanRef) -> Self {
    Self::new(child, self.order.clone())
  }
}
impl_plan_tree_node_for_unary! {BatchSort}
impl WithOrder for BatchSort {
  fn order(&self) -> Order {
    self.order.clone()
  }
}
impl WithDistribution for BatchSort {
  fn distribution(&self) -> Distribution {
    self.dist.clone()
  }
}
impl WithSchema for BatchSort {
  fn schema(&self) -> &Schema {
    &self.schema
  }
}

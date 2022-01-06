use std::fmt;

use risingwave_common::catalog::Schema;

use super::{PlanRef, PlanTreeNodeUnary};
use crate::optimizer::property::{Distribution, Order, WithDistribution, WithOrder, WithSchema};

#[derive(Debug, Clone)]
pub struct BatchExchange {
  order: Order,
  child: PlanRef,
  schema: Schema,
  dist: Distribution,
}
impl BatchExchange {
  pub fn new(child: PlanRef, order: Order, dist: Distribution) -> Self {
    let schema = child.schema().clone();
    BatchExchange {
      child,
      order,
      schema,
      dist,
    }
  }
}
impl fmt::Display for BatchExchange {
  fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
    todo!()
  }
}
impl PlanTreeNodeUnary for BatchExchange {
  fn child(&self) -> PlanRef {
    self.child.clone()
  }
  fn clone_with_child(&self, child: PlanRef) -> Self {
    Self::new(child, self.order.clone(), self.distribution())
  }
}
impl_plan_tree_node_for_unary! {BatchExchange}
impl WithOrder for BatchExchange {
  fn order(&self) -> Order {
    self.order.clone()
  }
}
impl WithDistribution for BatchExchange {
  fn distribution(&self) -> Distribution {
    self.dist.clone()
  }
}
impl WithSchema for BatchExchange {
  fn schema(&self) -> &Schema {
    &self.schema
  }
}

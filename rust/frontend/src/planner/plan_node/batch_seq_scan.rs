use std::fmt;

use risingwave_common::catalog::Schema;

use crate::planner::property::{WithDistribution, WithOrder, WithSchema};

#[derive(Debug, Clone)]
pub struct BatchSeqScan {
  // TODO(catalog)
}
impl WithSchema for BatchSeqScan {
  fn schema(&self) -> &Schema {
    todo!()
  }
}

impl_plan_tree_node_for_leaf! {BatchSeqScan}
impl fmt::Display for BatchSeqScan {
  fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
    todo!()
  }
}
impl WithOrder for BatchSeqScan {}
impl WithDistribution for BatchSeqScan {}

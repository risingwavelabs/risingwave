use std::fmt;

use risingwave_common::catalog::Schema;

use crate::optimizer::property::{WithDistribution, WithOrder, WithSchema};

#[derive(Debug, Clone)]
pub struct LogicalScan {
  // TODO(catalog)
}
impl WithSchema for LogicalScan {
  fn schema(&self) -> &Schema {
    todo!()
  }
}
impl_plan_tree_node_for_leaf! {LogicalScan}
impl WithOrder for LogicalScan {}
impl WithDistribution for LogicalScan {}

impl fmt::Display for LogicalScan {
  fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
    todo!()
  }
}

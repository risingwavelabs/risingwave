use std::fmt;

use risingwave_common::catalog::Schema;

use super::ToStreamProst;
use crate::optimizer::property::{WithDistribution, WithOrder, WithSchema};

#[derive(Debug, Clone)]
pub struct StreamTableSource {
    // TODO(catalog)
}

impl WithSchema for StreamTableSource {
    fn schema(&self) -> &Schema {
        todo!()
    }
}

impl_plan_tree_node_for_leaf! {StreamTableSource}
impl fmt::Display for StreamTableSource {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        todo!()
    }
}

impl WithDistribution for StreamTableSource {}

impl WithOrder for StreamTableSource {}

impl ToStreamProst for StreamTableSource {}

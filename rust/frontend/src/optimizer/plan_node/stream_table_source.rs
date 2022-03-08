use std::fmt;

use risingwave_common::catalog::Schema;

use super::{StreamBase, ToStreamProst};
use crate::optimizer::property::{WithSchema};

/// `StreamTableSource` continuously streams data from internal table or various kinds of
/// external sources.
#[derive(Debug, Clone)]
pub struct StreamTableSource {
    pub base: StreamBase,
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

impl ToStreamProst for StreamTableSource {}

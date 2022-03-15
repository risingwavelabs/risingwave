use std::fmt;

use risingwave_common::catalog::Schema;

use super::{LogicalScan, StreamBase, ToStreamProst};
use crate::optimizer::property::{Distribution, WithSchema};

/// `StreamTableSource` continuously streams data from internal table or various kinds of
/// external sources.
#[derive(Debug, Clone)]
pub struct StreamTableSource {
    pub base: StreamBase,
    // TODO: replace this with actual table. Currently we place the logical scan node here only to
    // pass plan tests.
    logical: LogicalScan,
}

impl StreamTableSource {
    pub fn new(logical: LogicalScan) -> Self {
        let ctx = logical.base.ctx.clone();
        // TODO: derive from input
        let base = StreamBase {
            dist: Distribution::any().clone(),
            id: ctx.borrow_mut().get_id(),
            ctx: ctx.clone(),
        };
        Self { logical, base }
    }
}

impl WithSchema for StreamTableSource {
    fn schema(&self) -> &Schema {
        self.logical.schema()
    }
}

impl_plan_tree_node_for_leaf! {StreamTableSource}
impl fmt::Display for StreamTableSource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StreamTableSource {{ logical: {} }}", self.logical)
    }
}

impl ToStreamProst for StreamTableSource {}

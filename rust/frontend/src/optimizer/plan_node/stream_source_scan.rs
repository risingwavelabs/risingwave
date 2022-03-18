use std::fmt;

use risingwave_common::catalog::Schema;
use risingwave_pb::stream_plan::stream_node::Node as ProstStreamNode;

use super::{LogicalScan, StreamBase, ToStreamProst};
use crate::catalog::ColumnId;
use crate::optimizer::property::{Distribution, WithSchema};

/// `StreamSourceScan` represents a scan from source.
#[derive(Debug, Clone)]
pub struct StreamSourceScan {
    pub base: StreamBase,
    logical: LogicalScan,
}

impl StreamSourceScan {
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

impl WithSchema for StreamSourceScan {
    fn schema(&self) -> &Schema {
        self.logical.schema()
    }
}

impl_plan_tree_node_for_leaf! { StreamSourceScan }

impl fmt::Display for StreamSourceScan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "StreamSourceScan {{ table: {}, columns: {:?} }}",
            self.logical.table_name(),
            &self.logical.column_names()
        )
    }
}

impl ToStreamProst for StreamSourceScan {
    fn to_stream_prost_body(&self) -> ProstStreamNode {
        ProstStreamNode::MergeNode(Default::default())
    }
}

use std::fmt;

use risingwave_common::catalog::Schema;
use risingwave_pb::stream_plan::stream_node::Node as ProstStreamNode;
use risingwave_pb::stream_plan::StreamNode as ProstStreamPlan;

use super::{LogicalScan, StreamBase, ToStreamProst};
use crate::optimizer::property::{Distribution, WithSchema};

/// `StreamScan` is a virtual plan node to represent a stream scan. It will be converted to chain +
/// upstream table scan + batch table scan when converting to MView creation request.
#[derive(Debug, Clone)]
pub struct StreamScan {
    pub base: StreamBase,
    logical: LogicalScan,
}

impl StreamScan {
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

impl WithSchema for StreamScan {
    fn schema(&self) -> &Schema {
        self.logical.schema()
    }
}

impl_plan_tree_node_for_leaf! { StreamScan }

impl fmt::Display for StreamScan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("StreamScan")
            .field("logical", &self.logical)
            .finish()
    }
}

impl ToStreamProst for StreamScan {
    fn to_stream_prost_body(&self) -> ProstStreamNode {
        unreachable!("stream scan cannot be converted into a prost body -- call `adhoc_to_stream_prost` instead.")
    }
}

impl StreamScan {
    pub fn adhoc_to_stream_prost(&self) -> ProstStreamPlan {
        ProstStreamPlan {
            input: vec![
                ProstStreamPlan {
                    node: Some(ProstStreamNode::MergeNode(Default::default())),
                    ..Default::default()
                },
                ProstStreamPlan {
                    node: Some(ProstStreamNode::ChainNode(Default::default())),
                    ..Default::default()
                },
            ],
            node: Some(ProstStreamNode::ChainNode(Default::default())),
            ..Default::default()
        }
    }
}

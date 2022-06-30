use std::fmt;

use itertools::Itertools;
use risingwave_pb::stream_plan::expand_node::Keys;
use risingwave_pb::stream_plan::stream_node::NodeBody as ProstStreamNode;
use risingwave_pb::stream_plan::ExpandNode;

use super::{LogicalExpand, PlanBase, PlanRef, PlanTreeNodeUnary, ToStreamProst};
use crate::optimizer::property::Distribution;

#[derive(Debug, Clone)]
pub struct StreamExpand {
    pub base: PlanBase,
    logical: LogicalExpand,
}

impl StreamExpand {
    pub fn new(logical: LogicalExpand) -> Self {
        let base = PlanBase::new_stream(
            logical.base.ctx.clone(),
            logical.schema().clone(),
            logical.base.pk_indices.to_vec(),
            Distribution::SomeShard,
            logical.input().append_only(),
        );
        StreamExpand { base, logical }
    }

    pub fn expanded_keys(&self) -> &Vec<Vec<usize>> {
        self.logical.expanded_keys()
    }
}

impl fmt::Display for StreamExpand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "StreamExpand {{ expanded_keys: {:#?} }}",
            self.logical.expanded_keys()
        )
    }
}

impl PlanTreeNodeUnary for StreamExpand {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input))
    }
}

impl_plan_tree_node_for_unary! { StreamExpand }

impl ToStreamProst for StreamExpand {
    fn to_stream_prost_body(&self) -> ProstStreamNode {
        ProstStreamNode::Expand(ExpandNode {
            expanded_keys: self
                .expanded_keys()
                .iter()
                .map(|keys| keys_to_protobuf(keys))
                .collect_vec(),
        })
    }
}

fn keys_to_protobuf(keys: &[usize]) -> Keys {
    let keys = keys.iter().map(|key| *key as u32).collect_vec();
    Keys { keys }
}

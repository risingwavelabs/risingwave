use std::fmt;

use risingwave_pb::stream_plan::stream_node::NodeBody as ProstStreamNode;

use super::{PlanBase, PlanRef, PlanTreeNodeUnary, StreamNode};
use crate::stream_fragmenter::BuildFragmentGraphState;

#[derive(Clone, Debug)]
pub struct StreamRowIdGen {
    pub base: PlanBase,
    input: PlanRef,
    row_id_index: usize,
}

impl StreamRowIdGen {
    pub fn new(input: PlanRef, row_id_index: usize) -> Self {
        let base = PlanBase::new_stream(
            input.ctx(),
            input.schema().clone(),
            input.logical_pk().to_vec(),
            input.functional_dependency().clone(),
            input.distribution().clone(),
            input.append_only(),
        );
        Self {
            base,
            input,
            row_id_index,
        }
    }
}

impl fmt::Display for StreamRowIdGen {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "StreamRowIdGen {{ row_id_index: {} }}",
            self.row_id_index
        )
    }
}

impl PlanTreeNodeUnary for StreamRowIdGen {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.row_id_index)
    }
}

impl_plan_tree_node_for_unary! {StreamRowIdGen}

impl StreamNode for StreamRowIdGen {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> ProstStreamNode {
        use risingwave_pb::stream_plan::*;

        ProstStreamNode::RowIdGen(RowIdGenNode {
            row_id_index: self.row_id_index as _,
        })
    }
}

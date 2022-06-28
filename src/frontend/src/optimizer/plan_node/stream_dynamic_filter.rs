use std::fmt;

use risingwave_pb::stream_plan::stream_node::NodeBody;

use crate::optimizer::plan_node::{PlanBase, PlanTreeNodeBinary, ToStreamProst};
use crate::optimizer::PlanRef;
use crate::utils::Condition;

#[derive(Clone, Debug)]
pub struct StreamDynamicFilter {
    pub base: PlanBase,
    /// The predicate (formed with exactly one of < , <=, >, >=)
    predicate: Condition,
    // dist_key_l: Distribution,
    left: PlanRef,
    right: PlanRef,
}

impl StreamDynamicFilter {
    pub fn new(predicate: Condition, left: PlanRef, right: PlanRef) -> Self {
        // TODO: derive from input
        let base = PlanBase::new_stream(
            left.ctx(),
            left.schema().clone(),
            left.pk_indices().to_vec(),
            left.distribution().clone(),
            false, /* we can have a new abstraction for append only and monotonically increasing
                    * in the future */
        );
        Self {
            base,
            predicate,
            left,
            right,
        }
    }
}

impl fmt::Display for StreamDynamicFilter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StreamDynamicFilter {{ predicate: {} }}", self.predicate)
    }
}

impl PlanTreeNodeBinary for StreamDynamicFilter {
    fn left(&self) -> PlanRef {
        self.left.clone()
    }

    fn right(&self) -> PlanRef {
        self.right.clone()
    }

    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self {
        Self::new(self.predicate.clone(), left, right)
    }
}

impl_plan_tree_node_for_binary! { StreamDynamicFilter }

impl ToStreamProst for StreamDynamicFilter {
    fn to_stream_prost_body(&self) -> NodeBody {
        unimplemented!()
    }
}

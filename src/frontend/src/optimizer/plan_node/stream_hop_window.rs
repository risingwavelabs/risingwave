use std::fmt;

use risingwave_common::types::{DataType, IntervalUnit, Scalar};
use risingwave_pb::stream_plan::stream_node::Node as ProstStreamNode;
use risingwave_pb::stream_plan::HopWindowNode;

use super::{LogicalHopWindow, PlanBase, PlanRef, PlanTreeNodeUnary, ToStreamProst};
use crate::expr::{Expr, InputRefDisplay, Literal};

/// [`StreamHopWindow`] represents a hop window table function.
#[derive(Debug, Clone)]
pub struct StreamHopWindow {
    pub base: PlanBase,
    logical: LogicalHopWindow,
}

impl StreamHopWindow {
    pub fn new(logical: LogicalHopWindow) -> Self {
        let ctx = logical.base.ctx.clone();
        let pk_indices = logical.base.pk_indices.to_vec();
        let input = logical.input();
        let input_dist = input.distribution();

        let base = PlanBase::new_stream(
            ctx,
            logical.schema().clone(),
            pk_indices,
            logical.distribution().clone(),
            logical.input().append_only(),
        );
        Self { base, logical }
    }
}

impl fmt::Display for StreamHopWindow {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("StreamHopWindow")
            .field("time_col", &InputRefDisplay(self.logical.time_col.index()))
            .field("slide", &self.logical.window_slide)
            .field("size", &self.logical.window_size)
            .finish_non_exhaustive()
    }
}

impl PlanTreeNodeUnary for StreamHopWindow {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input))
    }
}

impl_plan_tree_node_for_unary! {StreamHopWindow}

impl ToStreamProst for StreamHopWindow {
    fn to_stream_prost_body(&self) -> ProstStreamNode {
        let interval_to_literal = |interval: IntervalUnit| {
            Literal::new(Some(interval.to_scalar_value()), DataType::Interval)
        };
        ProstStreamNode::HopWindowNode(HopWindowNode {
            time_col: Some(self.logical.time_col.to_protobuf()),
            window_slide: Some(interval_to_literal(self.logical.window_slide).to_protobuf()),
            window_size: Some(interval_to_literal(self.logical.window_size).to_protobuf()),
        })
    }
}

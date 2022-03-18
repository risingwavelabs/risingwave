use std::fmt;

use risingwave_common::catalog::Schema;
use risingwave_pb::stream_plan::stream_node::Node as ProstStreamNode;
use risingwave_pb::stream_plan::FilterNode;

use super::{LogicalFilter, PlanRef, PlanTreeNodeUnary, ToStreamProst};
use crate::optimizer::plan_node::StreamBase;
use crate::optimizer::property::{Distribution, WithSchema};
use crate::utils::Condition;

/// `StreamFilter` implements [`super::LogicalFilter`]
#[derive(Debug, Clone)]
pub struct StreamFilter {
    pub base: StreamBase,
    logical: LogicalFilter,
}

impl StreamFilter {
    pub fn new(logical: LogicalFilter) -> Self {
        let ctx = logical.base.ctx.clone();
        // TODO: derive from input
        let base = StreamBase {
            dist: Distribution::any().clone(),
            id: ctx.borrow_mut().get_id(),
            ctx: ctx.clone(),
        };
        StreamFilter { logical, base }
    }

    pub fn predicate(&self) -> &Condition {
        self.logical.predicate()
    }
}

impl fmt::Display for StreamFilter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StreamFilter {{ predicate: {} }}", self.predicate())
    }
}

impl PlanTreeNodeUnary for StreamFilter {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input))
    }
}

impl_plan_tree_node_for_unary! { StreamFilter }

impl WithSchema for StreamFilter {
    fn schema(&self) -> &Schema {
        self.logical.schema()
    }
}

impl ToStreamProst for StreamFilter {
    fn to_stream_prost_body(&self) -> ProstStreamNode {
        ProstStreamNode::FilterNode(FilterNode {
            search_condition: Some(self.predicate().as_expr().to_protobuf()),
        })
    }
}

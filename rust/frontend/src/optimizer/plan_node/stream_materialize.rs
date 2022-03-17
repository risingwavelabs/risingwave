use std::fmt;

use risingwave_common::catalog::Schema;
use risingwave_pb::stream_plan::stream_node::Node as ProstStreamNode;

use super::{PlanRef, PlanTreeNodeUnary, ToStreamProst};
use crate::catalog::TableId;
use crate::optimizer::plan_node::StreamBase;
use crate::optimizer::property::{Distribution, WithSchema};
use crate::session::QueryContextRef;

/// Materializes a stream.
#[derive(Debug, Clone)]
pub struct StreamMaterialize {
    pub base: StreamBase,
    input: PlanRef,
    schema: Schema,
    table_id: TableId,
}

impl StreamMaterialize {
    pub fn new(ctx: QueryContextRef, input: PlanRef, table_id: TableId) -> Self {
        // TODO: derive from input
        let base = StreamBase {
            dist: Distribution::any().clone(),
            id: ctx.borrow_mut().get_id(),
            ctx: ctx.clone(),
        };
        Self {
            base,
            schema: input.schema().clone(),
            input,
            table_id,
        }
    }
}

impl fmt::Display for StreamMaterialize {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StreamMaterialize {{ table_id: {} }}", self.table_id)
    }
}

impl PlanTreeNodeUnary for StreamMaterialize {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.base.ctx.clone(), input, self.table_id)
    }
}

impl_plan_tree_node_for_unary! { StreamMaterialize }

impl WithSchema for StreamMaterialize {
    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl ToStreamProst for StreamMaterialize {
    fn to_stream_prost_body(&self) -> ProstStreamNode {
        ProstStreamNode::MaterializeNode(Default::default())
    }
}

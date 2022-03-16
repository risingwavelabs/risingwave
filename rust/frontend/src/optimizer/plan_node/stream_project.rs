use std::fmt;

use risingwave_common::catalog::Schema;
use risingwave_pb::stream_plan::stream_node::Node as ProstStreamNode;
use risingwave_pb::stream_plan::ProjectNode;

use super::{LogicalProject, PlanRef, PlanTreeNodeUnary, StreamBase, ToStreamProst};
use crate::expr::Expr;
use crate::optimizer::property::{Distribution, WithSchema};

/// `StreamProject` implements [`super::LogicalProject`] to evaluate specified expressions on input
/// rows.
#[derive(Debug, Clone)]
pub struct StreamProject {
    pub base: StreamBase,
    logical: LogicalProject,
}

impl fmt::Display for StreamProject {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.logical.fmt_with_name(f, "StreamProject")
    }
}

impl StreamProject {
    pub fn new(logical: LogicalProject) -> Self {
        let ctx = logical.base.ctx.clone();
        // TODO: derive from input
        let base = StreamBase {
            dist: Distribution::any().clone(),
            id: ctx.borrow_mut().get_id(),
            ctx: ctx.clone(),
        };
        StreamProject { logical, base }
    }
}

impl PlanTreeNodeUnary for StreamProject {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }
    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input))
    }
}
impl_plan_tree_node_for_unary! {StreamProject}
impl WithSchema for StreamProject {
    fn schema(&self) -> &Schema {
        self.logical.schema()
    }
}

impl ToStreamProst for StreamProject {
    fn to_stream_prost_body(&self) -> ProstStreamNode {
        ProstStreamNode::ProjectNode(ProjectNode {
            select_list: self.logical.exprs().iter().map(Expr::to_prost).collect(),
        })
    }
}

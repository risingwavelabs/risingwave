use std::fmt;

use risingwave_common::catalog::Schema;

use super::{LogicalProject, PlanRef, PlanTreeNodeUnary, StreamBase, ToStreamProst};
use crate::optimizer::property::{Distribution, WithOrder, WithSchema};

/// `StreamProject` implements [`super::LogicalProject`] to evaluate specified expressions on input
/// rows.
#[derive(Debug, Clone)]
pub struct StreamProject {
    pub base: StreamBase,
    logical: LogicalProject,
}

impl fmt::Display for StreamProject {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        todo!()
    }
}

impl StreamProject {
    pub fn new(logical: LogicalProject) -> Self {
        // TODO: derive from input
        let base = StreamBase {
            dist: Distribution::any().clone(),
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

impl ToStreamProst for StreamProject {}

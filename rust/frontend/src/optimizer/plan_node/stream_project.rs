use std::fmt;

use risingwave_common::catalog::Schema;

use super::{IntoPlanRef, LogicalProject, PlanRef, PlanTreeNodeUnary, ToStreamProst};
use crate::optimizer::property::{WithDistribution, WithOrder, WithSchema};

#[derive(Debug, Clone)]
pub struct StreamProject {
    logical: LogicalProject,
}
impl fmt::Display for StreamProject {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        todo!()
    }
}
impl StreamProject {
    pub fn new(logical: LogicalProject) -> Self {
        StreamProject { logical }
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

impl WithDistribution for StreamProject {}
impl WithOrder for StreamProject {}
impl ToStreamProst for StreamProject {}

use std::fmt;

use risingwave_common::catalog::Schema;

use super::{
    BatchBase, LogicalProject, PlanRef, PlanTreeNodeUnary, ToBatchProst, ToDistributedBatch,
};
use crate::optimizer::property::{Distribution, Order, WithSchema};

/// `BatchProject` implements [`super::LogicalProject`] to evaluate specified expressions on input
/// rows
#[derive(Debug, Clone)]
pub struct BatchProject {
    pub base: BatchBase,
    logical: LogicalProject,
}

impl BatchProject {
    pub fn new(logical: LogicalProject) -> Self {
        // TODO: derive from input
        let base = BatchBase {
            order: Order::any().clone(),
            dist: Distribution::any().clone(),
        };
        BatchProject { logical, base }
    }
}

impl fmt::Display for BatchProject {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        todo!()
    }
}

impl PlanTreeNodeUnary for BatchProject {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }
    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input))
    }
}

impl_plan_tree_node_for_unary! { BatchProject }

impl WithSchema for BatchProject {
    fn schema(&self) -> &Schema {
        self.logical.schema()
    }
}

impl ToDistributedBatch for BatchProject {
    fn to_distributed(&self) -> PlanRef {
        let new_input = self
            .input()
            .to_distributed_with_required(self.input_order_required(), Distribution::any());
        self.clone_with_input(new_input).into()
    }
}

impl ToBatchProst for BatchProject {}

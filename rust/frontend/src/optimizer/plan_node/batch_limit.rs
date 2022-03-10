use std::fmt;

use risingwave_common::catalog::Schema;

use super::{
    BatchBase, LogicalLimit, PlanRef, PlanTreeNodeUnary, ToBatchProst, ToDistributedBatch,
};
use crate::optimizer::property::{Distribution, WithSchema};

/// `BatchLimit` implements [`super::LogicalLimit`] to fetch specified rows from input
#[derive(Debug, Clone)]
pub struct BatchLimit {
    pub base: BatchBase,
    logical: LogicalLimit,
}

impl BatchLimit {
    pub fn new(logical: LogicalLimit) -> Self {
        let base = BatchBase {
            order: logical.input().order().clone(),
            dist: logical.input().distribution().clone(),
        };
        BatchLimit { logical, base }
    }
}

impl fmt::Display for BatchLimit {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        todo!()
    }
}

impl PlanTreeNodeUnary for BatchLimit {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }
    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input))
    }
}
impl_plan_tree_node_for_unary! {BatchLimit}

impl WithSchema for BatchLimit {
    fn schema(&self) -> &Schema {
        self.logical.schema()
    }
}

impl ToDistributedBatch for BatchLimit {
    fn to_distributed(&self) -> PlanRef {
        let new_input = self
            .input()
            .to_distributed_with_required(self.input_order_required(), Distribution::any());
        self.clone_with_input(new_input).into()
    }
}

impl ToBatchProst for BatchLimit {}

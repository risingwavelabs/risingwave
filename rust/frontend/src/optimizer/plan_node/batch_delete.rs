use std::fmt;

use risingwave_common::catalog::Schema;

use super::{
    BatchBase, LogicalDelete, PlanRef, PlanTreeNodeUnary, ToBatchProst, ToDistributedBatch,
};
use crate::optimizer::property::{Distribution, Order, WithSchema};

/// `BatchDelete` implements [`LogicalDelete`]
#[derive(Debug, Clone)]
pub struct BatchDelete {
    pub base: BatchBase,
    logical: LogicalDelete,
}

impl BatchDelete {
    pub fn new(logical: LogicalDelete) -> Self {
        let ctx = logical.base.ctx.clone();
        let id = ctx.borrow_mut().get_id();
        let base = BatchBase {
            id,
            order: Order::any().clone(),
            dist: Distribution::any().clone(),
            ctx,
        };
        Self { base, logical }
    }
}

impl fmt::Display for BatchDelete {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.logical.fmt_with_name(f, "BatchDelete")
    }
}

impl PlanTreeNodeUnary for BatchDelete {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input))
    }
}

impl_plan_tree_node_for_unary! { BatchDelete }

impl WithSchema for BatchDelete {
    fn schema(&self) -> &Schema {
        self.logical.schema()
    }
}

impl ToDistributedBatch for BatchDelete {
    fn to_distributed(&self) -> PlanRef {
        let new_input = self
            .input()
            .to_distributed_with_required(self.input_order_required(), Distribution::any());
        self.clone_with_input(new_input).into()
    }
}

impl ToBatchProst for BatchDelete {}

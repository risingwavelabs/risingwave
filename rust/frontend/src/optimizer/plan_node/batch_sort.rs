use std::fmt;

use risingwave_common::catalog::Schema;

use super::{BatchBase, PlanRef, PlanTreeNodeUnary, ToBatchProst, ToDistributedBatch};
use crate::optimizer::property::{Distribution, Order, WithOrder, WithSchema};

/// `BatchSort` buffers all data from input and sort these rows by specified order, providing the
/// collation required by user or parent plan node.
#[derive(Debug, Clone)]
pub struct BatchSort {
    pub base: BatchBase,
    input: PlanRef,
    schema: Schema,
}

impl BatchSort {
    pub fn new(input: PlanRef, order: Order) -> Self {
        let ctx = input.ctx();
        let schema = input.schema().clone();
        let dist = input.distribution().clone();
        let base = BatchBase {
            order,
            dist,
            ctx: ctx.clone(),
            id: ctx.borrow_mut().get_id(),
        };
        BatchSort {
            input,
            base,
            schema,
        }
    }
}

impl fmt::Display for BatchSort {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("BatchSort")
            .field("order", self.order())
            .finish()
    }
}

impl PlanTreeNodeUnary for BatchSort {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }
    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.base.order.clone())
    }
}
impl_plan_tree_node_for_unary! {BatchSort}

impl WithSchema for BatchSort {
    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl ToDistributedBatch for BatchSort {
    fn to_distributed(&self) -> PlanRef {
        let new_input = self
            .input()
            .to_distributed_with_required(self.input_order_required(), Distribution::any());
        self.clone_with_input(new_input).into()
    }
}

impl ToBatchProst for BatchSort {}

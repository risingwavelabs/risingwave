use std::fmt;

use risingwave_common::catalog::Schema;

use super::logical_agg::PlanAggCall;
use super::{BatchBase, LogicalAgg, PlanRef, PlanTreeNodeUnary, ToBatchProst, ToDistributedBatch};
use crate::optimizer::property::{Distribution, Order, WithSchema};

#[derive(Debug, Clone)]
pub struct BatchSimpleAgg {
    pub base: BatchBase,
    logical: LogicalAgg,
}

impl BatchSimpleAgg {
    pub fn new(logical: LogicalAgg) -> Self {
        let ctx = logical.base.ctx.clone();
        let base = BatchBase {
            order: Order::any().clone(),
            dist: Distribution::Single,
            id: ctx.borrow_mut().get_id(),
            ctx: ctx.clone(),
        };
        BatchSimpleAgg { logical, base }
    }
    pub fn agg_calls(&self) -> &[PlanAggCall] {
        self.logical.agg_calls()
    }
}

impl fmt::Display for BatchSimpleAgg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("BatchSimpleAgg")
            .field("aggs", &self.agg_calls())
            .finish()
    }
}

impl PlanTreeNodeUnary for BatchSimpleAgg {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input))
    }
}
impl_plan_tree_node_for_unary! { BatchSimpleAgg }

impl WithSchema for BatchSimpleAgg {
    fn schema(&self) -> &Schema {
        self.logical.schema()
    }
}

impl ToDistributedBatch for BatchSimpleAgg {
    fn to_distributed(&self) -> PlanRef {
        let new_input = self
            .input()
            .to_distributed_with_required(self.input_order_required(), &Distribution::Single);
        self.clone_with_input(new_input).into()
    }
}

impl ToBatchProst for BatchSimpleAgg {}

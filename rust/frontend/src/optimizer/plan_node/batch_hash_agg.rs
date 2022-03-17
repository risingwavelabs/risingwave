use std::fmt;

use risingwave_common::catalog::Schema;

use super::logical_agg::PlanAggCall;
use super::{BatchBase, LogicalAgg, PlanRef, PlanTreeNodeUnary, ToBatchProst, ToDistributedBatch};
use crate::optimizer::property::{Distribution, Order, WithSchema};

#[derive(Debug, Clone)]
pub struct BatchHashAgg {
    pub base: BatchBase,
    logical: LogicalAgg,
}

impl BatchHashAgg {
    pub fn new(logical: LogicalAgg) -> Self {
        let ctx = logical.base.ctx.clone();
        let base = BatchBase {
            order: Order::any().clone(),
            dist: Distribution::HashShard(logical.group_keys().to_vec()),
            id: ctx.borrow_mut().get_id(),
            ctx: ctx.clone(),
        };
        BatchHashAgg { logical, base }
    }
    pub fn agg_calls(&self) -> &[PlanAggCall] {
        self.logical.agg_calls()
    }
    pub fn group_keys(&self) -> &[usize] {
        self.logical.group_keys()
    }
}

impl fmt::Display for BatchHashAgg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("BatchHashAgg")
            .field("group_keys", &self.group_keys())
            .field("aggs", &self.agg_calls())
            .finish()
    }
}

impl PlanTreeNodeUnary for BatchHashAgg {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input))
    }
}
impl_plan_tree_node_for_unary! { BatchHashAgg }

impl WithSchema for BatchHashAgg {
    fn schema(&self) -> &Schema {
        self.logical.schema()
    }
}

impl ToDistributedBatch for BatchHashAgg {
    fn to_distributed(&self) -> PlanRef {
        let new_input = self.input().to_distributed_with_required(
            self.input_order_required(),
            &Distribution::HashShard(self.group_keys().to_vec()),
        );
        self.clone_with_input(new_input).into()
    }
}

impl ToBatchProst for BatchHashAgg {}

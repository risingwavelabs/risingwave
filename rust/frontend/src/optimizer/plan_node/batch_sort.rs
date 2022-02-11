use std::fmt;

use risingwave_common::catalog::Schema;

use super::{IntoPlanRef, PlanRef, PlanTreeNodeUnary, ToDistributedBatch};
use crate::optimizer::property::{Distribution, Order, WithDistribution, WithOrder, WithSchema};

#[derive(Debug, Clone)]
pub struct BatchSort {
    order: Order,
    input: PlanRef,
    schema: Schema,
    dist: Distribution,
}
impl BatchSort {
    pub fn new(input: PlanRef, order: Order) -> Self {
        let schema = input.schema().clone();
        let dist = input.distribution().clone();
        BatchSort {
            input,
            order,
            schema,
            dist,
        }
    }
}
impl fmt::Display for BatchSort {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        todo!()
    }
}
impl PlanTreeNodeUnary for BatchSort {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }
    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.order.clone())
    }
}
impl_plan_tree_node_for_unary! {BatchSort}
impl WithOrder for BatchSort {
    fn order(&self) -> &Order {
        &self.order
    }
}
impl WithDistribution for BatchSort {
    fn distribution(&self) -> &Distribution {
        &self.dist
    }
}
impl WithSchema for BatchSort {
    fn schema(&self) -> &Schema {
        &self.schema
    }
}
impl ToDistributedBatch for BatchSort {
    fn to_distributed(&self) -> PlanRef {
        let new_input = self
            .input()
            .to_distributed_with_required(&self.input_order_required(), &Distribution::any());
        self.clone_with_input(new_input).into_plan_ref()
    }
}

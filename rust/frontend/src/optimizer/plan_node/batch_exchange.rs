use std::fmt;

use risingwave_common::catalog::Schema;

use super::{IntoPlanRef, PlanRef, PlanTreeNodeUnary, ToBatchProst, ToDistributedBatch};
use crate::optimizer::property::{Distribution, Order, WithDistribution, WithOrder, WithSchema};

#[derive(Debug, Clone)]
pub struct BatchExchange {
    order: Order,
    input: PlanRef,
    schema: Schema,
    dist: Distribution,
}

impl BatchExchange {
    pub fn new(input: PlanRef, order: Order, dist: Distribution) -> Self {
        let schema = input.schema().clone();
        BatchExchange {
            input,
            order,
            schema,
            dist,
        }
    }
}

impl fmt::Display for BatchExchange {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        todo!()
    }
}

impl PlanTreeNodeUnary for BatchExchange {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }
    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.order.clone(), self.distribution().clone())
    }
}
impl_plan_tree_node_for_unary! {BatchExchange}
impl WithOrder for BatchExchange {
    fn order(&self) -> &Order {
        &self.order
    }
}

impl WithDistribution for BatchExchange {
    fn distribution(&self) -> &Distribution {
        &self.dist
    }
}

impl WithSchema for BatchExchange {
    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl ToDistributedBatch for BatchExchange {
    fn to_distributed(&self) -> PlanRef {
        unreachable!()
    }
}

impl ToBatchProst for BatchExchange {}

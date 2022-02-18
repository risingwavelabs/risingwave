use std::fmt;

use risingwave_common::catalog::Schema;

use super::{IntoPlanRef, PlanRef, PlanTreeNodeUnary, ToStreamProst};
use crate::optimizer::property::{Distribution, WithDistribution, WithOrder, WithSchema};

#[derive(Debug, Clone)]
pub struct StreamExchange {
    input: PlanRef,
    schema: Schema,
    dist: Distribution,
}
impl StreamExchange {
    pub fn new(input: PlanRef, dist: Distribution) -> Self {
        let schema = input.schema().clone();
        StreamExchange {
            input,
            schema,
            dist,
        }
    }
}
impl fmt::Display for StreamExchange {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        todo!()
    }
}
impl PlanTreeNodeUnary for StreamExchange {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }
    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.distribution().clone())
    }
}
impl_plan_tree_node_for_unary! {StreamExchange}
impl WithDistribution for StreamExchange {
    fn distribution(&self) -> &Distribution {
        &self.dist
    }
}
impl WithOrder for StreamExchange {}

impl WithSchema for StreamExchange {
    fn schema(&self) -> &Schema {
        &self.schema
    }
}
impl ToStreamProst for StreamExchange {}

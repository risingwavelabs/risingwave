use std::fmt;

use risingwave_common::catalog::Schema;

use super::{PlanRef, PlanTreeNodeUnary, StreamBase, ToStreamProst};
use crate::optimizer::property::{Distribution, WithDistribution, WithSchema};

/// `StreamExchange` imposes a particular distribution on its input
/// without changing its content.
#[derive(Debug, Clone)]
pub struct StreamExchange {
    pub base: StreamBase,
    input: PlanRef,
    schema: Schema,
}

impl StreamExchange {
    pub fn new(input: PlanRef, dist: Distribution) -> Self {
        let schema = input.schema().clone();
        let base = StreamBase { dist };
        StreamExchange {
            input,
            schema,
            base,
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

impl WithSchema for StreamExchange {
    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl ToStreamProst for StreamExchange {}

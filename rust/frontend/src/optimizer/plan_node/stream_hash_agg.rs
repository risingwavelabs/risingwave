use std::fmt;

use risingwave_common::catalog::Schema;

use super::logical_agg::PlanAggCall;
use super::{LogicalAgg, PlanRef, PlanTreeNodeUnary, StreamBase, ToStreamProst};
use crate::optimizer::property::{Distribution, WithSchema};

#[derive(Debug, Clone)]
pub struct StreamHashAgg {
    pub base: StreamBase,
    logical: LogicalAgg,
}

impl StreamHashAgg {
    pub fn new(logical: LogicalAgg) -> Self {
        let ctx = logical.base.ctx.clone();
        let base = StreamBase {
            dist: Distribution::any().clone(),
            id: ctx.borrow_mut().get_id(),
            ctx: ctx.clone(),
        };
        StreamHashAgg { logical, base }
    }
    pub fn agg_calls(&self) -> &[PlanAggCall] {
        self.logical.agg_calls()
    }
    pub fn group_keys(&self) -> &[usize] {
        self.logical.group_keys()
    }
}

impl fmt::Display for StreamHashAgg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("StreamHashAgg")
            .field("aggs", &self.agg_calls())
            .field("group_keys", &self.group_keys())
            .finish()
    }
}

impl PlanTreeNodeUnary for StreamHashAgg {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input))
    }
}
impl_plan_tree_node_for_unary! { StreamHashAgg }

impl WithSchema for StreamHashAgg {
    fn schema(&self) -> &Schema {
        self.logical.schema()
    }
}

impl ToStreamProst for StreamHashAgg {}

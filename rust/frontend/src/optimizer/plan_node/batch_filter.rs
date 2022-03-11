use std::fmt;

use risingwave_common::catalog::Schema;

use super::{LogicalFilter, PlanRef, PlanTreeNodeUnary, ToBatchProst, ToDistributedBatch};
use crate::optimizer::property::{Distribution, WithDistribution, WithOrder, WithSchema};
use crate::utils::Condition;

/// `BatchFilter` implements [`super::LogicalFilter`]
#[derive(Debug, Clone)]
pub struct BatchFilter {
    logical: LogicalFilter,
}

impl BatchFilter {
    pub fn new(logical: LogicalFilter) -> Self {
        BatchFilter { logical }
    }

    pub fn predicate(&self) -> &Condition {
        self.logical.predicate()
    }
}

impl fmt::Display for BatchFilter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BatchFilter {{ predicate: {:?} }}", self.predicate())
    }
}

impl PlanTreeNodeUnary for BatchFilter {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input))
    }
}

impl_plan_tree_node_for_unary! { BatchFilter }

impl WithOrder for BatchFilter {}

impl WithDistribution for BatchFilter {}

impl WithSchema for BatchFilter {
    fn schema(&self) -> &Schema {
        self.logical.schema()
    }
}

impl ToDistributedBatch for BatchFilter {
    fn to_distributed(&self) -> PlanRef {
        let new_input = self
            .input()
            .to_distributed_with_required(self.input_order_required(), Distribution::any());
        self.clone_with_input(new_input).into()
    }
}

impl ToBatchProst for BatchFilter {}

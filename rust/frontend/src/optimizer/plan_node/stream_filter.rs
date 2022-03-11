use std::fmt;

use risingwave_common::catalog::Schema;

use super::{LogicalFilter, PlanRef, PlanTreeNodeUnary, ToStreamProst};
use crate::optimizer::property::{WithDistribution, WithOrder, WithSchema};
use crate::utils::Condition;

/// `StreamFilter` implements [`super::LogicalFilter`]
#[derive(Debug, Clone)]
pub struct StreamFilter {
    logical: LogicalFilter,
}

impl StreamFilter {
    pub fn new(logical: LogicalFilter) -> Self {
        StreamFilter { logical }
    }

    pub fn predicate(&self) -> &Condition {
        self.logical.predicate()
    }
}

impl fmt::Display for StreamFilter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StreamFilter {{ predicate: {:?} }}", self.predicate())
    }
}

impl PlanTreeNodeUnary for StreamFilter {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input))
    }
}

impl_plan_tree_node_for_unary! { StreamFilter }

impl WithOrder for StreamFilter {}

impl WithDistribution for StreamFilter {}

impl WithSchema for StreamFilter {
    fn schema(&self) -> &Schema {
        self.logical.schema()
    }
}

impl ToStreamProst for StreamFilter {}

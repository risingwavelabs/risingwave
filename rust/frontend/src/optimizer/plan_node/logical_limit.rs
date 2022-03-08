use std::fmt;

use fixedbitset::FixedBitSet;
use risingwave_common::catalog::Schema;

use super::{BatchLimit, ColPrunable, LogicalBase, PlanRef, PlanTreeNodeUnary, ToBatch, ToStream};
use crate::optimizer::property::{WithDistribution, WithOrder, WithSchema};

/// `LogicalLimit` fetches up to `limit` rows from `offset`
#[derive(Debug, Clone)]
pub struct LogicalLimit {
    pub(super) base: LogicalBase,
    input: PlanRef,
    limit: usize,
    offset: usize,
}

impl LogicalLimit {
    fn new(input: PlanRef, limit: usize, offset: usize) -> Self {
        let schema = input.schema().clone();
        let base = LogicalBase { schema };
        LogicalLimit {
            input,
            limit,
            offset,
            base,
        }
    }

    /// the function will check if the cond is bool expression
    pub fn create(input: PlanRef, limit: usize, offset: usize) -> PlanRef {
        Self::new(input, limit, offset).into()
    }
}

impl PlanTreeNodeUnary for LogicalLimit {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }
    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.limit, self.offset)
    }
}
impl_plan_tree_node_for_unary! {LogicalLimit}
impl fmt::Display for LogicalLimit {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        todo!()
    }
}

impl WithOrder for LogicalLimit {}

impl WithDistribution for LogicalLimit {}

impl ColPrunable for LogicalLimit {
    fn prune_col(&self, required_cols: &FixedBitSet) -> PlanRef {
        self.must_contain_columns(required_cols);

        let new_input = self.input.prune_col(required_cols);
        self.clone_with_input(new_input).into()
    }
}

impl ToBatch for LogicalLimit {
    fn to_batch(&self) -> PlanRef {
        let new_input = self.input().to_batch();
        let new_logical = self.clone_with_input(new_input);
        BatchLimit::new(new_logical).into()
    }
}

impl ToStream for LogicalLimit {
    fn to_stream(&self) -> PlanRef {
        panic!("there is no limit stream operator");
    }
}

use std::fmt;

use risingwave_common::catalog::Schema;

use super::{ColPrunable, PlanRef, PlanTreeNodeUnary, ToBatch, ToStream};
use crate::optimizer::property::{FieldOrder, Order, WithDistribution, WithOrder, WithSchema};
use crate::utils::ColIndexMapping;

/// `LogicalTopN` sorts the input data and fetches up to `limit` rows from `offset`
#[derive(Debug, Clone)]
pub struct LogicalTopN {
    input: PlanRef,
    limit: usize,
    offset: usize,
    schema: Schema,
    order: Order,
}

impl LogicalTopN {
    fn new(input: PlanRef, limit: usize, offset: usize, order: Order) -> Self {
        let schema = input.schema().clone();
        LogicalTopN {
            input,
            limit,
            offset,
            schema,
            order,
        }
    }

    /// the function will check if the cond is bool expression
    pub fn create(input: PlanRef, limit: usize, offset: usize, order: Order) -> PlanRef {
        Self::new(input, limit, offset, order).into()
    }
}

impl PlanTreeNodeUnary for LogicalTopN {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }
    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.limit, self.offset, self.order.clone())
    }
}
impl_plan_tree_node_for_unary! {LogicalTopN}
impl fmt::Display for LogicalTopN {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        todo!()
    }
}

impl WithOrder for LogicalTopN {}

impl WithDistribution for LogicalTopN {}

impl WithSchema for LogicalTopN {
    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl ColPrunable for LogicalTopN {
    fn prune_col(&self, required_cols: &fixedbitset::FixedBitSet) -> PlanRef {
        let mapping = ColIndexMapping::with_remaining_columns(required_cols);
        let new_order = Order {
            field_order: self
                .order
                .field_order
                .iter()
                .map(|fo| FieldOrder {
                    index: mapping.map(fo.index),
                    direct: fo.direct.clone(),
                })
                .collect(),
        };
        let new_input = self.input.prune_col(required_cols);
        Self::new(new_input, self.limit, self.offset, new_order).into()
    }
}

impl ToBatch for LogicalTopN {
    fn to_batch(&self) -> PlanRef {
        todo!()
    }
}

impl ToStream for LogicalTopN {
    fn to_stream(&self) -> PlanRef {
        todo!()
    }
}

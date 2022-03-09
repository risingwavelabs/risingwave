use std::fmt;

use fixedbitset::FixedBitSet;

use super::{ColPrunable, LogicalBase, PlanRef, PlanTreeNodeUnary, ToBatch, ToStream};
use crate::optimizer::plan_node::LogicalProject;
use crate::optimizer::property::{FieldOrder, Order, WithSchema};
use crate::utils::ColIndexMapping;

/// `LogicalTopN` sorts the input data and fetches up to `limit` rows from `offset`
#[derive(Debug, Clone)]
pub struct LogicalTopN {
    pub base: LogicalBase,
    input: PlanRef,
    limit: usize,
    offset: usize,
    order: Order,
}

impl LogicalTopN {
    fn new(input: PlanRef, limit: usize, offset: usize, order: Order) -> Self {
        let schema = input.schema().clone();
        let base = LogicalBase { schema };
        LogicalTopN {
            input,
            limit,
            offset,
            base,
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

impl ColPrunable for LogicalTopN {
    fn prune_col(&self, required_cols: &FixedBitSet) -> PlanRef {
        self.must_contain_columns(required_cols);

        let mut input_required_cols = required_cols.clone();
        self.order
            .field_order
            .iter()
            .for_each(|fo| input_required_cols.insert(fo.index));

        let mapping = ColIndexMapping::with_remaining_columns(&input_required_cols);
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
        let top_n = Self::new(new_input, self.limit, self.offset, new_order).into();

        if *required_cols == input_required_cols {
            top_n
        } else {
            LogicalProject::with_mapping(
                top_n,
                ColIndexMapping::with_remaining_columns(
                    &required_cols.ones().map(|i| mapping.map(i)).collect(),
                ),
            )
            .into()
        }
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

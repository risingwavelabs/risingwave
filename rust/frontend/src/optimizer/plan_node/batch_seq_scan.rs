use std::fmt;

use risingwave_common::catalog::Schema;

use super::{BatchBase, PlanRef, ToBatchProst, ToDistributedBatch};
use crate::optimizer::plan_node::LogicalScan;
use crate::optimizer::property::{Distribution, Order, WithSchema};

/// `BatchSeqScan` implements [`super::LogicalScan`] to scan from a row-oriented table
#[derive(Debug, Clone)]
pub struct BatchSeqScan {
    pub base: BatchBase,
    logical: LogicalScan,
}

impl WithSchema for BatchSeqScan {
    fn schema(&self) -> &Schema {
        self.logical.schema()
    }
}

impl BatchSeqScan {
    pub fn new(logical: LogicalScan) -> Self {
        // TODO: derive from input
        let base = BatchBase {
            order: Order::any().clone(),
            dist: Distribution::any().clone(),
        };

        Self { logical, base }
    }
}

impl_plan_tree_node_for_leaf! {BatchSeqScan}
impl fmt::Display for BatchSeqScan {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        todo!()
    }
}

impl ToDistributedBatch for BatchSeqScan {
    fn to_distributed(&self) -> PlanRef {
        self.clone().into()
    }
}

impl ToBatchProst for BatchSeqScan {}

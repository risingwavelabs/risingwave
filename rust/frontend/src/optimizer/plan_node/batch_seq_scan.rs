use std::fmt;

use risingwave_common::catalog::Schema;

use super::{IntoPlanRef, PlanRef, ToBatchProst, ToDistributedBatch};
use crate::optimizer::plan_node::LogicalScan;
use crate::optimizer::property::{WithDistribution, WithOrder, WithSchema};

#[derive(Debug, Clone, Default)]
pub struct BatchSeqScan {
    logical: LogicalScan,
}
impl WithSchema for BatchSeqScan {
    fn schema(&self) -> &Schema {
        self.logical.schema()
    }
}

impl BatchSeqScan {
    pub fn new(logical: LogicalScan) -> Self {
        Self { logical }
    }
}

impl_plan_tree_node_for_leaf! {BatchSeqScan}
impl fmt::Display for BatchSeqScan {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        todo!()
    }
}
impl WithOrder for BatchSeqScan {}
impl WithDistribution for BatchSeqScan {}
impl ToDistributedBatch for BatchSeqScan {
    fn to_distributed(&self) -> PlanRef {
        self.clone().into_plan_ref()
    }
}
impl ToBatchProst for BatchSeqScan {}

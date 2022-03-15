use std::fmt;

use risingwave_common::catalog::Schema;

use super::{
    BatchBase, LogicalValues, PlanRef, PlanTreeNodeLeaf, ToBatchProst, ToDistributedBatch,
};
use crate::optimizer::property::{Distribution, Order, WithSchema};

#[derive(Debug, Clone)]
pub struct BatchValues {
    pub base: BatchBase,
    logical: LogicalValues,
}

impl PlanTreeNodeLeaf for BatchValues {}
impl_plan_tree_node_for_leaf!(BatchValues);

impl BatchValues {
    pub fn new(logical: LogicalValues) -> Self {
        let ctx = logical.base.ctx.clone();
        let base = BatchBase {
            order: Order::any().clone(),
            dist: Distribution::Broadcast,
            id: ctx.borrow_mut().get_id(),
            ctx: ctx.clone(),
        };
        BatchValues { logical, base }
    }
}

impl fmt::Display for BatchValues {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("BatchValues")
            .field("rows", &self.logical.rows())
            .finish()
    }
}

impl WithSchema for BatchValues {
    fn schema(&self) -> &Schema {
        self.logical.schema()
    }
}

impl ToDistributedBatch for BatchValues {
    fn to_distributed(&self) -> PlanRef {
        todo!()
    }
}

impl ToBatchProst for BatchValues {}

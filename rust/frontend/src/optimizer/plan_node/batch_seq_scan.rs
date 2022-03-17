use std::fmt;

use risingwave_common::catalog::Schema;
use risingwave_pb::plan::plan_node::NodeBody;
use risingwave_pb::plan::{CellBasedTableDesc, RowSeqScanNode};

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
        let ctx = logical.base.ctx.clone();
        // TODO: derive from input
        let base = BatchBase {
            id: ctx.borrow_mut().get_id(),
            order: Order::any().clone(),
            dist: Distribution::any().clone(),
            ctx: ctx.clone(),
        };

        Self { logical, base }
    }
}

impl_plan_tree_node_for_leaf! {BatchSeqScan}
impl fmt::Display for BatchSeqScan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut s = f.debug_struct("BatchScan");
        self.logical.fmt_fields(&mut s);
        s.finish()
    }
}

impl ToDistributedBatch for BatchSeqScan {
    fn to_distributed(&self) -> PlanRef {
        self.clone().into()
    }
}

impl ToBatchProst for BatchSeqScan {
    fn to_batch_prost_body(&self) -> NodeBody {
        // TODO(Bowen): Fix this serialization.
        NodeBody::RowSeqScan(RowSeqScanNode {
            table_desc: Some(CellBasedTableDesc {
                table_id: self.logical.table_id(),
                pk: vec![],
            }),
            ..Default::default()
        })
    }
}

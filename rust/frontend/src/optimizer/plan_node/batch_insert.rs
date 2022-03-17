use std::fmt;

use risingwave_common::catalog::Schema;
use risingwave_pb::plan::plan_node::NodeBody;
use risingwave_pb::plan::InsertNode;

use super::{LogicalInsert, PlanRef, PlanTreeNodeUnary, ToBatchProst, ToDistributedBatch};
use crate::optimizer::plan_node::BatchBase;
use crate::optimizer::property::{Distribution, Order, WithSchema};

/// `BatchInsert` implements [`LogicalInsert`]
#[derive(Debug, Clone)]
pub struct BatchInsert {
    pub base: BatchBase,
    logical: LogicalInsert,
}

impl BatchInsert {
    pub fn new(logical: LogicalInsert) -> Self {
        let ctx = logical.base.ctx.clone();
        // TODO: derive from input
        let base = BatchBase {
            order: Order::any().clone(),
            dist: Distribution::any().clone(),
            id: ctx.borrow_mut().get_id(),
            ctx: ctx.clone(),
        };
        BatchInsert { logical, base }
    }
}

impl fmt::Display for BatchInsert {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.logical.fmt_with_name(f, "BatchInsert")
    }
}

impl PlanTreeNodeUnary for BatchInsert {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input))
    }
}

impl_plan_tree_node_for_unary! { BatchInsert }

impl WithSchema for BatchInsert {
    fn schema(&self) -> &Schema {
        self.logical.schema()
    }
}

impl ToDistributedBatch for BatchInsert {
    fn to_distributed(&self) -> PlanRef {
        let new_input = self
            .input()
            .to_distributed_with_required(self.input_order_required(), Distribution::any());
        self.clone_with_input(new_input).into()
    }
}

impl ToBatchProst for BatchInsert {
    fn to_batch_prost_body(&self) -> NodeBody {
        #[allow(unreachable_code)]
        NodeBody::Insert(InsertNode {
            table_source_ref_id: todo!("fill source id here"),
            column_ids: todo!("this field is unused now"),
        })
    }
}

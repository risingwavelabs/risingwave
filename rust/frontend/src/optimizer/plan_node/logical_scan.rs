use std::fmt;

use risingwave_common::catalog::Schema;

use super::{ColPrunable, IntoPlanRef, PlanRef, ToBatch, ToStream};
use crate::catalog::column_catalog::ColumnCatalog;
use crate::catalog::TableId;
use crate::optimizer::plan_node::BatchSeqScan;
use crate::optimizer::property::{WithDistribution, WithOrder, WithSchema};

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct LogicalScan {
    table_id: TableId,
    columns: Vec<ColumnCatalog>,
    schema: Schema,
}
impl WithSchema for LogicalScan {
    fn schema(&self) -> &Schema {
        &self.schema
    }
}
impl_plan_tree_node_for_leaf! {LogicalScan}
impl WithOrder for LogicalScan {}
impl WithDistribution for LogicalScan {}

impl fmt::Display for LogicalScan {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        todo!()
    }
}
impl ColPrunable for LogicalScan {}
impl ToBatch for LogicalScan {
    fn to_batch(&self) -> PlanRef {
        BatchSeqScan::new(self.clone()).into_plan_ref()
    }
}
impl ToStream for LogicalScan {
    fn to_stream(&self) -> PlanRef {
        todo!()
    }
}

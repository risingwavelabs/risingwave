use std::fmt;

use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;

use super::{ColPrunable, IntoPlanRef, PlanRef, ToBatch, ToStream};
use crate::catalog::{ColumnId, TableId};
use crate::optimizer::plan_node::BatchSeqScan;
use crate::optimizer::property::{WithDistribution, WithOrder, WithSchema};

#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub struct LogicalScan {
    table_id: TableId,
    columns: Vec<ColumnId>,
    schema: Schema,
}

impl LogicalScan {
    /// Create a LogicalScan node. Used internally by optimizer.
    pub fn new(table_id: TableId, columns: Vec<ColumnId>, schema: Schema) -> Self {
        Self {
            table_id,
            columns,
            schema,
        }
    }

    /// Create a LogicalScan node. Used by planner.
    pub fn create(table_id: TableId, columns: Vec<ColumnId>, schema: Schema) -> Result<Self> {
        Ok(Self::new(table_id, columns, schema))
    }
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
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let columns = self
            .schema
            .fields()
            .iter()
            .map(|f| f.name.clone())
            .collect::<Vec<_>>();
        f.debug_struct("LogicalScan")
            .field("table", &"TODO".to_string())
            .field("columns", &columns)
            .finish()
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

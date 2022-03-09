use std::fmt;

use fixedbitset::FixedBitSet;
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;

use super::{ColPrunable, PlanRef, ToBatch, ToStream};
use crate::catalog::{ColumnId, TableId};
use crate::optimizer::plan_node::BatchSeqScan;
use crate::optimizer::property::{WithDistribution, WithOrder, WithSchema};

/// `LogicalScan` returns contents of a table or other equivalent object
#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub struct LogicalScan {
    table_name: String,
    table_id: TableId,
    columns: Vec<ColumnId>,
    schema: Schema,
}

impl LogicalScan {
    /// Create a LogicalScan node. Used internally by optimizer.
    pub fn new(
        table_name: String,
        table_id: TableId,
        columns: Vec<ColumnId>,
        schema: Schema,
    ) -> Self {
        Self {
            table_name,
            table_id,
            columns,
            schema,
        }
    }

    /// Create a LogicalScan node. Used by planner.
    pub fn create(
        table_name: String,
        table_id: TableId,
        columns: Vec<ColumnId>,
        schema: Schema,
    ) -> Result<PlanRef> {
        Ok(Self::new(table_name, table_id, columns, schema).into())
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
            .field("table", &self.table_name)
            .field("columns", &columns)
            .finish()
    }
}

impl ColPrunable for LogicalScan {
    fn prune_col(&self, required_cols: &FixedBitSet) -> PlanRef {
        self.must_contain_columns(required_cols);

        let (columns, fields) = required_cols
            .ones()
            .map(|id| (self.columns[id], self.schema.fields[id].clone()))
            .unzip();
        Self {
            table_name: self.table_name.clone(),
            table_id: self.table_id,
            columns,
            schema: Schema { fields },
        }
        .into()
    }
}

impl ToBatch for LogicalScan {
    fn to_batch(&self) -> PlanRef {
        BatchSeqScan::new(self.clone()).into()
    }
}

impl ToStream for LogicalScan {
    fn to_stream(&self) -> PlanRef {
        todo!()
    }
}

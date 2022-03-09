use std::fmt;

use fixedbitset::FixedBitSet;
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;

use super::{ColPrunable, LogicalBase, PlanRef, ToBatch, ToStream};
use crate::catalog::{ColumnId, TableId};
use crate::optimizer::plan_node::BatchSeqScan;
use crate::optimizer::property::WithSchema;

/// `LogicalScan` returns contents of a table or other equivalent object
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct LogicalScan {
    pub base: LogicalBase,
    table_name: String,
    table_id: TableId,
    columns: Vec<ColumnId>,
}

impl LogicalScan {
    /// Create a LogicalScan node. Used internally by optimizer.
    pub fn new(
        table_name: String,
        table_id: TableId,
        columns: Vec<ColumnId>,
        schema: Schema,
    ) -> Self {
        let base = LogicalBase { schema };
        Self {
            table_name,
            table_id,
            columns,
            base,
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

    pub(super) fn fmt_fields(&self, f: &mut fmt::DebugStruct) {
        let columns = self
            .schema
            .fields()
            .iter()
            .map(|f| f.name.clone())
            .collect::<Vec<_>>();
        f.field("table", &self.table_name)
            .field("columns", &columns);
    }
}

impl_plan_tree_node_for_leaf! {LogicalScan}

impl fmt::Display for LogicalScan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut s = f.debug_struct("LogicalScan");
        self.fmt_fields(&mut s);
        s.finish()
    }
}

impl ColPrunable for LogicalScan {
    fn prune_col(&self, required_cols: &FixedBitSet) -> PlanRef {
        self.must_contain_columns(required_cols);

        let (columns, fields) = required_cols
            .ones()
            .map(|id| (self.columns[id], self.schema().fields[id].clone()))
            .unzip();

        Self::new(
            self.table_name.clone(),
            self.table_id,
            columns,
            Schema { fields },
        )
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

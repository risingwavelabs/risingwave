use std::fmt;

use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::Result;
use risingwave_common::types::DataType;

use super::{ColPrunable, LogicalBase, PlanRef, PlanTreeNodeUnary, ToBatch, ToStream};
use crate::binder::BaseTableRef;
use crate::catalog::ColumnId;
use crate::optimizer::property::{WithDistribution, WithOrder};

/// `LogicalInsert` iterates on input relation and insert the data into specified table.
///
/// It corresponds to the `INSERT` statements in SQL. Especially, for `INSERT ... VALUES`
/// statements, the input relation would be [`super::LogicalValues`].
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct LogicalInsert {
    pub base: LogicalBase,
    table: BaseTableRef,
    columns: Vec<ColumnId>,
    input: PlanRef,
}

impl LogicalInsert {
    /// Create a LogicalInsert node. Used internally by optimizer.
    pub fn new(input: PlanRef, table: BaseTableRef, columns: Vec<ColumnId>) -> Self {
        let schema = Schema::new(vec![Field::unnamed(DataType::Int64)]);
        let base = LogicalBase { schema };
        Self {
            table,
            columns,
            input,
            base,
        }
    }

    /// Create a LogicalInsert node. Used by planner.
    pub fn create(input: PlanRef, table: BaseTableRef, columns: Vec<ColumnId>) -> Result<Self> {
        Ok(Self::new(input, table, columns))
    }
}

impl PlanTreeNodeUnary for LogicalInsert {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }
    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.table.clone(), self.columns.clone())
    }
}
impl_plan_tree_node_for_unary! {LogicalInsert}

impl fmt::Display for LogicalInsert {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "LogicalInsert {{ table_name: {}, columns: {:?} }}",
            self.table.name, self.columns,
        )
    }
}

impl ColPrunable for LogicalInsert {
    fn prune_col(&self, _required_cols: &fixedbitset::FixedBitSet) -> PlanRef {
        panic!("column pruning should not be called on insert")
    }
}

impl ToBatch for LogicalInsert {
    fn to_batch(&self) -> PlanRef {
        todo!()
    }
}

impl ToStream for LogicalInsert {
    fn to_stream(&self) -> PlanRef {
        todo!()
    }
}

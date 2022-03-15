use std::fmt;

use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::Result;
use risingwave_common::types::DataType;

use super::{BatchInsert, ColPrunable, LogicalBase, PlanRef, PlanTreeNodeUnary, ToBatch, ToStream};
use crate::binder::BaseTableRef;
use crate::catalog::ColumnId;

/// `LogicalInsert` iterates on input relation and insert the data into specified table.
///
/// It corresponds to the `INSERT` statements in SQL. Especially, for `INSERT ... VALUES`
/// statements, the input relation would be [`super::LogicalValues`].
#[derive(Debug, Clone)]
pub struct LogicalInsert {
    pub base: LogicalBase,
    table: BaseTableRef,
    columns: Vec<ColumnId>,
    input: PlanRef,
}

impl LogicalInsert {
    /// Create a [`LogicalInsert`] node. Used internally by optimizer.
    pub fn new(input: PlanRef, table: BaseTableRef, columns: Vec<ColumnId>) -> Self {
        let ctx = input.ctx();
        let schema = Schema::new(vec![Field::unnamed(DataType::Int64)]);
        let id = ctx.borrow_mut().get_id();
        let base = LogicalBase { id, schema, ctx };

        Self {
            table,
            columns,
            input,
            base,
        }
    }

    /// Create a [`LogicalInsert`] node. Used by planner.
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
    fn prune_col(&self, required_cols: &fixedbitset::FixedBitSet) -> PlanRef {
        // TODO: special handling for insert column pruning
        self.clone_with_input(self.input.prune_col(required_cols))
            .into()
    }
}

impl ToBatch for LogicalInsert {
    fn to_batch(&self) -> PlanRef {
        let new_input = self.input().to_batch();
        let new_logical = self.clone_with_input(new_input);
        BatchInsert::new(new_logical).into()
    }
}

impl ToStream for LogicalInsert {
    fn to_stream(&self) -> PlanRef {
        unreachable!("insert should always be converted to batch plan");
    }
}

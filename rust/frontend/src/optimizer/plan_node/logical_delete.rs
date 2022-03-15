use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::Result;
use risingwave_common::types::DataType;

use super::{ColPrunable, LogicalBase, PlanRef, PlanTreeNodeUnary, ToBatch, ToStream};
use crate::binder::BaseTableRef;

/// [`LogicalDelete`] iterates on input relation and delete the data from specified table.
///
/// It corresponds to the `DELETE` statements in SQL.
#[derive(Debug, Clone)]
pub struct LogicalDelete {
    pub base: LogicalBase,
    table: BaseTableRef,
    input: PlanRef,
}

impl LogicalDelete {
    /// Create a [`LogicalDelete`] node. Used internally by optimizer.
    pub fn new(input: PlanRef, table: BaseTableRef) -> Self {
        let ctx = input.ctx();
        let schema = Schema::new(vec![Field::unnamed(DataType::Int64)]);
        let id = ctx.borrow_mut().get_id();
        let base = LogicalBase { id, schema, ctx };

        Self { base, table, input }
    }

    /// Create a [`LogicalDelete`] node. Used by planner.
    pub fn create(input: PlanRef, table: BaseTableRef) -> Result<Self> {
        Ok(Self::new(input, table))
    }
}

impl PlanTreeNodeUnary for LogicalDelete {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.table.clone())
    }
}

impl_plan_tree_node_for_unary! { LogicalDelete }

impl std::fmt::Display for LogicalDelete {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LogicalDelete {{ table_name: {} }}", self.table.name)
    }
}

impl ColPrunable for LogicalDelete {
    fn prune_col(&self, required_cols: &fixedbitset::FixedBitSet) -> PlanRef {
        // TODO: do we need to do column pruning for deletion?
        self.clone_with_input(self.input.prune_col(required_cols))
            .into()
    }
}

impl ToBatch for LogicalDelete {
    fn to_batch(&self) -> PlanRef {
        todo!()
    }
}

impl ToStream for LogicalDelete {
    fn to_stream(&self) -> PlanRef {
        unreachable!("delete should always be converted to batch plan");
    }
}

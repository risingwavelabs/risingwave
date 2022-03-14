use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::Result;
use risingwave_common::types::DataType;

use super::{LogicalBase, PlanRef, PlanTreeNodeUnary};
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

// impl_plan_tree_node_for_unary! { LogicalDelete }

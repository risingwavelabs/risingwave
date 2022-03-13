use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{ErrorCode, Result};

use crate::binder::{BaseTableRef, TableRef};
use crate::optimizer::plan_node::{LogicalScan, PlanRef};
use crate::planner::Planner;

impl Planner {
    pub(super) fn plan_table_ref(&mut self, table_ref: TableRef) -> Result<PlanRef> {
        match table_ref {
            TableRef::BaseTable(t) => self.plan_base_table_ref(*t),
            TableRef::Join(j) => Err(ErrorCode::NotImplementedError(format!(
                "join plan not implemented, {:?}",
                j
            ))
            .into()),
        }
    }

    pub(super) fn plan_base_table_ref(&mut self, table_ref: BaseTableRef) -> Result<PlanRef> {
        let (column_ids, fields) = table_ref
            .columns
            .iter()
            .map(|c| (c.id(), Field::with_name(c.data_type(), c.name())))
            .unzip();
        let schema = Schema::new(fields);
        LogicalScan::create(
            table_ref.name,
            table_ref.table_id,
            column_ids,
            schema,
            self.ctx(),
        )
    }
}

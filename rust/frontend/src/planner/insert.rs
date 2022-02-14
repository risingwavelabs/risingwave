use risingwave_common::error::Result;

use crate::binder::BoundInsert;
use crate::optimizer::plan_node::{IntoPlanRef as _, LogicalInsert, PlanRef};
use crate::planner::Planner;

impl Planner {
    pub(super) fn plan_insert(&mut self, insert: BoundInsert) -> Result<PlanRef> {
        let input = self.plan_query(*insert.source)?;
        // `columns` not used by backend yet.
        Ok(LogicalInsert::create(input, insert.table.table_id, vec![])?.into_plan_ref())
    }
}

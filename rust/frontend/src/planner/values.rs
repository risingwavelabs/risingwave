use risingwave_common::error::Result;

use crate::binder::BoundValues;
use crate::optimizer::plan_node::{IntoPlanRef as _, LogicalValues, PlanRef};
use crate::planner::Planner;

impl Planner {
    pub(super) fn plan_values(&mut self, values: BoundValues) -> Result<PlanRef> {
        Ok(LogicalValues::create(values.rows, values.schema)?.into_plan_ref())
    }
}

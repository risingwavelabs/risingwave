use risingwave_common::error::Result;

use super::Planner;
use crate::binder::BoundDelete;
use crate::optimizer::PlanRoot;

impl Planner {
    pub(super) fn plan_delete(&mut self, _delete: BoundDelete) -> Result<PlanRoot> {
        todo!()
    }
}

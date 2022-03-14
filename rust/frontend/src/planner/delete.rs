use risingwave_common::error::Result;

use super::Planner;
use crate::binder::BoundDelete;
use crate::optimizer::plan_node::{LogicalDelete, LogicalFilter};
use crate::optimizer::PlanRoot;

impl Planner {
    pub(super) fn plan_delete(&mut self, delete: BoundDelete) -> Result<PlanRoot> {
        let scan = self.plan_base_table_ref(delete.table.clone())?;
        let input = if let Some(expr) = delete.selection {
            LogicalFilter::create(scan, expr)?.into()
        } else {
            scan
        };
        let plan = LogicalDelete::new(input, delete.table);

        todo!()
    }
}

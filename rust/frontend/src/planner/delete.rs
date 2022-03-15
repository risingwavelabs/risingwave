use fixedbitset::FixedBitSet;
use risingwave_common::error::Result;

use super::Planner;
use crate::binder::BoundDelete;
use crate::optimizer::plan_node::{LogicalDelete, LogicalFilter};
use crate::optimizer::property::{Distribution, Order};
use crate::optimizer::{PlanRef, PlanRoot};

impl Planner {
    pub(super) fn plan_delete(&mut self, delete: BoundDelete) -> Result<PlanRoot> {
        let scan = self.plan_base_table_ref(delete.table.clone())?;
        let input = if let Some(expr) = delete.selection {
            LogicalFilter::create(scan, expr)?.into()
        } else {
            scan
        };
        let plan: PlanRef = LogicalDelete::create(input, delete.table)?.into();

        let order = Order::any().clone();
        let dist = Distribution::any().clone();
        let mut out_fields = FixedBitSet::with_capacity(plan.schema().len());
        out_fields.insert_range(..);

        let root = PlanRoot::new(plan, dist, order, out_fields);
        Ok(root)
    }
}

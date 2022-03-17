use fixedbitset::FixedBitSet;
use risingwave_common::error::Result;

use crate::binder::BoundInsert;
use crate::optimizer::plan_node::{LogicalInsert, PlanRef};
use crate::optimizer::property::{Distribution, Order};
use crate::optimizer::PlanRoot;
use crate::planner::Planner;

impl Planner {
    pub(super) fn plan_insert(&mut self, insert: BoundInsert) -> Result<PlanRoot> {
        let input = self.plan_query(insert.source)?.as_subplan();
        // `columns` not used by backend yet.
        let plan: PlanRef = LogicalInsert::create(input, insert.table, vec![])?.into();
        let order = Order::any().clone();
        let dist = Distribution::Single;
        let mut out_fields = FixedBitSet::with_capacity(plan.schema().len());
        out_fields.insert_range(..);
        let root = PlanRoot::new(plan, dist, order, out_fields);
        Ok(root)
    }
}

use fixedbitset::FixedBitSet;
use risingwave_common::error::Result;

use crate::binder::BoundQuery;
use crate::optimizer::property::{Distribution, Order};
use crate::optimizer::PlanRoot;
use crate::planner::Planner;

impl Planner {
    pub(super) fn plan_query(&mut self, query: BoundQuery) -> Result<PlanRoot> {
        let plan = self.plan_set_expr(query.body)?;
        // plan order and limit here
        let order = Order {
            field_order: vec![],
        };
        let dist = Distribution::Any;
        let mut out_fields = FixedBitSet::with_capacity(plan.schema().len());
        out_fields.insert_range(..);
        let root = PlanRoot::new(plan, dist, order, out_fields);
        Ok(root)
    }
}

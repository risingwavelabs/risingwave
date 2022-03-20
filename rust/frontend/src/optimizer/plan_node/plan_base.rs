use risingwave_common::catalog::Schema;

use super::PlanNodeId;
use crate::optimizer::property::{Distribution, Order};
use crate::session::QueryContextRef;

/// the common fields of all nodes, please make a field named `base` in
/// every planNode and correctly valued it when construct the planNode.
#[derive(Clone, Debug)]
pub struct PlanBase {
    pub id: PlanNodeId,
    pub ctx: QueryContextRef,
    pub schema: Schema,
    /// the order property of the PlanNode's output, store an `Order::any()` here will not affect
    /// correctness, but insert unnecessary sort in plan
    pub order: Order,
    /// the distribution property of the PlanNode's output, store an `Distribution::any()` here
    /// will not affect correctness, but insert unnecessary exchange in plan
    pub dist: Distribution,
}
impl PlanBase {
    pub fn new_logical(ctx: QueryContextRef, schema: Schema) -> Self {
        let id = ctx.borrow_mut().get_id();
        Self {
            id,
            ctx,
            schema,
            dist: Distribution::any().clone(),
            order: Order::any().clone(),
        }
    }
    pub fn new_stream(ctx: QueryContextRef, schema: Schema, dist: Distribution) -> Self {
        let id = ctx.borrow_mut().get_id();
        Self {
            id,
            ctx,
            schema,
            dist,
            order: Order::any().clone(),
        }
    }
    pub fn new_batch(
        ctx: QueryContextRef,
        schema: Schema,
        dist: Distribution,
        order: Order,
    ) -> Self {
        let id = ctx.borrow_mut().get_id();
        Self {
            id,
            ctx,
            schema,
            dist,
            order,
        }
    }
}

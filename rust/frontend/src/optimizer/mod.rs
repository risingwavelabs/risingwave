#![allow(dead_code)]
pub mod plan_node;
use fixedbitset::FixedBitSet;
pub use plan_node::PlanRef;
pub mod plan_pass;
pub mod property;
pub mod rule;

use property::{Distribution, Order};
use risingwave_common::catalog::Schema;

/// `PlanRoot` is used to describe a plan. planner will construct a `PlanRoot` with LogicalNode and
/// required distribution and order. And `PlanRoot` can generate corresponding streaming or batch
/// Plan with optimization. the required Order and Distribution columns might be more than the
/// output columns. for example:
/// ```SQL
///    select v1 from t order by id;
/// ```
/// the plan will return two columns (id, v1), and the required order column is id. the id
/// column is required in optimization, but the final generated plan will remove the unnecessary
/// column in the result.
#[derive(Debug, Clone)]
pub struct PlanRoot {
    logical_plan: PlanRef,
    required_dist: Distribution,
    required_order: Order,
    out_fields: FixedBitSet,
    schema: Schema,
}

impl PlanRoot {
    pub fn new(
        plan: PlanRef,
        required_dist: Distribution,
        required_order: Order,
        out_fields: FixedBitSet,
    ) -> Self {
        let input_schema = plan.schema();
        assert_eq!(input_schema.fields().len(), out_fields.len());

        let schema = Schema {
            fields: out_fields
                .ones()
                .map(|i| input_schema.fields()[i].clone())
                .collect(),
        };
        Self {
            logical_plan: plan,
            required_dist,
            required_order,
            out_fields,
            schema,
        }
    }

    /// Get a reference to the plan root's schema.
    fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn into_logical(self) -> PlanRef {
        self.logical_plan
    }

    /// optimize and generate a batch query plan
    pub fn gen_batch_query_plan(&self) -> PlanRef {
        // TODO: add a `HeuristicOptimizer` here
        let mut plan = self.logical_plan.prune_col(&self.out_fields);
        plan = plan.to_batch_with_order_required(&self.required_order);
        plan = plan.to_distributed_with_required(&self.required_order, &self.required_dist);
        // FIXME: add a Batch Project for the Plan, to remove the unnecessary column in the
        // result.
        // TODO: do a final column pruning after add the batch project, but now the column
        // pruning is not used in batch node, need to think.

        plan
    }

    /// optimize and generate a create materialize view plan
    pub fn gen_create_mv_plan(&self) -> PlanRef {
        // TODO: add a `HeuristicOptimizer` here
        let mut plan = self.logical_plan.prune_col(&self.out_fields);
        plan = plan.to_stream_with_dist_required(&self.required_dist);
        // FIXME: add `Materialize` operator on the top of plan
        // FIXME: add a Streaming Project for the Plan to remove the unnecessary column in the
        // result.
        // TODO: do a final column pruning after add the streaming project, but now
        // the column pruning is not used in streaming node, need to think.
        plan
    }
}

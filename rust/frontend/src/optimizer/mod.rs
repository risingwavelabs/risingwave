#![allow(dead_code)]
pub mod plan_node;
pub use plan_node::PlanRef;
use risingwave_common::catalog::Schema;
use risingwave_pb::plan::exchange_info::Distribution;

use self::property::Order;
pub mod plan_pass;
pub mod property;
pub mod rule;

#[derive(Debug, Clone)]
/// used to describe a plan, its required
/// and planner will construct a `PlanRoot` with LogicalNode and required Order. And `PlanRoot` can
/// generate corresponding streaming or batch Plan with optimization.
/// the required Order and Distribution columns might be more than the output columns. for example:
/// ```SQL
///    select v1 from t order by id
/// ```
/// the plan will return two columns `(id, v1)`, and the required order column is `id`. the id
/// column is required in optimization, but the final generated plan will remove the unnecessary
/// column in the result.
struct PlanRoot {
    logical_plan: PlanRef,
    required_dist: Distribution,
    required_order: Order,
    out_fields: Vec<usize>,
    schema: Schema,
}

impl PlanRoot {
    pub fn new(
        plan: PlanRef,
        required_dist: Distribution,
        required_order: Order,
        out_fields: Vec<usize>,
    ) -> Self {
        let input_schema = plan.schema();
        let schema = Schema {
            fields: out_fields
                .iter()
                .map(|i| input_schema.fields()[*i].clone())
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

    pub fn with_required_dist(self, dist: Distribution) -> Self {
        Self::new(
            self.logical_plan,
            dist,
            self.required_order,
            self.out_fields,
        )
    }

    pub fn with_required_order(self, order: Order) -> Self {
        Self::new(
            self.logical_plan,
            self.required_dist,
            order,
            self.out_fields,
        )
    }

    pub fn with_plan(self, plan: PlanRef) -> Self {
        Self::new(
            plan,
            self.required_dist,
            self.required_order,
            self.out_fields,
        )
    }

    pub fn with_out_fields(self, out_fields: Vec<usize>) -> Self {
        Self::new(
            self.logical_plan,
            self.required_dist,
            self.required_order,
            out_fields,
        )
    }

    /// Get a reference to the plan root's schema.
    fn schema(&self) -> &Schema {
        &self.schema
    }

    /// optimize and generate a batch query plan
    pub fn gen_batch_query_plan() -> PlanRef {
        todo!()
    }

    /// optimize and generate a create materialize view plan
    pub fn gen_create_mv_plan() -> PlanRef {
        todo!()
    }
}

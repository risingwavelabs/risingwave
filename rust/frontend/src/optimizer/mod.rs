#![allow(dead_code)]
pub mod plan_node;
use fixedbitset::FixedBitSet;
use itertools::Itertools as _;
pub use plan_node::PlanRef;
pub mod plan_pass;
pub mod property;
pub mod rule;

use property::{Distribution, Order};
use risingwave_common::catalog::Schema;

use self::plan_node::LogicalProject;
use crate::expr::InputRef;

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

    /// Transform the PlanRoot back to a PlanRef suitable to be used as a subplan, for example as
    /// insert source or subquery. This ignores Order but retains post-Order pruning (`out_fields`).
    pub fn as_subplan(self) -> PlanRef {
        if self.out_fields.count_ones(..) == self.out_fields.len() {
            return self.logical_plan;
        }
        let (exprs, expr_aliases) = self
            .out_fields
            .ones()
            .zip_eq(self.schema.fields)
            .map(|(index, field)| {
                (
                    InputRef::new(index, field.data_type).into(),
                    Some(field.name),
                )
            })
            .unzip();
        LogicalProject::create(self.logical_plan, exprs, expr_aliases)
    }

    /// optimize and generate a batch query plan
    pub fn gen_batch_query_plan(&self) -> PlanRef {
        // TODO: add a `HeuristicOptimizer` here
        let mut plan = self.logical_plan.prune_col(&self.out_fields);
        plan = plan.to_batch_with_order_required(&self.required_order);
        // TODO: plan.to_distributed_with_required()
        // FIXME: add a Batch Project for the Plan, to remove the unnecessary column in the result.
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

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::Field;
    use risingwave_common::types::DataType;

    use super::*;
    use crate::catalog::{ColumnId, TableId};
    use crate::optimizer::plan_node::LogicalScan;

    #[test]
    fn test_as_subplan() {
        let scan = LogicalScan::create(
            "test_table".into(),
            TableId::new(3),
            vec![ColumnId::new(2), ColumnId::new(7)],
            Schema::new(vec![
                Field::with_name(DataType::Int32, "v1".into()),
                Field::with_name(DataType::Varchar, "v2".into()),
            ]),
        )
        .unwrap();
        let out_fields = FixedBitSet::with_capacity_and_blocks(2, [1]);
        let root = PlanRoot::new(
            scan,
            Distribution::any().clone(),
            Order::any().clone(),
            out_fields,
        );
        let subplan = root.as_subplan();
        assert_eq!(
            subplan.schema(),
            &Schema::new(vec![Field::with_name(DataType::Int32, "v1".into()),])
        );
    }
}

// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod plan_node;
pub use plan_node::PlanRef;
pub mod property;

mod heuristic;
mod plan_rewriter;
mod plan_visitor;
mod rule;

use fixedbitset::FixedBitSet;
use itertools::Itertools as _;
use property::{Distribution, Order};
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;

use self::heuristic::{ApplyOrder, HeuristicOptimizer};
use self::plan_node::{Convention, LogicalProject, StreamMaterialize};
use self::rule::*;
use crate::expr::InputRef;

/// `PlanRoot` is used to describe a plan. planner will construct a `PlanRoot` with `LogicalNode`.
/// and required distribution and order. And `PlanRoot` can generate corresponding streaming or
/// batch plan with optimization. the required Order and Distribution columns might be more than the
/// output columns. for example:
/// ```sql
///    select v1 from t order by id;
/// ```
/// the plan will return two columns (id, v1), and the required order column is id. the id
/// column is required in optimization, but the final generated plan will remove the unnecessary
/// column in the result.
#[derive(Debug, Clone)]
pub struct PlanRoot {
    plan: PlanRef,
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
            plan,
            required_dist,
            required_order,
            out_fields,
            schema,
        }
    }

    /// Change the distribution of [`PlanRoot`].
    pub fn with_distribution(&self, dist: Distribution) -> Self {
        Self {
            required_dist: dist,
            ..self.clone()
        }
    }

    /// Get a reference to the plan root's schema.
    #[allow(dead_code)]
    fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Transform the [`PlanRoot`] back to a [`PlanRef`] suitable to be used as a subplan, for
    /// example as insert source or subquery. This ignores Order but retains post-Order pruning
    /// (`out_fields`).
    pub fn as_subplan(self) -> PlanRef {
        if self.out_fields.count_ones(..) == self.out_fields.len() {
            return self.plan;
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
        LogicalProject::create(self.plan, exprs, expr_aliases)
    }

    /// Apply logical optimization to the plan.
    pub fn gen_optimized_logical_plan(&self) -> PlanRef {
        let mut plan = self.plan.clone();

        // Subquery Unnesting
        plan = {
            let rules = vec![
                // Apply Push-down
                ApplyProjectRule::create(),
                ApplyFilterRule::create(),
                // Apply Eliminate
                ApplyScanRule::create(),
            ];
            let heuristic_optimizer =
                HeuristicOptimizer::new(ApplyOrder::BottomUpAndThenTopDown, rules);
            heuristic_optimizer.optimize(plan)
        };

        // Predicate Push-down
        plan = {
            let rules = vec![
                FilterJoinRule::create(),
                FilterProjectRule::create(),
                FilterAggRule::create(),
            ];
            let heuristic_optimizer = HeuristicOptimizer::new(ApplyOrder::TopDown, rules);
            heuristic_optimizer.optimize(plan)
        };

        // Prune Columns
        plan = plan.prune_col(&self.out_fields);

        plan = {
            let rules = vec![
                ProjectMergeRule::create(), // merge should be applied before eliminate
                ProjectEliminateRule::create(),
            ];
            let heuristic_optimizer = HeuristicOptimizer::new(ApplyOrder::BottomUp, rules);
            heuristic_optimizer.optimize(plan)
        };

        plan
    }

    /// optimize and generate a batch query plan
    pub fn gen_batch_query_plan(&self) -> PlanRef {
        let mut plan = self.gen_optimized_logical_plan();

        // Convert to physical plan node
        plan = plan.to_batch_with_order_required(&self.required_order);

        // TODO: Enable this when distributed e2e is OK.
        // plan = plan.to_distributed_with_required(&self.required_order, &self.required_dist);
        // FIXME: add a Batch Project for the Plan, to remove the unnecessary column in the result.
        // TODO: do a final column pruning after add the batch project, but now the column
        // pruning is not used in batch node, need to think.

        plan
    }

    /// Optimize and generate a batch query plan.
    /// Currently only used by test runner (Have distributed plan but not schedule yet).
    /// Will be removed after dist execution.
    pub fn gen_dist_batch_query_plan(&self) -> PlanRef {
        let plan = self.gen_batch_query_plan();

        plan.to_distributed_with_required(&self.required_order, &self.required_dist)
    }

    /// Optimize and generate a create materialize view plan.
    ///
    /// The `MaterializeExecutor` won't be generated at this stage, and will be attached in
    /// `gen_create_mv_plan`.
    pub fn gen_create_mv_plan(&mut self, mv_name: String) -> Result<StreamMaterialize> {
        let stream_plan = match self.plan.convention() {
            Convention::Logical => {
                let plan = self.gen_optimized_logical_plan();
                let (plan, out_col_change) = plan.logical_rewrite_for_stream();
                self.required_dist = out_col_change
                    .rewrite_required_distribution(&self.required_dist)
                    .unwrap();
                self.required_order = out_col_change
                    .rewrite_required_order(&self.required_order)
                    .unwrap();
                self.out_fields = out_col_change.rewrite_bitset(&self.out_fields);
                self.schema = plan.schema().clone();
                plan.to_stream_with_dist_required(&self.required_dist)
            }
            Convention::Stream => self
                .required_dist
                .enforce_if_not_satisfies(self.plan.clone(), Order::any()),
            _ => panic!(),
        };

        // Ignore the required_dist and required_order, as they are provided by user now.
        // TODO: need more thinking and refactor.

        // Convert to physical plan node, using distribution of the input node
        // After that, we will need to wrap a `MaterializeExecutor` on it in `gen_create_mv_plan`.

        StreamMaterialize::create(
            stream_plan,
            mv_name,
            self.required_order.clone(),
            self.out_fields.clone(),
        )
    }

    /// Set the plan root's required dist.
    pub fn set_required_dist(&mut self, required_dist: Distribution) {
        self.required_dist = required_dist;
    }
}

#[cfg(test)]
mod tests {

    use risingwave_common::catalog::Field;
    use risingwave_common::types::DataType;

    use super::*;
    use crate::optimizer::plan_node::LogicalValues;
    use crate::session::OptimizerContext;

    #[tokio::test]
    async fn test_as_subplan() {
        let ctx = OptimizerContext::mock().await;
        let values = LogicalValues::new(
            vec![],
            Schema::new(vec![
                Field::with_name(DataType::Int32, "v1"),
                Field::with_name(DataType::Varchar, "v2"),
            ]),
            ctx,
        )
        .into();
        let out_fields = FixedBitSet::with_capacity_and_blocks(2, [1]);
        let root = PlanRoot::new(
            values,
            Distribution::any().clone(),
            Order::any().clone(),
            out_fields,
        );
        let subplan = root.as_subplan();
        assert_eq!(
            subplan.schema(),
            &Schema::new(vec![Field::with_name(DataType::Int32, "v1"),])
        );
    }
}

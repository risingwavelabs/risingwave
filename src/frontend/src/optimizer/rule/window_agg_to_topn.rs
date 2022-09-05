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

use fixedbitset::FixedBitSet;
use risingwave_common::types::DataType;

use super::Rule;
use crate::expr::{ExprType, WindowFunctionType};
use crate::optimizer::plan_node::{
    LogicalFilter, LogicalProject, LogicalTopN, PlanTreeNodeUnary, PlanWindowFunction,
};
use crate::optimizer::property::{FieldOrder, Order};
use crate::planner::LIMIT_ALL_COUNT;
use crate::PlanRef;

/// Transforms the following pattern to group `TopN`
///
/// ```sql
/// SELECT .. from
///   (SELECT .., ROW_NUMBER() OVER(PARTITION BY .. ORDER BY ..) rank from ..)
/// WHERE rank < ..;
/// ```
pub struct WindowAggToTopNRule;

impl WindowAggToTopNRule {
    pub fn create() -> Box<dyn Rule> {
        Box::new(WindowAggToTopNRule)
    }
}

impl Rule for WindowAggToTopNRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let project_upper = plan.as_logical_project()?;
        let plan = project_upper.input();
        let filter = plan.as_logical_filter()?;
        let plan = filter.input();
        let project_lower = plan.as_logical_project()?;
        let plan = project_lower.input();
        let window_agg = plan.as_logical_window_agg()?;
        let input = window_agg.input();

        // The position of the window function in project_lower
        let window_agg_pos = {
            let window_agg_len = window_agg.schema().len();
            project_lower.exprs().iter().position(|e| {
                e.as_input_ref()
                    .map(|i| i.index() == window_agg_len - 1)
                    .unwrap_or(false)
            })?
        };

        if project_upper.exprs().iter().any(|expr| {
            expr.collect_input_refs(project_lower.schema().len())
                .contains(window_agg_pos)
        }) {
            // TopN with ranking output is not supported yet.
            return None;
        }

        let PlanWindowFunction {
            function_type,
            return_type: _,
            partition_by,
            order_by,
        } = &window_agg.window_function;
        assert_eq!(function_type, &WindowFunctionType::RowNumber);

        let (rank_pred, other_pred) = {
            let predicate = filter.predicate();
            let mut rank_col = FixedBitSet::with_capacity(project_lower.schema().len());
            rank_col.set(window_agg_pos, true);
            predicate.clone().split_disjoint(&rank_col)
        };

        // TODO: support multiple complex rank predicates. Currently only support rank<N
        let (limit, offset) = {
            if rank_pred.conjunctions.len() != 1 {
                return None;
            }
            let (input_ref, cmp, v) = rank_pred.conjunctions[0].as_comparison_const()?;
            assert_eq!(input_ref.index, window_agg_pos);
            let v = v
                .cast_implicit(DataType::Int64)
                .ok()?
                .eval_row_const()
                .ok()??;
            let v = *v.as_int64();
            // Note: rank functions start from 1
            match cmp {
                ExprType::LessThanOrEqual => (v.max(0) as usize, 0),
                ExprType::LessThan => ((v - 1).max(0) as usize, 0),
                ExprType::GreaterThan => (LIMIT_ALL_COUNT, v.max(0) as usize),
                ExprType::GreaterThanOrEqual => (LIMIT_ALL_COUNT, (v - 1).max(0) as usize),
                _ => unreachable!(),
            }
        };

        let topn = LogicalTopN::with_group(
            input,
            limit,
            offset,
            Order {
                field_order: order_by
                    .iter()
                    .map(|f| FieldOrder {
                        index: f.input.index,
                        direct: f.direction,
                    })
                    .collect(),
            },
            partition_by.iter().map(|i| i.index).collect(),
        )
        .into();
        let project_lower = {
            let mut project_lower_exprs = project_lower.exprs().clone();
            project_lower_exprs.pop();
            LogicalProject::create(topn, project_lower_exprs)
        };
        let filter = LogicalFilter::create(project_lower, other_pred);
        Some(project_upper.clone_with_input(filter).into())
    }
}

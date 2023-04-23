// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use fixedbitset::FixedBitSet;
use risingwave_common::types::DataType;

use super::Rule;
use crate::expr::{ExprImpl, ExprType, WindowFunctionType};
use crate::optimizer::plan_node::{LogicalFilter, LogicalTopN, PlanTreeNodeUnary};
use crate::optimizer::property::Order;
use crate::planner::LIMIT_ALL_COUNT;
use crate::PlanRef;

/// Transforms the following pattern to group `TopN`
///
/// ```sql
/// SELECT .. from
///   (SELECT .., ROW_NUMBER() OVER(PARTITION BY .. ORDER BY ..) rank from ..)
/// WHERE rank [ < | <= | > | >= | = ] ..;
/// ```
pub struct OverAggToTopNRule;

impl OverAggToTopNRule {
    pub fn create() -> Box<dyn Rule> {
        Box::new(OverAggToTopNRule)
    }
}

impl Rule for OverAggToTopNRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let project = plan.as_logical_project()?;
        let plan = project.input();
        let filter = plan.as_logical_filter()?;
        let plan = filter.input();
        // The filter is directly on top of the over agg after predicate pushdown.
        let over_agg = plan.as_logical_over_agg()?;

        if over_agg.window_functions.len() != 1 {
            // Queries with multiple window function calls are not supported yet.
            return None;
        }

        let f = &over_agg.window_functions[0];
        if !f.function_type.is_rank_function() {
            return None;
        }

        let over_agg_len = over_agg.schema().len();
        let window_func_pos = over_agg_len - 1;

        if project.exprs().iter().any(|expr| {
            expr.collect_input_refs(over_agg_len)
                .contains(window_func_pos)
        }) {
            // TopN with ranking output is not supported yet.
            tracing::error!("TopN with ranking output is not supported yet.");
            return None;
        }

        let with_ties = match f.function_type {
            // Only `ROW_NUMBER` and `RANK` can be optimized to TopN now.
            WindowFunctionType::RowNumber => false,
            WindowFunctionType::Rank => true,
            WindowFunctionType::DenseRank => unimplemented!("should be banned in planner"),
            _ => unreachable!("window functions other than rank functions should not reach here"),
        };

        let (rank_pred, other_pred) = {
            let predicate = filter.predicate();
            let mut rank_col = FixedBitSet::with_capacity(over_agg_len);
            rank_col.set(window_func_pos, true);
            predicate.clone().split_disjoint(&rank_col)
        };

        let (limit, offset) = handle_rank_preds(&rank_pred.conjunctions, window_func_pos)?;

        if offset > 0 && with_ties {
            tracing::error!("Failed to optimize with ties and offset");
            return None;
        }

        let topn = LogicalTopN::with_group(
            over_agg.input(),
            limit,
            offset,
            with_ties,
            Order {
                column_orders: f.order_by.to_vec(),
            },
            f.partition_by.iter().map(|i| i.index).collect(),
        )
        .into();
        let filter = LogicalFilter::create(topn, other_pred);
        Some(project.clone_with_input(filter).into())
    }
}

/// Returns `None` if the conditions are too complex or invalid. `Some((limit, offset))` otherwise.
fn handle_rank_preds(rank_preds: &[ExprImpl], window_func_pos: usize) -> Option<(u64, u64)> {
    // rank >= lb
    let mut lb: Option<i64> = None;
    // rank <= ub
    let mut ub: Option<i64> = None;
    // rank == eq
    let mut eq: Option<i64> = None;

    for cond in rank_preds {
        if let Some((input_ref, cmp, v)) = cond.as_comparison_const() {
            assert_eq!(input_ref.index, window_func_pos);
            let v = v
                .cast_implicit(DataType::Int64)
                .ok()?
                .eval_row_const()
                .ok()??;
            let v = *v.as_int64();
            match cmp {
                ExprType::LessThanOrEqual => ub = ub.map_or(Some(v), |ub| Some(ub.min(v))),
                ExprType::LessThan => ub = ub.map_or(Some(v - 1), |ub| Some(ub.min(v - 1))),
                ExprType::GreaterThan => lb = lb.map_or(Some(v + 1), |lb| Some(lb.max(v + 1))),
                ExprType::GreaterThanOrEqual => lb = lb.map_or(Some(v), |lb| Some(lb.max(v))),
                _ => unreachable!(),
            }
        } else if let Some((input_ref, v)) = cond.as_eq_const() {
            assert_eq!(input_ref.index, window_func_pos);
            let v = v
                .cast_implicit(DataType::Int64)
                .ok()?
                .eval_row_const()
                .ok()??;
            let v = *v.as_int64();
            if let Some(eq) = eq && eq != v {
                tracing::error!(
                    "Failed to optimize rank predicate with conflicting equal conditions."
                );
                return None;
            }
            eq = Some(v)
        } else {
            // TODO: support between and in
            tracing::error!("Failed to optimize complex rank predicate {:?}", cond);
            return None;
        }
    }

    // Note: rank functions start from 1
    if let Some(eq) = eq {
        if eq < 1 {
            tracing::error!(
                "Failed to optimize rank predicate with invalid predicate rank={}.",
                eq
            );
            return None;
        }
        let lb = lb.unwrap_or(i64::MIN);
        let ub = ub.unwrap_or(i64::MAX);
        if !(lb <= eq && eq <= ub) {
            tracing::error!("Failed to optimize rank predicate with conflicting bounds.");
            return None;
        }
        Some((1, (eq - 1) as u64))
    } else {
        match (lb, ub) {
            (Some(lb), Some(ub)) => Some(((ub - lb + 1).max(0) as u64, (lb - 1).max(0) as u64)),
            (Some(lb), None) => Some((LIMIT_ALL_COUNT, (lb - 1).max(0) as u64)),
            (None, Some(ub)) => Some((ub.max(0) as u64, 0)),
            (None, None) => unreachable!(),
        }
    }
}

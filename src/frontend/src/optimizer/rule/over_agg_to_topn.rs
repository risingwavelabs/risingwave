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
    LogicalFilter, LogicalTopN, PlanTreeNodeUnary, PlanWindowFunction,
};
use crate::optimizer::property::{FieldOrder, Order};
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
        let input = over_agg.input();

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

        let PlanWindowFunction {
            function_type,
            return_type: _,
            partition_by,
            order_by,
        } = &over_agg.window_function;
        let with_ties = match function_type {
            WindowFunctionType::RowNumber => false,
            WindowFunctionType::Rank => true,
            WindowFunctionType::DenseRank => unreachable!("Not implemented. Banned in planner."),
        };

        let (rank_pred, other_pred) = {
            let predicate = filter.predicate();
            let mut rank_col = FixedBitSet::with_capacity(over_agg_len);
            rank_col.set(window_func_pos, true);
            predicate.clone().split_disjoint(&rank_col)
        };

        // Currently rank [ < | <= | > | >= ] N is used to implement group topn.
        // While rank = 1 is used to implement deduplication.
        let (limit, offset) = {
            // rank >= lb
            let mut lb = vec![];
            // rank <= ub
            let mut ub = vec![];
            // rank == eq
            let mut eq = vec![];

            for cond in rank_pred.conjunctions {
                if let Some((input_ref, cmp, v)) = cond.as_comparison_const() {
                    assert_eq!(input_ref.index, window_func_pos);
                    let v = v
                        .cast_implicit(DataType::Int64)
                        .ok()?
                        .eval_row_const()
                        .ok()??;
                    let v = *v.as_int64();
                    // Note: rank functions start from 1
                    match cmp {
                        ExprType::LessThanOrEqual => ub.push(v),
                        ExprType::LessThan => ub.push(v - 1),
                        ExprType::GreaterThan => lb.push(v + 1),
                        ExprType::GreaterThanOrEqual => lb.push(v),
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
                    eq.push(v);
                } else {
                    tracing::error!("Failed to optimize complex rank predicate {:?}", cond);
                    return None;
                }
            }

            if eq.len() > 1
                || lb.len() > 1
                || ub.len() > 1
                || (!eq.is_empty() && (!lb.is_empty() || !ub.is_empty()))
            {
                tracing::error!("Failed to optimize multiple complex rank predicates");
                return None;
            }
            if !eq.is_empty() {
                if eq[0] == 1 {
                    (1, 0)
                } else {
                    tracing::error!("Failed to optimize complex rank predicate rank={}", eq[0]);
                    return None;
                }
            } else {
                let lb = lb.into_iter().next();
                let ub = ub.into_iter().next();
                match (lb, ub) {
                    (Some(lb), Some(ub)) => ((ub - lb + 1).max(0) as u64, (lb - 1).max(0) as u64),
                    (Some(lb), None) => (LIMIT_ALL_COUNT, (lb - 1).max(0) as u64),
                    (None, Some(ub)) => (ub.max(0) as u64, 0),
                    (None, None) => unreachable!(),
                }
            }
        };

        if offset > 0 && with_ties {
            tracing::error!("Failed to optimize with ties and offset");
            return None;
        }

        let topn = LogicalTopN::with_group(
            input,
            limit,
            offset,
            with_ties,
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
        let filter = LogicalFilter::create(topn, other_pred);
        Some(project.clone_with_input(filter).into())
    }
}

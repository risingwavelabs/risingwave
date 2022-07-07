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

use itertools::Itertools;
use risingwave_common::types::DataType;
use risingwave_expr::expr::AggKind;

use super::{BoxedRule, Rule};
use crate::expr::{ExprType, ExprVisitor, FunctionCall, InputRef, Literal};
use crate::optimizer::plan_node::{LogicalAgg, LogicalExpand};
use crate::optimizer::PlanRef;

/// Transform distinct aggregates to `LogicalAgg` -> `LogicalAgg` -> `Expand` -> `Input`.
///
/// Here is an example:
///
/// `LogicalAgg(group by(0, 1), count($2) filter(where $2 < 100), sum(distinct $3))`
///
/// -> `Input(len of schema: 4)`
///
/// will be transformed to
///
/// `LogicalAgg(group by(0, 1), sum($4) filter(where $3 = 1), sum($5) filter(where $3 = 0))`
///
/// -> `LogicalAgg(group by(0, 1, 3, 4), count($2) filter(where $2 < 100), sum($3))`
///
/// -> `Expand(column_subsets: [[0, 1, 3], [0, 1, 2, 2]])` -> `Input(len of schema: 4)`
pub struct DistinctAgg {}
impl Rule for DistinctAgg {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let agg: &LogicalAgg = plan.as_logical_agg()?;
        let (agg_calls, agg_group_keys, input) = agg.clone().decompose();
        // The index of `flag` in schema of `Expand`.
        let pos_of_flag = input.schema().len();
        let (distinct_aggs, non_distinct_aggs): (Vec<_>, Vec<_>) = agg_calls
            .clone()
            .into_iter()
            .partition(|agg_call| agg_call.distinct);
        if distinct_aggs.is_empty() {
            return None;
        }

        let mut collect_input_index = CollectInputIndex {
            input_indices: vec![],
        };
        let mut column_subsets = distinct_aggs
            .iter()
            .map(|agg_call| {
                assert!(collect_input_index.input_indices.is_empty());
                agg_call.filter.visit_expr(&mut collect_input_index);
                let mut subset = agg_group_keys.clone();
                subset.extend(agg_call.input_indices());
                subset.append(&mut collect_input_index.input_indices);
                subset
            })
            .collect_vec();
        if !non_distinct_aggs.is_empty() {
            column_subsets.push({
                assert!(collect_input_index.input_indices.is_empty());
                let mut subset = agg_group_keys.clone();
                subset.extend(
                    non_distinct_aggs
                        .iter()
                        .flat_map(|agg_call| {
                            agg_call.filter.visit_expr(&mut collect_input_index);
                            agg_call.input_indices()
                        })
                        .collect_vec(),
                );
                subset.append(&mut collect_input_index.input_indices);
                subset
            });
        }
        let expand = LogicalExpand::create(input, column_subsets);

        let mut group_by_keys = agg_group_keys;
        let old_group_keys_len = group_by_keys.len();
        // The middle `LogicalAgg` groups by (`agg_group_keys` + arguments of distinct aggregates +
        // `flag`).
        group_by_keys.extend(
            distinct_aggs
                .iter()
                .flat_map(|agg_call| agg_call.input_indices()),
        );
        group_by_keys.push(pos_of_flag);
        let new_group_keys_len = group_by_keys.len();
        let mut new_agg_calls = agg_calls.clone();
        new_agg_calls
            .iter_mut()
            .for_each(|agg_call| agg_call.distinct = false);
        let group_by_agg = LogicalAgg::new(new_agg_calls, group_by_keys, expand);

        let mut distinct_agg_index = -1;
        // The index of `flag` in schema of the middle `LogicalAgg`.
        let pos_of_flag = new_group_keys_len - 1;
        let mut selective_aggs = agg_calls;
        selective_aggs
            .iter_mut()
            .enumerate()
            .for_each(|(index, agg_call)| {
                match agg_call.agg_kind {
                    AggKind::Count
                    | AggKind::RowCount
                    | AggKind::Sum
                    | AggKind::ApproxCountDistinct => {
                        agg_call.agg_kind = AggKind::Sum;
                    }
                    _ => {}
                };
                agg_call.inputs = vec![InputRef::new(
                    new_group_keys_len + index,
                    agg_call.return_type.clone(),
                )];

                let flag_value = if agg_call.distinct {
                    distinct_agg_index += 1;
                    distinct_agg_index
                } else {
                    distinct_aggs.len() as i32
                };
                // `filter_expr` is used to pick up the rows that are really needed by aggregates.
                let filter_expr = FunctionCall::new(
                    ExprType::Equal,
                    vec![
                        InputRef::new(pos_of_flag, DataType::Int64).into(),
                        Literal::new(Some(flag_value.into()), DataType::Int64).into(),
                    ],
                )
                .unwrap();
                // As we have offloaded the responsbility of filtering rows to the middle
                // `LogicalAgg`, we just use the new filter condition to overwrite
                // the orginal one.
                agg_call.filter.conjunctions = vec![filter_expr.into()];

                agg_call.distinct = false;
            });
        Some(
            LogicalAgg::new(
                selective_aggs,
                (0..old_group_keys_len).collect_vec(),
                group_by_agg.into(),
            )
            .into(),
        )
    }
}

impl DistinctAgg {
    pub fn create() -> BoxedRule {
        Box::new(DistinctAgg {})
    }
}

struct CollectInputIndex {
    input_indices: Vec<usize>,
}

impl ExprVisitor for CollectInputIndex {
    fn visit_input_ref(&mut self, input_ref: &InputRef) {
        self.input_indices.push(input_ref.index());
    }
}

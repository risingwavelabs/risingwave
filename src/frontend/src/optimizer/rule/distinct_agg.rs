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
use itertools::Itertools;
use risingwave_common::types::DataType;
use risingwave_expr::expr::AggKind;

use super::{BoxedRule, Rule};
use crate::expr::{ExprType, FunctionCall, InputRef, Literal};
use crate::optimizer::plan_node::{CollectInputRef, LogicalAgg, LogicalExpand, PlanAggCall};
use crate::optimizer::PlanRef;
use crate::utils::Condition;

/// Transform distinct aggregates to `LogicalAgg` -> `LogicalAgg` -> `Expand` -> `Input`.
pub struct DistinctAgg {}
impl Rule for DistinctAgg {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let agg: &LogicalAgg = plan.as_logical_agg()?;
        let (agg_calls, agg_group_keys, input) = agg.clone().decompose();
        // The index of `flag` in schema of `Expand`.
        let pos_of_flag = input.schema().len();
        let group_keys_len = agg_group_keys.len();
        let (distinct_aggs, non_distinct_aggs): (Vec<_>, Vec<_>) = agg_calls
            .clone()
            .into_iter()
            .partition(|agg_call| agg_call.distinct);
        if distinct_aggs.is_empty() {
            return None;
        }
        let flag_value_of_distinct_agg = if non_distinct_aggs.is_empty() { 0 } else { 1 };

        let expand = Self::build_expand(input, &agg_group_keys, &distinct_aggs, &non_distinct_aggs);

        let group_by_agg =
            Self::build_middle_agg(expand, agg_group_keys, agg_calls.clone(), pos_of_flag);

        Some(Self::build_final_agg(
            group_by_agg,
            group_keys_len,
            agg_calls,
            flag_value_of_distinct_agg,
        ))
    }
}

impl DistinctAgg {
    pub fn create() -> BoxedRule {
        Box::new(DistinctAgg {})
    }

    fn build_expand(
        input: PlanRef,
        group_keys: &[usize],
        distinct_aggs: &[PlanAggCall],
        non_distinct_aggs: &[PlanAggCall],
    ) -> PlanRef {
        // each `subset` in `column_subsets` consists of `group_keys`, `agg_call`'s input indices
        // and the input indices of `agg_call`'s `filter`.
        let mut column_subsets = vec![];

        if !non_distinct_aggs.is_empty() {
            column_subsets.push({
                let mut collect_input_ref = CollectInputRef::with_capacity(input.schema().len());
                collect_input_ref.extend(group_keys.to_owned());
                non_distinct_aggs.iter().for_each(|agg_call| {
                    collect_input_ref.extend(agg_call.input_indices());
                    agg_call.filter.visit_expr(&mut collect_input_ref);
                });
                FixedBitSet::from(collect_input_ref).ones().collect_vec()
            });
        }

        distinct_aggs.iter().for_each(|agg_call| {
            column_subsets.push({
                let mut collect_input_ref = CollectInputRef::with_capacity(input.schema().len());
                collect_input_ref.extend(group_keys.to_owned());
                collect_input_ref.extend(agg_call.input_indices());
                agg_call.filter.visit_expr(&mut collect_input_ref);
                FixedBitSet::from(collect_input_ref).ones().collect_vec()
            });
        });

        LogicalExpand::create(input, column_subsets)
    }

    fn build_middle_agg(
        input: PlanRef,
        mut group_keys: Vec<usize>,
        agg_calls: Vec<PlanAggCall>,
        pos_of_flag: usize,
    ) -> LogicalAgg {
        // The middle `LogicalAgg` groups by (`agg_group_keys` + arguments of distinct aggregates +
        // `flag`).
        let agg_calls = agg_calls
            .into_iter()
            .filter_map(|mut agg_call| {
                if agg_call.distinct {
                    // collect distinct agg's input indices.
                    group_keys.extend(agg_call.input_indices());
                    // filter out distinct agg without real filter(i.e. filter that isn't always
                    // true).
                    if agg_call.filter.always_true() {
                        return None;
                    }
                    // convert distinct agg with real filter to count(*) with orginal filter.
                    agg_call = PlanAggCall::with_condition(agg_call.filter);
                }
                Some(agg_call)
            })
            .collect_vec();
        group_keys.push(pos_of_flag);
        LogicalAgg::new(agg_calls, group_keys, input)
    }

    fn build_final_agg(
        input: LogicalAgg,
        old_group_keys_len: usize,
        mut agg_calls: Vec<PlanAggCall>,
        mut flag_value_of_distinct_agg: i32,
    ) -> PlanRef {
        // The index of `flag` in schema of the middle `LogicalAgg`.
        let pos_of_flag = input.group_key().len() - 1;

        // ```ignore
        // the input(middle agg) has the following schema:
        // original group columns | distinct agg arguments | flag | count_star_with_filter or non-distinct agg
        // <-                group                              -> <-             agg calls                 ->
        // ```

        // scan through `distinct agg arguments`.
        let mut index_of_distinct_agg_argument = old_group_keys_len;
        // scan through `count_star_with_filter` or `non-distinct agg`.
        let mut index_of_middle_agg = input.group_key().len();
        agg_calls.iter_mut().for_each(|agg_call| {
            let flag_value;
            if agg_call.distinct {
                agg_call.distinct = false;

                agg_call.inputs.iter_mut().for_each(|input_ref| {
                    input_ref.index = index_of_distinct_agg_argument;
                    index_of_distinct_agg_argument += 1;
                });

                // distinct-agg with real filter has its corresponding middle agg, which is count(*)
                // with its original filter.
                if !agg_call.filter.always_true() {
                    // make sure count(*) with orginal filter > 0.
                    let check_count = FunctionCall::new(
                        ExprType::GreaterThan,
                        vec![
                            InputRef::new(index_of_middle_agg, DataType::Int64).into(),
                            Literal::new(Some(0.into()), DataType::Int64).into(),
                        ],
                    )
                    .unwrap();
                    index_of_middle_agg += 1;
                    agg_call.filter.conjunctions = vec![check_count.into()];
                }

                flag_value = flag_value_of_distinct_agg;
                flag_value_of_distinct_agg += 1;
            } else {
                // non-distinct agg has its corresponding middle agg.
                agg_call.inputs = vec![InputRef::new(
                    index_of_middle_agg,
                    agg_call.return_type.clone(),
                )];
                index_of_middle_agg += 1;

                // the filter of non-distinct agg has been calculated in middle agg.
                agg_call.filter = Condition::true_cond();

                // change final agg's agg_kind just like two-phase agg.
                match agg_call.agg_kind {
                    AggKind::Count | AggKind::ApproxCountDistinct => {
                        agg_call.agg_kind = AggKind::Sum;
                    }
                    _ => {}
                };

                // the index of non-distinct aggs' subset in `column_subsets` is always 0 if it
                // exists.
                flag_value = 0;
            }

            // `filter_expr` is used to pick up the rows that are really needed by aggregates.
            let filter_expr = FunctionCall::new(
                ExprType::Equal,
                vec![
                    InputRef::new(pos_of_flag, DataType::Int64).into(),
                    Literal::new(Some(flag_value.into()), DataType::Int64).into(),
                ],
            )
            .unwrap();
            agg_call.filter.conjunctions.push(filter_expr.into());
        });

        LogicalAgg::new(
            agg_calls,
            (0..old_group_keys_len).collect_vec(),
            input.into(),
        )
        .into()
    }
}

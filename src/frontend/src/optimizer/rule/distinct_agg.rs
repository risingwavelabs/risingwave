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

use std::mem;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::types::DataType;
use risingwave_expr::expr::AggKind;

use super::{BoxedRule, Rule};
use crate::expr::{ExprImpl, ExprType, FunctionCall, InputRef, Literal};
use crate::optimizer::plan_node::{
    CollectInputRef, LogicalAgg, LogicalExpand, LogicalProject, PlanAggCall,
};
use crate::optimizer::PlanRef;
use crate::utils::{ColIndexMapping, Condition};

/// Transform distinct aggregates to `LogicalAgg` -> `LogicalAgg` -> `Expand` -> `Input`.
pub struct DistinctAggRule {}
impl Rule for DistinctAggRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let agg: &LogicalAgg = plan.as_logical_agg()?;
        let (mut agg_calls, mut agg_group_keys, input) = agg.clone().decompose();
        let original_group_keys_len = agg_group_keys.len();
        let (distinct_aggs, non_distinct_aggs): (Vec<_>, Vec<_>) = agg_calls
            .clone()
            .into_iter()
            .partition(|agg_call| agg_call.distinct);
        if distinct_aggs.is_empty() {
            return None;
        }
        let flag_value_of_distinct_agg = if non_distinct_aggs.is_empty() { 0 } else { 1 };
        let input_schema_len = input.schema().len();

        let expand = Self::build_expand(input, &agg_group_keys, &distinct_aggs, &non_distinct_aggs);
        let project = Self::build_project(
            input_schema_len,
            expand,
            &mut agg_group_keys,
            &mut agg_calls,
        );
        let mid_agg = Self::build_middle_agg(project, agg_group_keys, agg_calls.clone());
        Some(Self::build_final_agg(
            mid_agg,
            original_group_keys_len,
            agg_calls,
            flag_value_of_distinct_agg,
        ))
    }
}

impl DistinctAggRule {
    pub fn create() -> BoxedRule {
        Box::new(DistinctAggRule {})
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
                let mut subset = FixedBitSet::from_iter(group_keys.to_owned());
                non_distinct_aggs.iter().for_each(|agg_call| {
                    subset.extend(agg_call.input_indices());
                });
                subset.ones().collect_vec()
            });
        }

        distinct_aggs.iter().for_each(|agg_call| {
            column_subsets.push({
                let mut subset = FixedBitSet::from_iter(group_keys.to_owned());
                subset.extend(agg_call.input_indices());
                subset.ones().collect_vec()
            });
        });

        LogicalExpand::create(input, column_subsets)
    }

    fn build_project(
        input_schema_len: usize,
        expand: PlanRef,
        group_keys: &mut Vec<usize>,
        agg_calls: &mut Vec<PlanAggCall>,
    ) -> PlanRef {
        // shift the indices of filter first to make later rewrite more convenient.
        let mut shift_with_offset =
            ColIndexMapping::with_shift_offset(input_schema_len, input_schema_len as isize);
        for agg_call in agg_calls.iter_mut() {
            agg_call.filter = mem::replace(&mut agg_call.filter, Condition::true_cond())
                .rewrite_expr(&mut shift_with_offset);
        }

        // collect indices.
        let expand_schema_len = expand.schema().len();
        let mut input_indices = CollectInputRef::with_capacity(expand_schema_len);
        input_indices.extend(group_keys.clone());
        for agg_call in agg_calls.iter() {
            input_indices.extend(agg_call.input_indices());
            agg_call.filter.visit_expr(&mut input_indices);
        }
        // append `flag`.
        input_indices.extend(vec![expand_schema_len - 1]);
        let mut mapping = ColIndexMapping::with_remaining_columns(
            &FixedBitSet::from(input_indices).ones().collect_vec(),
            expand_schema_len,
        );

        // remap indices.
        for i in group_keys {
            *i = mapping.map(*i);
        }
        for agg_call in agg_calls {
            for input in &mut agg_call.inputs {
                input.index = mapping.map(input.index);
            }
            agg_call.filter = mem::replace(&mut agg_call.filter, Condition::true_cond())
                .rewrite_expr(&mut mapping);
        }

        LogicalProject::with_mapping(expand, mapping).into()
    }

    fn build_middle_agg(
        project: PlanRef,
        mut group_keys: Vec<usize>,
        agg_calls: Vec<PlanAggCall>,
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
                    // convert distinct agg with real filter to count(*) with original filter.
                    agg_call = PlanAggCall::count_star().with_condition(agg_call.filter);
                }
                Some(agg_call)
            })
            .collect_vec();
        // append `flag`.
        group_keys.push(project.schema().len() - 1);
        LogicalAgg::new(agg_calls, group_keys, project)
    }

    fn build_final_agg(
        mid_agg: LogicalAgg,
        original_group_keys_len: usize,
        mut agg_calls: Vec<PlanAggCall>,
        mut flag_value_of_distinct_agg: i64,
    ) -> PlanRef {
        // The index of `flag` in schema of the middle `LogicalAgg`.
        let pos_of_flag = mid_agg.group_key().len() - 1;

        // ```ignore
        // the input(middle agg) has the following schema:
        // original group columns | distinct agg arguments | flag | count_star_with_filter or non-distinct agg
        // <-                group                              -> <-             agg calls                 ->
        // ```

        // scan through `distinct agg arguments`.
        let mut index_of_distinct_agg_argument = original_group_keys_len;
        // scan through `count_star_with_filter` or `non-distinct agg`.
        let mut index_of_middle_agg = mid_agg.group_key().len();
        let mut indices_of_count = vec![];
        agg_calls.iter_mut().enumerate().for_each(|(i, agg_call)| {
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
                    // make sure count(*) with original filter > 0.
                    let check_count = FunctionCall::new(
                        ExprType::GreaterThan,
                        vec![
                            InputRef::new(index_of_middle_agg, DataType::Int64).into(),
                            Literal::new(Some(0_i64.into()), DataType::Int64).into(),
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
                //
                // future `AggKind` may or may not be able to use the same agg call for mid and
                // final agg so we use exhaustive match here to make compiler remind
                // people adding new `AggKind` to update it.
                match agg_call.agg_kind {
                    AggKind::Min
                    | AggKind::Max
                    | AggKind::Sum
                    | AggKind::Avg
                    | AggKind::StringAgg
                    | AggKind::SingleValue
                    | AggKind::ApproxCountDistinct
                    | AggKind::ArrayAgg => (),
                    AggKind::Count => {
                        indices_of_count.push(i);
                        agg_call.agg_kind = AggKind::Sum;
                    }
                }

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

        let mut plan: PlanRef = LogicalAgg::new(
            agg_calls,
            (0..original_group_keys_len).collect_vec(),
            mid_agg.into(),
        )
        .into();

        if !indices_of_count.is_empty() {
            let mut exprs: Vec<ExprImpl> = plan
                .schema()
                .data_types()
                .into_iter()
                .enumerate()
                .map(|(i, data_type)| InputRef::new(i, data_type).into())
                .collect_vec();
            for i in indices_of_count {
                let index = original_group_keys_len + i;
                exprs[index] = FunctionCall::new(
                    ExprType::Coalesce,
                    vec![
                        exprs[index].clone(),
                        Literal::new(Some(0_i64.into()), DataType::Int64).into(),
                    ],
                )
                .unwrap()
                .into();
            }
            plan = LogicalProject::create(plan, exprs);
        }
        plan
    }
}

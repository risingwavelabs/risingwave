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

use std::collections::HashMap;
use std::mem;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::types::DataType;
use risingwave_expr::agg::{agg_kinds, AggKind};

use super::{BoxedRule, Rule};
use crate::expr::{CollectInputRef, ExprType, FunctionCall, InputRef, Literal};
use crate::optimizer::plan_node::generic::Agg;
use crate::optimizer::plan_node::{LogicalAgg, LogicalExpand, LogicalProject, PlanAggCall};
use crate::optimizer::PlanRef;
use crate::utils::{ColIndexMapping, Condition, IndexSet};

/// Transform distinct aggregates to `LogicalAgg` -> `LogicalAgg` -> `Expand` -> `Input`.
pub struct DistinctAggRule {
    for_stream: bool,
}

impl Rule for DistinctAggRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let agg: &LogicalAgg = plan.as_logical_agg()?;
        let (mut agg_calls, mut agg_group_keys, grouping_sets, input) = agg.clone().decompose();
        assert!(grouping_sets.is_empty());

        if agg_calls.iter().all(|c| !c.distinct) {
            // there's no distinct agg call
            return None;
        }

        if self.for_stream && !agg_group_keys.is_empty() {
            // Due to performance issue, we don't do 2-phase agg for stream distinct agg with group
            // by. See https://github.com/risingwavelabs/risingwave/issues/7271 for more.
            return None;
        }

        if !agg_calls.iter().all(|c| {
            assert!(
                !matches!(c.agg_kind, agg_kinds::rewritten!()),
                "We shouldn't see agg kind {} here",
                c.agg_kind
            );
            let agg_kind_ok = !matches!(c.agg_kind, agg_kinds::simply_cannot_two_phase!());
            let order_ok = matches!(c.agg_kind, agg_kinds::result_unaffected_by_order_by!())
                || c.order_by.is_empty();
            agg_kind_ok && order_ok
        }) {
            tracing::warn!("DistinctAggRule: unsupported agg kind, fallback to backend impl");
            return None;
        }

        let (node, flag_values, has_expand) =
            Self::build_expand(input, &mut agg_group_keys, &mut agg_calls);
        let mid_agg =
            Self::build_middle_agg(node, agg_group_keys.clone(), agg_calls.clone(), has_expand);

        // The middle agg will extend some fields for `agg_group_keys`, so we need to find out the
        // original group key for the final agg.
        let mut final_agg_group_keys = IndexSet::empty();
        for (i, v) in mid_agg.group_key.indices().enumerate() {
            if agg_group_keys.contains(v) {
                final_agg_group_keys.insert(i);
            }
        }

        Some(Self::build_final_agg(
            mid_agg,
            final_agg_group_keys,
            agg_calls,
            flag_values,
            has_expand,
        ))
    }
}

impl DistinctAggRule {
    pub fn create(for_stream: bool) -> BoxedRule {
        Box::new(DistinctAggRule { for_stream })
    }

    /// Construct `Expand` for distinct aggregates.
    /// `group_keys` and `agg_calls` will be changed in `build_project` due to column pruning.
    /// It returns either `LogicalProject` or original input, plus `flag_values` for every distinct
    /// aggregate and `has_expand` as a flag.
    ///
    /// To simplify, we will first deduplicate `column_subsets` and then skip building
    /// `Expand` if there is only one `subset`.
    fn build_expand(
        input: PlanRef,
        group_keys: &mut IndexSet,
        agg_calls: &mut Vec<PlanAggCall>,
    ) -> (PlanRef, Vec<usize>, bool) {
        let input_schema_len = input.schema().len();
        // each `subset` in `column_subsets` consists of `group_keys`, `agg_call`'s input indices
        // and the input indices of `agg_call`'s `filter`.
        let mut column_subsets = vec![];
        // flag values of distinct aggregates.
        let mut flag_values = vec![];
        // mapping from `subset` to `flag_value`, which is used to deduplicate `column_subsets`.
        let mut hash_map = HashMap::new();
        let (distinct_aggs, non_distinct_aggs): (Vec<_>, Vec<_>) =
            agg_calls.iter().partition(|agg_call| agg_call.distinct);
        assert!(!distinct_aggs.is_empty());

        if !non_distinct_aggs.is_empty() {
            let subset = {
                let mut subset = group_keys.clone();
                non_distinct_aggs.iter().for_each(|agg_call| {
                    subset.extend(agg_call.input_indices());
                });
                subset.to_vec()
            };
            hash_map.insert(subset.clone(), 0);
            column_subsets.push(subset);
        }

        distinct_aggs.iter().for_each(|agg_call| {
            let subset = {
                let mut subset = group_keys.clone();
                subset.extend(agg_call.input_indices());
                subset.to_vec()
            };
            if let Some(i) = hash_map.get(&subset) {
                flag_values.push(*i);
            } else {
                let flag_value = column_subsets.len();
                flag_values.push(flag_value);
                hash_map.insert(subset.clone(), flag_value);
                column_subsets.push(subset);
            }
        });

        let n_different_distinct = distinct_aggs
            .iter()
            .unique_by(|agg_call| agg_call.input_indices()[0])
            .count();
        assert_ne!(n_different_distinct, 0); // since `distinct_aggs` is not empty here
        if n_different_distinct == 1 {
            // no need to have expand if there is only one distinct aggregates.
            return (input, flag_values, false);
        }

        let expand = LogicalExpand::create(input, column_subsets);
        // manual version of column pruning for expand.
        let project = Self::build_project(input_schema_len, expand, group_keys, agg_calls);
        (project, flag_values, true)
    }

    /// Used to do column pruning for `Expand`.
    fn build_project(
        input_schema_len: usize,
        expand: PlanRef,
        group_keys: &mut IndexSet,
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
        input_indices.extend(group_keys.indices());
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
        let mut new_group_keys = IndexSet::empty();
        for i in group_keys.indices() {
            new_group_keys.insert(mapping.map(i))
        }
        *group_keys = new_group_keys;
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
        mut group_keys: IndexSet,
        agg_calls: Vec<PlanAggCall>,
        has_expand: bool,
    ) -> Agg<PlanRef> {
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
        if has_expand {
            // append `flag`.
            group_keys.insert(project.schema().len() - 1);
        }
        Agg::new(agg_calls, group_keys, project)
    }

    fn build_final_agg(
        mid_agg: Agg<PlanRef>,
        final_agg_group_keys: IndexSet,
        mut agg_calls: Vec<PlanAggCall>,
        flag_values: Vec<usize>,
        has_expand: bool,
    ) -> PlanRef {
        // the index of `flag` in schema of the middle `LogicalAgg`, if has `Expand`.
        let pos_of_flag = mid_agg.group_key.len() - 1;
        let mut flag_values = flag_values.into_iter();

        // ```ignore
        // if has `Expand`, the input(middle agg) has the following schema:
        // original group columns | distinct agg arguments | flag | count_star_with_filter or non-distinct agg
        // <-                group                              -> <-             agg calls                 ->
        // ```

        // scan through `count_star_with_filter` or `non-distinct agg`.
        let mut index_of_middle_agg = mid_agg.group_key.len();
        agg_calls.iter_mut().for_each(|agg_call| {
            let flag_value = if agg_call.distinct {
                agg_call.distinct = false;

                agg_call.inputs.iter_mut().for_each(|input_ref| {
                    input_ref.index = mid_agg
                        .group_key
                        .indices()
                        .position(|x| x == input_ref.index)
                        .unwrap();
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

                flag_values.next().unwrap() as i64
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
                agg_call.agg_kind = agg_call.agg_kind.partial_to_total().expect(
                    "we should get a valid total phase agg kind here since unsupported cases have been filtered out"
                );

                // the index of non-distinct aggs' subset in `column_subsets` is always 0 if it
                // exists.
                0
            };
            if has_expand {
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
            }
        });

        Agg::new(agg_calls, final_agg_group_keys, mid_agg.into()).into()
    }
}

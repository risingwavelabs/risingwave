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

use risingwave_common::types::DataType;
use risingwave_expr::expr::AggKind;
use risingwave_pb::plan_common::JoinType;

use super::{BoxedRule, Rule};
use crate::expr::{ExprImpl, ExprType, FunctionCall, InputRef};
use crate::optimizer::plan_node::{LogicalAgg, LogicalApply, LogicalFilter, LogicalProject};
use crate::optimizer::PlanRef;
use crate::utils::{ColIndexMapping, Condition};

/// Push `LogicalApply` down `LogicalAgg`.
pub struct ApplyAggRule {}
impl Rule for ApplyAggRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply: &LogicalApply = plan.as_logical_apply()?;
        let (left, right, on, join_type, correlated_id, correlated_indices, max_one_row) =
            apply.clone().decompose();
        assert_eq!(join_type, JoinType::Inner);
        let agg: &LogicalAgg = right.as_logical_agg()?;
        let (mut agg_calls, agg_group_key, agg_input) = agg.clone().decompose();
        let is_scalar_agg = agg_group_key.is_empty();
        let agg_input_len = agg_input.schema().len();
        let apply_left_len = left.schema().len();

        if !is_scalar_agg && max_one_row {
            // We can only eliminate max_one_row for scalar aggregation.
            return None;
        }

        let input = if is_scalar_agg {
            // add a constant column to help convert count(*) to count(c) where c is non-nullable.
            let mut exprs: Vec<ExprImpl> = agg_input
                .schema()
                .data_types()
                .into_iter()
                .enumerate()
                .map(|(i, data_type)| InputRef::new(i, data_type).into())
                .collect();
            exprs.push(ExprImpl::literal_int(1));
            LogicalProject::create(agg_input, exprs)
        } else {
            agg_input
        };

        let node = if is_scalar_agg {
            // LOJ Apply need to be converted to cross Apply.
            let left_len = left.schema().len();
            let eq_predicates = left
                .schema()
                .data_types()
                .into_iter()
                .enumerate()
                .map(|(i, data_type)| {
                    let left = InputRef::new(i, data_type.clone());
                    let right = InputRef::new(i + left_len, data_type);
                    // use null-safe equal
                    FunctionCall::new_unchecked(
                        ExprType::IsNotDistinctFrom,
                        vec![left.into(), right.into()],
                        DataType::Boolean,
                    )
                    .into()
                })
                .collect();
            LogicalApply::new(
                left.clone(),
                input,
                JoinType::LeftOuter,
                Condition::true_cond(),
                correlated_id,
                correlated_indices,
                false,
            )
            .translate_apply(left, eq_predicates)
        } else {
            LogicalApply::new(
                left,
                input,
                JoinType::Inner,
                Condition::true_cond(),
                correlated_id,
                correlated_indices,
                false,
            )
            .into()
        };

        let group_agg = {
            // shift index of agg_calls' `InputRef` with `apply_left_len`.
            let offset = apply_left_len as isize;
            let mut shift_index = ColIndexMapping::with_shift_offset(agg_input_len, offset);
            agg_calls.iter_mut().for_each(|agg_call| {
                agg_call.inputs.iter_mut().for_each(|input_ref| {
                    input_ref.shift_with_offset(offset);
                });
                agg_call
                    .order_by_fields
                    .iter_mut()
                    .for_each(|o| o.input.shift_with_offset(offset));
                agg_call.filter = agg_call.filter.clone().rewrite_expr(&mut shift_index);
            });
            if is_scalar_agg {
                // convert count(*) to count(1).
                let pos_of_constant_column = node.schema().len() - 1;
                agg_calls.iter_mut().for_each(|agg_call| {
                    if agg_call.agg_kind == AggKind::Count && agg_call.inputs.is_empty() {
                        let input_ref = InputRef::new(pos_of_constant_column, DataType::Int32);
                        agg_call.inputs.push(input_ref);
                    }
                });
            }
            let mut group_keys: Vec<usize> = (0..apply_left_len).collect();
            group_keys.extend(agg_group_key.into_iter().map(|key| key + apply_left_len));
            LogicalAgg::new(agg_calls, group_keys, node).into()
        };

        let filter = LogicalFilter::create(group_agg, on);
        Some(filter)
    }
}

impl ApplyAggRule {
    pub fn create() -> BoxedRule {
        Box::new(ApplyAggRule {})
    }
}

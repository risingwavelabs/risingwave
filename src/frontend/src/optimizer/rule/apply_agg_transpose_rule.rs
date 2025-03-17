// Copyright 2025 RisingWave Labs
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

use risingwave_common::types::DataType;
use risingwave_expr::aggregate::{AggType, PbAggKind};
use risingwave_pb::plan_common::JoinType;

use super::{ApplyOffsetRewriter, BoxedRule, Rule};
use crate::expr::{ExprImpl, ExprType, FunctionCall, InputRef};
use crate::optimizer::PlanRef;
use crate::optimizer::plan_node::generic::Agg;
use crate::optimizer::plan_node::{LogicalAgg, LogicalApply, LogicalFilter, LogicalProject};
use crate::utils::{Condition, IndexSet};

/// Transpose `LogicalApply` and `LogicalAgg`.
///
/// Before:
///
/// ```text
///     LogicalApply
///    /            \
///  Domain      LogicalAgg
///                  |
///                Input
/// ```
///
/// After:
///
/// ```text
///      LogicalAgg
///          |
///     LogicalApply
///    /            \
///  Domain        Input
/// ```
pub struct ApplyAggTransposeRule {}
impl Rule for ApplyAggTransposeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply: &LogicalApply = plan.as_logical_apply()?;
        let (left, right, on, join_type, correlated_id, correlated_indices, max_one_row) =
            apply.clone().decompose();
        assert_eq!(join_type, JoinType::Inner);
        let agg: &LogicalAgg = right.as_logical_agg()?;
        let (mut agg_calls, agg_group_key, grouping_sets, agg_input, enable_two_phase) =
            agg.clone().decompose();
        assert!(grouping_sets.is_empty());
        let is_scalar_agg = agg_group_key.is_empty();
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
                correlated_indices.clone(),
                false,
                false,
            )
            .translate_apply(left, eq_predicates)
        } else {
            LogicalApply::create(
                left,
                input,
                JoinType::Inner,
                Condition::true_cond(),
                correlated_id,
                correlated_indices.clone(),
                false,
            )
        };

        let group_agg = {
            // shift index of agg_calls' `InputRef` with `apply_left_len`.
            let offset = apply_left_len as isize;
            let mut rewriter =
                ApplyOffsetRewriter::new(apply_left_len, &correlated_indices, correlated_id);
            agg_calls.iter_mut().for_each(|agg_call| {
                agg_call.inputs.iter_mut().for_each(|input_ref| {
                    input_ref.shift_with_offset(offset);
                });
                agg_call
                    .order_by
                    .iter_mut()
                    .for_each(|o| o.shift_with_offset(offset));
                agg_call.filter = agg_call.filter.clone().rewrite_expr(&mut rewriter);
            });
            if is_scalar_agg {
                // convert count(*) to count(1).
                let pos_of_constant_column = node.schema().len() - 1;
                agg_calls.iter_mut().for_each(|agg_call| {
                    match agg_call.agg_type {
                        AggType::Builtin(PbAggKind::Count) if agg_call.inputs.is_empty() => {
                            let input_ref = InputRef::new(pos_of_constant_column, DataType::Int32);
                            agg_call.inputs.push(input_ref);
                        }
                        AggType::Builtin(PbAggKind::ArrayAgg
                        | PbAggKind::JsonbAgg
                        | PbAggKind::JsonbObjectAgg)
                        | AggType::UserDefined(_)
                        | AggType::WrapScalar(_) => {
                            let input_ref = InputRef::new(pos_of_constant_column, DataType::Int32);
                            let cond = FunctionCall::new(ExprType::IsNotNull, vec![input_ref.into()]).unwrap();
                            agg_call.filter.conjunctions.push(cond.into());
                        }
                        AggType::Builtin(PbAggKind::Count
                        | PbAggKind::Sum
                        | PbAggKind::Sum0
                        | PbAggKind::Avg
                        | PbAggKind::Min
                        | PbAggKind::Max
                        | PbAggKind::BitAnd
                        | PbAggKind::BitOr
                        | PbAggKind::BitXor
                        | PbAggKind::BoolAnd
                        | PbAggKind::BoolOr
                        | PbAggKind::StringAgg
                        // not in PostgreSQL
                        | PbAggKind::ApproxCountDistinct
                        | PbAggKind::FirstValue
                        | PbAggKind::LastValue
                        | PbAggKind::InternalLastSeenValue
                        // All statistical aggregates only consider non-null inputs.
                        | PbAggKind::ApproxPercentile
                        | PbAggKind::VarPop
                        | PbAggKind::VarSamp
                        | PbAggKind::StddevPop
                        | PbAggKind::StddevSamp
                        // All ordered-set aggregates ignore null values in their aggregated input.
                        | PbAggKind::PercentileCont
                        | PbAggKind::PercentileDisc
                        | PbAggKind::Mode
                        // `grouping` has no *aggregate* input and unreachable when `is_scalar_agg`.
                        | PbAggKind::Grouping)
                        => {
                            // no-op when `agg(0 rows) == agg(1 row of nulls)`
                        }
                        AggType::Builtin(PbAggKind::Unspecified | PbAggKind::UserDefined | PbAggKind::WrapScalar) => {
                            panic!("Unexpected aggregate function: {:?}", agg_call.agg_type)
                        }
                    }
                });
            }
            let mut group_keys: IndexSet = (0..apply_left_len).collect();
            group_keys.extend(agg_group_key.indices().map(|key| key + apply_left_len));
            Agg::new(agg_calls, group_keys, node)
                .with_enable_two_phase(enable_two_phase)
                .into()
        };

        let filter = LogicalFilter::create(group_agg, on);
        Some(filter)
    }
}

impl ApplyAggTransposeRule {
    pub fn create() -> BoxedRule {
        Box::new(ApplyAggTransposeRule {})
    }
}

// Copyright 2026 RisingWave Labs
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

use itertools::Itertools;
use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqFast;

use super::prelude::{PlanRef, *};
use crate::expr::{Expr, ExprImpl, ExprRewriter, FunctionCall, InputRef};
use crate::optimizer::plan_node::generic::{Agg, GenericPlanRef};
use crate::optimizer::plan_node::*;
use crate::utils::{ColIndexMapping, IndexSet};

/// Computes a group key in a project above the aggregation instead, if its expression in the input
/// project is computed from the expressions of other group keys.
///
/// Before:
/// ```text
/// LogicalAgg group_key: [k, window_start, window_end]
///   LogicalProject exprs: [k, tumble_start(ts, size), tumble_start(ts, size) + size]
/// ```
/// After:
/// ```text
/// LogicalProject exprs: [k, window_start, window_start + size, ..]
///   LogicalAgg group_key: [k, window_start]
///     LogicalProject exprs: [k, tumble_start(ts, size), tumble_start(ts, size) + size]
/// ```
///
/// Unlike [`AggGroupBySimplifyRule`], which relies on functional dependencies that only hold for
/// the current content of the input, the expression holds for every row, so the groups are the
/// same before and after the rewrite, including for changes within a barrier.
pub struct AggGroupByDerivedKeyRule {}

impl Rule<Logical> for AggGroupByDerivedKeyRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let agg: &LogicalAgg = plan.as_logical_agg()?;
        let (agg_calls, group_key, grouping_sets, input, enable_two_phase) =
            agg.clone().decompose();
        let project = input.as_logical_project()?;
        if !grouping_sets.is_empty() {
            return None;
        }
        let exprs = project.exprs();
        let group_key = group_key.to_vec();

        // Derived keys are computed from base keys, which contain no other key, so that every base
        // key is kept and derived keys never depend on each other.
        let base_keys = group_key
            .iter()
            .copied()
            .filter(|&k| {
                group_key
                    .iter()
                    .all(|&other| other == k || !contains(&exprs[k], &exprs[other]))
            })
            .collect_vec();
        let usable_base_keys = base_keys
            .iter()
            .copied()
            .filter(|&k| equal_values_are_identical(&exprs[k].return_type()))
            .collect_vec();
        let derived = group_key
            .iter()
            .map(|&k| {
                let expr = &exprs[k];
                if base_keys.contains(&k) || expr.is_impure() {
                    return None;
                }
                compute_from_keys(expr, exprs, &usable_base_keys)
            })
            .collect_vec();
        let kept = group_key
            .iter()
            .zip_eq_fast(&derived)
            .filter(|(_, derived)| derived.is_none())
            .map(|(&k, _)| k)
            .collect_vec();
        if kept.len() == group_key.len() {
            return None;
        }

        // `derived` refers to the input columns of the aggregation; map them to the new output.
        let mut mapping = ColIndexMapping::with_remaining_columns(&kept, input.schema().len());
        let mut output_exprs = group_key
            .iter()
            .zip_eq_fast(derived)
            .map(|(&k, derived)| match derived {
                Some(expr) => mapping.rewrite_expr(expr),
                None => InputRef::new(mapping.map(k), exprs[k].return_type()).into(),
            })
            .collect_vec();
        output_exprs.extend(
            agg_calls
                .iter()
                .enumerate()
                .map(|(i, call)| InputRef::new(kept.len() + i, call.return_type.clone()).into()),
        );

        let new_agg = Agg::new(agg_calls, IndexSet::from(kept), input)
            .with_enable_two_phase(enable_two_phase);
        Some(LogicalProject::create(new_agg.into(), output_exprs))
    }
}

impl AggGroupByDerivedKeyRule {
    pub fn create() -> BoxedRule {
        Box::new(AggGroupByDerivedKeyRule {})
    }
}

/// Rewrites `expr` of a project to refer to the outputs `keys` of the project wherever their
/// expressions occur, if no other column is left.
fn compute_from_keys(expr: &ExprImpl, exprs: &[ExprImpl], keys: &[usize]) -> Option<ExprImpl> {
    if let Some(&key) = keys.iter().find(|&&key| exprs[key] == *expr) {
        return Some(InputRef::new(key, expr.return_type()).into());
    }
    match expr {
        ExprImpl::Literal(_) => Some(expr.clone()),
        ExprImpl::FunctionCall(call) => {
            let inputs = call
                .inputs()
                .iter()
                .map(|input| compute_from_keys(input, exprs, keys))
                .collect::<Option<Vec<_>>>()?;
            Some(FunctionCall::new_unchecked(call.func_type(), inputs, call.return_type()).into())
        }
        _ => None,
    }
}

/// Whether `sub` is a proper subexpression of `expr`.
fn contains(expr: &ExprImpl, sub: &ExprImpl) -> bool {
    match expr {
        ExprImpl::FunctionCall(call) => call
            .inputs()
            .iter()
            .any(|input| input == sub || contains(input, sub)),
        _ => false,
    }
}

/// Whether values of `data_type` that compare equal are always identical. Otherwise an expression
/// may tell apart values in one group, such as `-0.0` and `0.0`, `1.0` and `1.00` of decimal, or
/// `1 day` and `24 hours` of interval.
fn equal_values_are_identical(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Int256
            | DataType::Serial
            | DataType::Varchar
            | DataType::Bytea
            | DataType::Date
            | DataType::Time
            | DataType::Timestamp
            | DataType::Timestamptz
    )
}

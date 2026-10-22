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
use crate::expr::{Expr, ExprImpl, ExprRewriter, ExprType, FunctionCall, InputRef, Literal};
use crate::optimizer::plan_node::generic::{Agg, GenericPlanRef};
use crate::optimizer::plan_node::*;
use crate::utils::{ColIndexMapping, IndexSet};

/// Removes a group key that is a pure function of other group keys of the same input row, and
/// computes it in a project above the aggregation instead.
///
/// Before:
/// group by = [k, `window_start`, `window_end`], where `window_end` = `window_start` + size
/// After:
/// project [k, `window_start`, `window_start` + size, ..]
///   group by = [k, `window_start`]
///
/// Unlike [`AggGroupBySimplifyRule`], which relies on functional dependencies that only hold for
/// the current content of the input, this derivation holds for every row, so the groups are the
/// same before and after the rewrite, including for changes within a barrier.
pub struct AggGroupByDerivedKeyRule {}

impl Rule<Logical> for AggGroupByDerivedKeyRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let agg: &LogicalAgg = plan.as_logical_agg()?;
        let (agg_calls, group_key, grouping_sets, input, enable_two_phase) =
            agg.clone().decompose();
        if !grouping_sets.is_empty() {
            return None;
        }

        let group_key = group_key.to_vec();
        let mut kept = group_key.clone();
        // A key used by a derivation must stay, so that derivations never chain.
        let mut pinned = vec![];
        let mut derived = vec![None; group_key.len()];
        for (i, &col) in group_key.iter().enumerate() {
            if pinned.contains(&col) {
                continue;
            }
            let others = kept.iter().copied().filter(|&c| c != col).collect_vec();
            // Keep at least one group key, since an aggregation without any outputs a row for an
            // empty input, e.g. for `group by 1`.
            if others.is_empty() {
                continue;
            }
            if let Some(expr) = derive_column(&input, col, &others) {
                pinned.extend(expr.collect_input_refs(input.schema().len()).ones());
                kept.retain(|&c| c != col);
                derived[i] = Some(expr);
            }
        }
        if kept.len() == group_key.len() {
            return None;
        }

        // `derived` refers to the input columns of the aggregation; map them to the new output.
        let mut mapping = ColIndexMapping::with_remaining_columns(&kept, input.schema().len());
        let mut exprs = vec![];
        for (&col, derived) in group_key.iter().zip_eq_fast(derived) {
            exprs.push(match derived {
                Some(expr) => mapping.rewrite_expr(expr),
                None => {
                    InputRef::new(mapping.map(col), input.schema().fields()[col].data_type()).into()
                }
            });
        }
        for (i, call) in agg_calls.iter().enumerate() {
            exprs.push(InputRef::new(kept.len() + i, call.return_type.clone()).into());
        }

        let new_agg = Agg::new(agg_calls, IndexSet::from(kept), input)
            .with_enable_two_phase(enable_two_phase);
        Some(LogicalProject::create(new_agg.into(), exprs))
    }
}

impl AggGroupByDerivedKeyRule {
    pub fn create() -> BoxedRule {
        Box::new(AggGroupByDerivedKeyRule {})
    }
}

/// Returns an expression that computes column `col` of `plan` from the columns `keys` of the same
/// row, referring to them by their index in `plan`.
fn derive_column(plan: &PlanRef, col: usize, keys: &[usize]) -> Option<ExprImpl> {
    if let Some(project) = plan.as_logical_project() {
        let expr = &project.exprs()[col];
        if expr.is_impure() {
            return None;
        }
        return derive_project_expr(project, expr, keys);
    }
    if let Some(hop) = plan.as_logical_hop_window() {
        let start = hop.output_window_start_col_idx()?;
        if hop.output_window_end_col_idx() != Some(col) || !keys.contains(&start) {
            return None;
        }
        let window_size = hop.window_end_offset_from_start()?;
        let start = InputRef::new(start, plan.schema().fields()[start].data_type());
        let window_size = Literal::new(Some(window_size.into()), DataType::Interval);
        return FunctionCall::new(ExprType::Add, vec![start.into(), window_size.into()])
            .ok()
            .map(Into::into);
    }
    None
}

/// Rewrites `expr` of `project` over the outputs `keys` of `project`, by replacing every subtree
/// equal to the expression of a key with a reference to that key.
fn derive_project_expr(
    project: &LogicalProject,
    expr: &ExprImpl,
    keys: &[usize],
) -> Option<ExprImpl> {
    let exprs = project.exprs();
    let data_type = expr.return_type();
    if equal_values_are_identical(&data_type)
        && let Some(&key) = keys.iter().find(|&&k| exprs[k] == *expr)
    {
        return Some(InputRef::new(key, data_type).into());
    }
    match expr {
        ExprImpl::Literal(_) => Some(expr.clone()),
        ExprImpl::FunctionCall(call) => {
            let inputs = call
                .inputs()
                .iter()
                .map(|input| derive_project_expr(project, input, keys))
                .collect::<Option<Vec<_>>>()?;
            Some(FunctionCall::new_unchecked(call.func_type(), inputs, call.return_type()).into())
        }
        ExprImpl::InputRef(input_ref) => {
            // A column of the project's input, which may be derived from the keys that are plain
            // columns of that input.
            let (input_keys, keys): (Vec<_>, Vec<_>) = keys
                .iter()
                .filter_map(|&k| Some((exprs[k].as_input_ref()?.index(), k)))
                .unzip();
            let derived = derive_column(&project.input(), input_ref.index(), &input_keys)?;
            let mut mapping = ColIndexMapping::new(
                (0..project.input().schema().len())
                    .map(|i| input_keys.iter().position(|&k| k == i).map(|p| keys[p]))
                    .collect(),
                exprs.len(),
            );
            Some(mapping.rewrite_expr(derived))
        }
        _ => None,
    }
}

/// Whether group key values of `data_type` that compare equal are always identical. Otherwise a
/// function may tell apart values in one group, such as `-0.0` and `0.0`, `1.0` and `1.00` of
/// decimal, or `1 day` and `24 hours` of interval.
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

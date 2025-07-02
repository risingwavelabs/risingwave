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

use std::collections::HashMap;

use risingwave_common::types::DataType;
use risingwave_pb::plan_common::JoinType;

use super::{BoxedRule, Rule};
use crate::expr::{Expr, ExprImpl, ExprType, FunctionCall, InputRef};
use crate::optimizer::PlanRef;
use crate::optimizer::plan_node::{LogicalFilter, LogicalJoin, LogicalProject};
use crate::optimizer::plan_visitor::PlanCorrelatedIdFinder;
use crate::utils::Condition;

/// Eliminate `LogicalApply` if we can't find its `correlated_id` in its RHS.
///
/// Before:
///
/// ```text
///    LogicalApply
///    /           \
///  Domain       RHS
/// ```
///
/// If it can remove DAG.
/// After:
///
/// ```text
///  LogicalProject
///        |
///  LogicalFilter (Null reject for equal)
///        |
///       RHS
/// ```
///
///
/// If it can't remove DAG.
/// After:
///
/// ```text
///     LogicalJoin
///    /           \
///  Domain       RHS
/// ```
pub struct ApplyEliminateRule {}
impl Rule for ApplyEliminateRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply = plan.as_logical_apply()?;
        let (left, right, on, join_type, correlated_id, correlated_indices, max_one_row) =
            apply.clone().decompose();

        if max_one_row {
            return None;
        }

        // Still can find `correlated_id`, so bail out.
        if PlanCorrelatedIdFinder::find_correlated_id(right.clone(), &correlated_id) {
            return None;
        }

        let apply_left_len = left.schema().len();
        assert_eq!(join_type, JoinType::Inner);

        // Record the mapping from `CorrelatedInputRef`'s index to `InputRef`'s index.
        // We currently can remove DAG only if ALL the `CorrelatedInputRef` are equal joined to
        // `InputRef`.
        // TODO: Do some transformation for IN, and try to remove DAG for it.
        let mut column_mapping = HashMap::new();
        on.conjunctions.iter().for_each(|expr| {
            if let ExprImpl::FunctionCall(func_call) = expr
                && let Some((left, right, data_type)) = Self::check(func_call, apply_left_len)
            {
                column_mapping.insert(left, (right, data_type));
            }
        });
        if column_mapping.len() == apply_left_len {
            // Remove DAG.

            // Replace `LogicalApply` with `LogicalProject` and insert the `InputRef`s which is
            // equal to `CorrelatedInputRef` at the beginning of `LogicalProject`.
            // See the fourth section of Unnesting Arbitrary Queries for how to do the optimization.
            let mut exprs: Vec<ExprImpl> = (0..correlated_indices.len())
                .map(|i| {
                    let (col_index, data_type) = column_mapping.get(&i).unwrap();
                    InputRef::new(*col_index - apply_left_len, data_type.clone()).into()
                })
                .collect();
            exprs.extend(
                right
                    .schema()
                    .data_types()
                    .into_iter()
                    .enumerate()
                    .map(|(index, data_type)| InputRef::new(index, data_type).into()),
            );
            let project = LogicalProject::create(right, exprs);

            // Null reject for equal
            let filter_exprs: Vec<ExprImpl> = (0..correlated_indices.len())
                .map(|i| {
                    ExprImpl::FunctionCall(Box::new(FunctionCall::new_unchecked(
                        ExprType::IsNotNull,
                        vec![ExprImpl::InputRef(Box::new(InputRef::new(
                            i,
                            project.schema().fields[i].data_type.clone(),
                        )))],
                        DataType::Boolean,
                    )))
                })
                .collect();

            let filter = LogicalFilter::create(
                project,
                Condition {
                    conjunctions: filter_exprs,
                },
            );

            Some(filter)
        } else {
            let join = LogicalJoin::new(left, right, join_type, on);
            Some(join.into())
        }
    }
}

impl ApplyEliminateRule {
    pub fn create() -> BoxedRule {
        Box::new(ApplyEliminateRule {})
    }

    /// Check whether the `func_call` is like v1 = v2, in which v1 and v2 belong respectively to
    /// `LogicalApply`'s left and right.
    fn check(func_call: &FunctionCall, apply_left_len: usize) -> Option<(usize, usize, DataType)> {
        let inputs = func_call.inputs();
        if func_call.func_type() == ExprType::Equal && inputs.len() == 2 {
            let left = &inputs[0];
            let right = &inputs[1];
            match (left, right) {
                (ExprImpl::InputRef(left), ExprImpl::InputRef(right)) => {
                    let left_type = left.return_type();
                    let left = left.index();
                    let right_type = right.return_type();
                    let right = right.index();
                    if left < apply_left_len && right >= apply_left_len {
                        Some((left, right, right_type))
                    } else if left >= apply_left_len && right < apply_left_len {
                        Some((right, left, left_type))
                    } else {
                        None
                    }
                }
                _ => None,
            }
        } else {
            None
        }
    }
}

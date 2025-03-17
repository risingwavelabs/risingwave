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

use risingwave_pb::plan_common::JoinType;

use super::{BoxedRule, Rule};
use crate::expr::InputRef;
use crate::optimizer::PlanRef;
use crate::optimizer::plan_node::{LogicalApply, LogicalFilter, LogicalOverWindow};
use crate::utils::Condition;

/// Transpose `LogicalApply` and `LogicalOverWindow`.
///
/// Before:
///
/// ```text
///     LogicalApply
///    /            \
///  Domain    LogicalOverWindow
///                  |
///                Input
/// ```
///
/// After:
///
/// ```text
///    LogicalOverWindow
///          |
///     LogicalApply
///    /            \
///  Domain        Input
/// ```
pub struct ApplyOverWindowTransposeRule {}
impl Rule for ApplyOverWindowTransposeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply: &LogicalApply = plan.as_logical_apply()?;
        let (left, right, on, join_type, correlated_id, correlated_indices, max_one_row) =
            apply.clone().decompose();
        assert_eq!(join_type, JoinType::Inner);
        let over_window: &LogicalOverWindow = right.as_logical_over_window()?;
        let (window_input, mut window_functions) = over_window.clone().decompose();

        if max_one_row {
            return None;
        }

        let apply_left_len = left.schema().len();
        let apply_left_schema = left.schema().clone();

        let new_apply = LogicalApply::create(
            left,
            window_input,
            JoinType::Inner,
            Condition::true_cond(),
            correlated_id,
            correlated_indices,
            false,
        );

        let new_over_window = {
            // Shift index of window functions' `InputRef` with `apply_left_len`.
            // Add domain
            window_functions.iter_mut().for_each(|func| {
                func.args
                    .iter_mut()
                    .for_each(|arg| arg.shift_with_offset(apply_left_len as isize));
                func.order_by
                    .iter_mut()
                    .for_each(|c| c.column_index += apply_left_len);
                func.partition_by
                    .iter_mut()
                    .for_each(|x| x.shift_with_offset(apply_left_len as isize));
                // Add the domain columns to the partition by clauses.
                func.partition_by = (0..apply_left_len)
                    .map(|i| InputRef::new(i, apply_left_schema.fields[i].data_type()))
                    .chain(func.partition_by.drain(..))
                    .collect();
            });

            LogicalOverWindow::new(window_functions, new_apply)
        };

        let filter = LogicalFilter::create(new_over_window.into(), on);
        Some(filter)
    }
}

impl ApplyOverWindowTransposeRule {
    pub fn create() -> BoxedRule {
        Box::new(ApplyOverWindowTransposeRule {})
    }
}

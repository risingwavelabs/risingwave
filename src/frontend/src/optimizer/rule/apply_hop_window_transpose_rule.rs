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
use crate::optimizer::PlanRef;
use crate::optimizer::plan_node::{LogicalApply, LogicalFilter, LogicalHopWindow};
use crate::utils::Condition;

/// Transpose `LogicalApply` and `LogicalHopWindow`.
///
/// Before:
///
/// ```text
///     LogicalApply
///    /            \
///  Domain      LogicalHopWindow
///                  |
///                Input
/// ```
///
/// After:
///
/// ```text
///    LogicalHopWindow
///          |
///    LogicalApply
///    /            \
///  Domain        Input
/// ```
pub struct ApplyHopWindowTransposeRule {}
impl Rule for ApplyHopWindowTransposeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply: &LogicalApply = plan.as_logical_apply()?;
        let (left, right, on, join_type, correlated_id, correlated_indices, max_one_row) =
            apply.clone().decompose();
        let hop_window: &LogicalHopWindow = right.as_logical_hop_window()?;
        assert_eq!(join_type, JoinType::Inner);

        if !hop_window.output_indices_are_trivial() {
            return None;
        }

        let (hop_window_input, time_col, window_slide, window_size, window_offset, _output_indices) =
            hop_window.clone().into_parts();

        let apply_left_len = left.schema().len() as isize;

        if max_one_row {
            return None;
        }

        let new_apply = LogicalApply::create(
            left,
            hop_window_input,
            JoinType::Inner,
            Condition::true_cond(),
            correlated_id,
            correlated_indices,
            false,
        );

        let new_hop_window = LogicalHopWindow::create(
            new_apply,
            time_col.clone_with_offset(apply_left_len),
            window_slide,
            window_size,
            window_offset,
        );

        let filter = LogicalFilter::create(new_hop_window, on);
        Some(filter)
    }
}

impl ApplyHopWindowTransposeRule {
    pub fn create() -> BoxedRule {
        Box::new(ApplyHopWindowTransposeRule {})
    }
}

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

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_pb::plan_common::JoinType;

use super::{BoxedRule, Rule};
use crate::optimizer::PlanRef;
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{LogicalApply, LogicalExpand, LogicalFilter, LogicalProject};
use crate::utils::Condition;

/// Transpose `LogicalApply` and `LogicalExpand`.
///
/// Before:
///
/// ```text
///     LogicalApply
///    /            \
///  Domain      LogicalExpand
///                  |
///                Input
/// ```
///
/// After:
///
/// ```text
///     LogicalProject (type alignment)
///           |
///      LogicalExpand
///          |
///     LogicalApply
///    /            \
///  Domain        Input
/// ```
pub struct ApplyExpandTransposeRule {}
impl Rule for ApplyExpandTransposeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply: &LogicalApply = plan.as_logical_apply()?;
        let (left, right, on, join_type, correlated_id, correlated_indices, max_one_row) =
            apply.clone().decompose();
        assert_eq!(join_type, JoinType::Inner);
        let logical_expand: &LogicalExpand = right.as_logical_expand()?;
        let (expand_input, mut column_subsets) = logical_expand.clone().decompose();

        let apply_left_len = left.schema().len();

        if max_one_row {
            return None;
        }

        let new_apply: PlanRef = LogicalApply::create(
            left,
            expand_input,
            JoinType::Inner,
            Condition::true_cond(),
            correlated_id,
            correlated_indices,
            false,
        );

        let new_apply_schema_len = new_apply.schema().len();

        let new_expand = {
            // Shift index of column_subsets with `apply_left_len`.
            // Add domain to the column_subsets.
            column_subsets.iter_mut().for_each(|subset| {
                subset.iter_mut().for_each(|i| *i += apply_left_len);
                *subset = (0..apply_left_len).chain(subset.drain(..)).collect_vec();
            });
            LogicalExpand::new(new_apply, column_subsets)
        };

        // Since `LogicalExpand`'s schema is based on its input, we need a project to align the
        // schema type. Previous apply schema = Domain + Input + Input + flag
        // New expand schema = Domain + Input + Domain + Input + flag
        let mut fixed_bit_set = FixedBitSet::with_capacity(new_expand.base.schema().len());
        fixed_bit_set.toggle_range(..);
        fixed_bit_set.toggle_range(new_apply_schema_len..(new_apply_schema_len + apply_left_len));
        let project = LogicalProject::with_out_fields(new_expand.into(), &fixed_bit_set);

        let filter = LogicalFilter::create(project.into(), on);
        Some(filter)
    }
}

impl ApplyExpandTransposeRule {
    pub fn create() -> BoxedRule {
        Box::new(ApplyExpandTransposeRule {})
    }
}

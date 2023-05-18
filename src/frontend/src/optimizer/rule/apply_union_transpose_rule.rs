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

use itertools::Itertools;

use super::{BoxedRule, Rule};
use crate::optimizer::plan_node::{LogicalApply, LogicalUnion, PlanTreeNode, PlanTreeNodeBinary};
use crate::optimizer::PlanRef;

/// Transpose `LogicalApply` and `LogicalUnion`.
///
/// Before:
///
/// ```text
///     LogicalApply
///    /            \
///  Domain      LogicalUnion
///                /      \
///               T1     T2
/// ```
///
/// After:
///
/// ```text
///           LogicalUnion
///         /            \
///  LogicalApply     LogicalApply
///   /      \           /      \
/// Domain   T1        Domain   T2
/// ```

pub struct ApplyUnionTransposeRule {}
impl Rule for ApplyUnionTransposeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply: &LogicalApply = plan.as_logical_apply()?;
        if apply.max_one_row() {
            return None;
        }
        let left = apply.left();
        let right = apply.right();
        let union: &LogicalUnion = right.as_logical_union()?;

        let new_inputs = union
            .inputs()
            .into_iter()
            .map(|input| apply.clone_with_left_right(left.clone(), input).into())
            .collect_vec();
        Some(union.clone_with_inputs(&new_inputs))
    }
}

impl ApplyUnionTransposeRule {
    pub fn create() -> BoxedRule {
        Box::new(ApplyUnionTransposeRule {})
    }
}

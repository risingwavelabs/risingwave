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
use risingwave_pb::plan_common::JoinType;

use super::{ApplyOffsetRewriter, BoxedRule, Rule};
use crate::expr::{ExprImpl, ExprRewriter, InputRef};
use crate::optimizer::plan_node::{LogicalApply, LogicalProject};
use crate::optimizer::PlanRef;

/// Transpose `LogicalApply` and `LogicalProject`.
///
/// Before:
///
/// ```text
///     LogicalApply
///    /            \
///  Domain      LogicalProject
///                  |
///                Input
/// ```
///
/// After:
///
/// ```text
///    LogicalProject
///          |
///    LogicalApply
///    /            \
///  Domain        Input
/// ```
pub struct ApplyProjectTransposeRule {}
impl Rule for ApplyProjectTransposeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply: &LogicalApply = plan.as_logical_apply()?;
        let (left, right, on, join_type, correlated_id, correlated_indices, max_one_row) =
            apply.clone().decompose();
        let project = right.as_logical_project()?;
        assert_eq!(join_type, JoinType::Inner);

        // Insert all the columns of `LogicalApply`'s left at the beginning of the new
        // `LogicalProject`.
        let mut exprs: Vec<ExprImpl> = left
            .schema()
            .data_types()
            .into_iter()
            .enumerate()
            .map(|(index, data_type)| InputRef::new(index, data_type).into())
            .collect();

        let (proj_exprs, proj_input) = project.clone().decompose();

        // replace correlated_input_ref in project exprs
        let mut rewriter =
            ApplyOffsetRewriter::new(left.schema().len(), &correlated_indices, correlated_id);

        let new_proj_exprs: Vec<ExprImpl> = proj_exprs
            .into_iter()
            .map(|expr| rewriter.rewrite_expr(expr))
            .collect_vec();

        exprs.extend(new_proj_exprs.clone().into_iter());

        let mut rewriter = ApplyOnConditionRewriter {
            left_input_len: left.schema().len(),
            mapping: new_proj_exprs,
        };
        let new_on = on.rewrite_expr(&mut rewriter);
        let new_apply = LogicalApply::create(
            left,
            proj_input,
            join_type,
            new_on,
            correlated_id,
            correlated_indices,
            max_one_row,
        );

        let new_project = LogicalProject::create(new_apply, exprs);
        Some(new_project)
    }
}

impl ApplyProjectTransposeRule {
    pub fn create() -> BoxedRule {
        Box::new(ApplyProjectTransposeRule {})
    }
}

pub struct ApplyOnConditionRewriter {
    pub left_input_len: usize,
    pub mapping: Vec<ExprImpl>,
}

impl ExprRewriter for ApplyOnConditionRewriter {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        if input_ref.index >= self.left_input_len {
            self.mapping[input_ref.index() - self.left_input_len].clone()
        } else {
            input_ref.into()
        }
    }
}

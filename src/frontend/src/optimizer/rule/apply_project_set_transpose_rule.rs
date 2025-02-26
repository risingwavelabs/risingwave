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

use itertools::Itertools;
use risingwave_pb::plan_common::JoinType;

use super::{ApplyOffsetRewriter, BoxedRule, Rule};
use crate::expr::{ExprImpl, ExprRewriter, InputRef};
use crate::optimizer::PlanRef;
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{LogicalApply, LogicalProject, LogicalProjectSet};

/// Transpose `LogicalApply` and `LogicalProjectSet`.
///
/// Before:
///
/// ```text
///     LogicalApply
///    /            \
///  Domain      LogicalProjectSet
///                  |
///                Input
/// ```
///
/// After:
///
/// ```text
///     LogicalProject (reorder)
///           |
///    LogicalProjectSet
///          |
///    LogicalApply
///    /            \
///  Domain        Input
/// ```
pub struct ApplyProjectSetTransposeRule {}
impl Rule for ApplyProjectSetTransposeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply: &LogicalApply = plan.as_logical_apply()?;
        let (left, right, on, join_type, correlated_id, correlated_indices, max_one_row) =
            apply.clone().decompose();
        let project_set: &LogicalProjectSet = right.as_logical_project_set()?;
        let left_schema_len = left.schema().len();
        assert_eq!(join_type, JoinType::Inner);

        // Insert all the columns of `LogicalApply`'s left at the beginning of the new
        // `LogicalProjectSet`.
        let mut exprs: Vec<ExprImpl> = left
            .schema()
            .data_types()
            .into_iter()
            .enumerate()
            .map(|(index, data_type)| InputRef::new(index, data_type).into())
            .collect();

        let (proj_exprs, proj_input) = project_set.clone().decompose();

        // replace correlated_input_ref in project exprs
        let mut rewriter =
            ApplyOffsetRewriter::new(left.schema().len(), &correlated_indices, correlated_id);

        let new_proj_exprs: Vec<ExprImpl> = proj_exprs
            .into_iter()
            .map(|expr| rewriter.rewrite_expr(expr))
            .collect_vec();

        exprs.extend(new_proj_exprs.clone());

        let mut rewriter =
            ApplyOnCondRewriterForProjectSet::new(left.schema().len(), new_proj_exprs);
        let new_on = on.rewrite_expr(&mut rewriter);

        if rewriter.refer_table_function {
            // The join on condition refers to the table function column of the `project_set` which
            // cannot be unnested.
            return None;
        }

        let new_apply = LogicalApply::create(
            left,
            proj_input,
            join_type,
            new_on,
            correlated_id,
            correlated_indices,
            max_one_row,
        );

        let new_project_set = LogicalProjectSet::create(new_apply, exprs);

        // Since `project_set` has a field `projected_row_id` in its left most column, we need a
        // project to reorder it to align the schema type.
        let out_col_idxs = (1..=left_schema_len)
            .chain(vec![0])
            .chain((left_schema_len + 1)..new_project_set.schema().len());
        let reorder_project = LogicalProject::with_out_col_idx(new_project_set, out_col_idxs);

        Some(reorder_project.into())
    }
}

impl ApplyProjectSetTransposeRule {
    pub fn create() -> BoxedRule {
        Box::new(ApplyProjectSetTransposeRule {})
    }
}

pub struct ApplyOnCondRewriterForProjectSet {
    pub left_input_len: usize,
    pub mapping: Vec<ExprImpl>,
    pub refer_table_function: bool,
}

impl ApplyOnCondRewriterForProjectSet {
    pub fn new(left_input_len: usize, mapping: Vec<ExprImpl>) -> Self {
        Self {
            left_input_len,
            mapping,
            refer_table_function: false,
        }
    }
}

impl ExprRewriter for ApplyOnCondRewriterForProjectSet {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        if input_ref.index >= self.left_input_len {
            // We need to minus 1 to align `projected_row_id` field in `project_set`.
            let expr = self.mapping[input_ref.index() - self.left_input_len - 1].clone();
            if matches!(expr, ExprImpl::TableFunction(_)) {
                self.refer_table_function = true;
            }
            expr
        } else {
            input_ref.into()
        }
    }
}

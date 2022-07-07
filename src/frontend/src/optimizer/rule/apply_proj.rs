// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_pb::plan_common::JoinType;

use super::{BoxedRule, Rule};
use crate::expr::{ExprImpl, ExprRewriter, InputRef};
use crate::optimizer::plan_node::{LogicalProject, PlanTreeNodeBinary, PlanTreeNodeUnary};
use crate::optimizer::PlanRef;
use crate::utils::ColIndexMapping;

/// Push `LogicalApply` down `LogicalProject`.
pub struct ApplyProj {}
impl Rule for ApplyProj {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply = plan.as_logical_apply()?;
        assert_eq!(apply.join_type(), JoinType::Inner);
        let right = apply.right();
        let project = right.as_logical_project()?;

        // Insert all the columns of `LogicalApply`'s left at the beginning of the new
        // `LogicalProject`.
        let mut exprs: Vec<ExprImpl> = apply
            .left()
            .schema()
            .data_types()
            .into_iter()
            .enumerate()
            .map(|(index, data_type)| InputRef::new(index, data_type).into())
            .collect();

        let (proj_exprs, proj_input) = project.clone().decompose();
        let mut col_mapping = ColIndexMapping::with_shift_offset(
            project.input().schema().len(),
            apply.left().schema().len() as isize,
        );
        exprs.extend(
            proj_exprs
                .into_iter()
                .map(|expr| col_mapping.rewrite_expr(expr)),
        );

        let new_apply = apply.clone_with_left_right(apply.left(), proj_input);
        let new_project = LogicalProject::create(new_apply.into(), exprs);
        Some(new_project)
    }
}

impl ApplyProj {
    pub fn create() -> BoxedRule {
        Box::new(ApplyProj {})
    }
}

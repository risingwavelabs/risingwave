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

use super::super::plan_node::*;
use super::{BoxedRule, LiftCorrelatedInputRef, Rule};
use crate::expr::{ExprImpl, ExprRewriter, InputRef};
use crate::utils::ColIndexMapping;

pub struct ApplyProjectRule {}
impl Rule for ApplyProjectRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply = plan.as_logical_apply()?;
        let right = apply.right();
        let project = right.as_logical_project()?;
        let new_right = project.input();

        println!("ApplyProjectRule is used");

        let mut lift_correlated_input_ref = LiftCorrelatedInputRef {};
        let mut shift_input_ref = ColIndexMapping::with_shift_offset(
            project.input().schema().len(),
            apply.left().schema().len() as isize,
        );

        // All the columns in the left.
        let mut exprs: Vec<ExprImpl> = apply
            .left()
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| InputRef::new(i, field.data_type()).into())
            .collect();
        // Extend with the project columns in the right.
        exprs.extend(project.exprs().clone().into_iter().map(|mut expr| {
            expr = lift_correlated_input_ref.rewrite_expr(expr);
            expr = shift_input_ref.rewrite_expr(expr);
            expr
        }));
        let mut expr_alias = vec![None; apply.left().schema().len()];
        expr_alias.extend(project.expr_alias().iter().cloned());

        let new_apply = apply.clone_with_left_right(apply.left(), new_right);
        let lifted_project: PlanRef =
            LogicalProject::new(new_apply.into(), exprs, expr_alias).into();
        Some(lifted_project)
    }
}

impl ApplyProjectRule {
    pub fn create() -> BoxedRule {
        Box::new(ApplyProjectRule {})
    }
}

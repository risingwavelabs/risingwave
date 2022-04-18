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

use risingwave_expr::expr::AggKind;

use super::{BoxedRule, Rule};
use crate::expr::{ExprImpl, ExprRewriter, InputRef};
use crate::optimizer::plan_node::{
    LogicalAgg, LogicalProject, PlanTreeNodeBinary, PlanTreeNodeUnary,
};
use crate::optimizer::PlanRef;
use crate::utils::ColIndexMapping;

/// This rule is for pattern: Apply->Project->Agg, and it will be converted into
/// Project->Agg->Apply.
/// Scalar agg will be converted into group agg using all columns of apply's
/// left child, which is different from original formula.
/// Project will have all columns of Apply's left child at its beginning.
/// Please note that Project's input is Agg here, so we don't have to
/// worry about edge cases of pulling up Project.
pub struct ApplyProjAggRule {}
impl Rule for ApplyProjAggRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply = plan.as_logical_apply()?;
        let apply_left_len = apply.left().schema().fields().len();

        let right = apply.right();
        let project = right.as_logical_project()?;

        let input = project.input();
        let agg = input.as_logical_agg()?;

        // Use agg.input as apply's right.
        let new_apply = apply.clone_with_left_right(apply.left(), agg.input());

        // To pull LogicalAgg up on top of LogicalApply, we need to convert scalar agg to group agg
        // using pks of apply.left as its group keys and convert count(*) to count(pk).
        let (mut agg_calls, agg_call_alias, mut group_keys, input) = agg.clone().decompose();
        // We currently only support scalar agg in correlated subquery.
        assert!(group_keys.is_empty());

        // We use all columns of apply's left child as pks here, which is different from original
        // formula.
        group_keys.extend(0..apply.left().schema().fields().len());

        if let Some(count) = agg_calls.iter_mut().find(|agg_call| {
            matches!(agg_call.agg_kind, AggKind::Count) && agg_call.inputs.is_empty()
        }) {
            let input_ref = InputRef::new(input.pk_indices()[0], count.return_type.clone());
            count.inputs.push(input_ref);
        }
        // Shift index of agg_calls' input_ref with offset.
        agg_calls.iter_mut().for_each(|agg_call| {
            agg_call.inputs.iter_mut().for_each(|input_ref| {
                input_ref.shift_with_offset(apply_left_len as isize);
            });
        });
        let agg = LogicalAgg::new(agg_calls, agg_call_alias, group_keys, new_apply.into());

        // Columns of old apply's left child should be in the left.
        let mut exprs: Vec<ExprImpl> = apply
            .left()
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| InputRef::new(i, field.data_type()).into())
            .collect();
        let mut expr_alias = vec![None; apply.left().schema().len()];

        let mut shift_input_ref =
            ColIndexMapping::with_shift_offset(agg.agg_calls().len(), apply_left_len as isize);
        // Extend with the project columns in the right.
        exprs.extend(project.exprs().clone().into_iter().map(|expr| {
            // We currently assume that there is no correlated variable in LogicalProject.
            shift_input_ref.rewrite_expr(expr)
        }));
        expr_alias.extend(project.expr_alias().iter().cloned());

        let project = LogicalProject::new(agg.into(), exprs, expr_alias);

        Some(project.into())
    }
}

impl ApplyProjAggRule {
    pub fn create() -> BoxedRule {
        Box::new(ApplyProjAggRule {})
    }
}

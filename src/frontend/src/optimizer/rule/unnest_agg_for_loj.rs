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

use risingwave_common::types::DataType;
use risingwave_expr::expr::AggKind;
use risingwave_pb::plan_common::JoinType;

use super::{BoxedRule, Rule};
use crate::expr::{ExprImpl, ExprRewriter, InputRef};
use crate::optimizer::plan_node::{
    LogicalAgg, LogicalProject, PlanTreeNodeBinary, PlanTreeNodeUnary,
};
use crate::optimizer::PlanRef;
use crate::utils::ColIndexMapping;

/// This rule is for pattern: Apply->Project(p1)->Agg->Project(p2), and it will be converted into
/// Project(p1')->Agg->Apply->Project(p2').
///
/// Scalar agg will be converted into group agg using pks of Apply's left child, while we use
/// all columns of Apply's left child here in order to align with Project. Besides, count(*) will be
/// converted to count(pk).
///
/// Project p1' will have all columns of Apply's left child at its beginning.
///
/// Project p2' will have one constant column at the end of its `exprs`.
///
/// Please note that the input of Project p1 is Agg here, so we don't have to
/// worry about edge cases of pulling up Project.
pub struct UnnestAggForLOJ {}
impl Rule for UnnestAggForLOJ {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply = plan.as_logical_apply()?;
        if apply.join_type() != JoinType::LeftOuter {
            return None;
        }
        let apply_left_len = apply.left().schema().fields().len();

        let right = apply.right();
        let project = right.as_logical_project()?;

        let input = project.input();
        let agg = input.as_logical_agg()?;

        let input = agg.input();
        let input_project = input.as_logical_project()?;
        let (mut exprs, mut expr_alias, input) = input_project.clone().decompose();

        // Add a constant column at the end of `input_project`.
        exprs.push(ExprImpl::literal_int(1));
        expr_alias.push(Some("1".into()));
        let idx_of_constant = exprs.len() - 1;
        let new_project = LogicalProject::new(input, exprs, expr_alias);

        let new_apply = apply.clone_with_left_right(apply.left(), new_project.into());

        // To pull LogicalAgg up on top of LogicalApply, we need to convert scalar agg to group agg
        // using pks of Apply.left as its group keys and convert count(*) to count(pk).
        let (mut agg_calls, mut group_keys, _) = agg.clone().decompose();
        // TODO: currently only scalar agg is supported in correlated subquery.
        if !group_keys.is_empty() {
            return None;
        }

        // We use all columns of Apply's left child as group keys here,.
        group_keys.extend(0..apply_left_len);

        agg_calls
            .iter_mut()
            .filter(|agg_call| {
                matches!(agg_call.agg_kind, AggKind::Count) && agg_call.inputs.is_empty()
            })
            .for_each(|count| {
                let input_ref = InputRef::new(idx_of_constant, DataType::Int32);
                count.inputs.push(input_ref);
            });

        // Shift index of agg_calls' input_ref with `apply_left_len`.
        agg_calls.iter_mut().for_each(|agg_call| {
            agg_call.inputs.iter_mut().for_each(|input_ref| {
                input_ref.shift_with_offset(apply_left_len as isize);
            });
        });
        let agg = LogicalAgg::new(agg_calls, group_keys, new_apply.into());

        // Columns of old Apply's left child should be in the left.
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

impl UnnestAggForLOJ {
    pub fn create() -> BoxedRule {
        Box::new(UnnestAggForLOJ {})
    }
}

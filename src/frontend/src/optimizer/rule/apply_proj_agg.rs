use risingwave_expr::expr::AggKind;

use super::{BoxedRule, Rule};
use crate::expr::{ExprImpl, ExprRewriter, InputRef};
use crate::optimizer::plan_node::{
    LogicalAgg, LogicalProject, PlanTreeNodeBinary, PlanTreeNodeUnary,
};
use crate::optimizer::PlanRef;
use crate::utils::ColIndexMapping;

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
        // We currently on support scalar agg in correlated subquery.
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
        // Rewrite!
        // Shift index of agg_calls' input_ref with offset.
        agg_calls.iter_mut().for_each(|agg_call| {
            agg_call.inputs.iter_mut().for_each(|input_ref| {
                input_ref.index += apply_left_len;
            });
        });

        let agg = LogicalAgg::new(agg_calls, agg_call_alias, group_keys, new_apply.into());

        // Old apply's left child's columns should be in the left.
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

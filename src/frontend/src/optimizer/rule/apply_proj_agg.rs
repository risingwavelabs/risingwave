use super::super::plan_node::*;
use super::Rule;
use crate::optimizer::PlanRef;

pub struct ApplyProjAggRule {}
impl Rule for ApplyProjAggRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply = plan.as_logical_apply()?;

        let right = apply.right();
        let project = right.as_logical_project()?;

        let input = project.input();
        let agg = input.as_logical_agg()?;

        // Pull LogicalProject and LogicalAgg up on top of LogicalApply.
        // Convert ScalarAgg to GroupAgg whose group keys are the pk of apply.left.
        // Convert count(*) to count(c), c is any column of pk.

        None
    }
}

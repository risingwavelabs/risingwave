use crate::optimizer::PlanRef;
use super::Rule;
use super::super::plan_node::*;

pub struct ApplyProjFilterRule {}
impl Rule for ApplyProjFilterRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply = plan.as_logical_apply()?;
        let right = apply.right();
        let project = right.as_logical_project()?;
        let filter = project.input().as_logical_filter()?;
        
        // Remove expressions contain correlated variables from LogicalFilter.

        // Add columns involved in expressions removed from LogicalFilter to LogicalProject.

        // Merge these expressions with LogicalApply into LogicalJoin. 

    }
}
use super::super::plan_node::*;
use super::Rule;
struct ProjectJoinRule {}
impl Rule for ProjectJoinRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let project = plan.as_logical_project()?;
        let _join = project.child().clone().as_logical_join()?;
        todo!()
    }
}

use super::super::plan_node::*;
use super::{BoxedRule, Rule};
use crate::utils::Substitute;

/// Pushes a [`LogicalFilter`] past a [`LogicalProject`].
pub struct FilterProjectRule {}
impl Rule for FilterProjectRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let filter = plan.as_logical_filter()?;
        let input = filter.input();
        let project = input.as_logical_project()?;

        // convert the predicate to one that references the child of the project
        let mut subst = Substitute {
            mapping: project.exprs().clone(),
        };
        let predicate = filter.predicate().clone().rewrite_expr(&mut subst);

        let input = project.input();
        let pushed_filter = LogicalFilter::new(input, predicate);
        Some(project.clone_with_input(pushed_filter.into()).into())
    }
}

impl FilterProjectRule {
    pub fn create() -> BoxedRule {
        Box::new(FilterProjectRule {})
    }
}

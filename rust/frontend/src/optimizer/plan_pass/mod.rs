//! Define all passes of plan tree, which traverse or rewrite a plan tree
use super::property::{Distribution, Order};
use super::PlanRef;

mod plan_rewriter;
pub use plan_rewriter::*;
mod plan_visitor;
pub use plan_visitor::*;
mod heuristic;
pub use heuristic::*;

/// `PlanPass` receive a plan tree and transform it to a new one.
trait PlanPass {
    fn pass(&mut self, plan: PlanRef) -> PlanRef {
        self.pass_with_require(plan, Order::any(), Distribution::any())
    }
    fn pass_with_require(
        &mut self,
        plan: PlanRef,
        required_order: &Order,
        required_dist: &Distribution,
    ) -> PlanRef;
}

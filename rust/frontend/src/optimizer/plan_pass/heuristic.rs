use itertools::Itertools;

use super::PlanPass;
use crate::optimizer::property::{Distribution, Order};
use crate::optimizer::rule::BoxedRule;
use crate::optimizer::PlanRef;

/// Traverse order of [`HeuristicOptimizer`]
#[allow(dead_code)]
pub enum ApplyOrder {
    TopDown,
    BottomUp,
}

impl Default for ApplyOrder {
    fn default() -> Self {
        ApplyOrder::TopDown
    }
}

// TODO: we should have a builder of HeuristicOptimizer here
/// A rule-based heuristic optimizer, which traverses every plan nodes and tries to
/// apply each rule on them.
pub struct HeuristicOptimizer {
    apply_order: ApplyOrder,
    rules: Vec<BoxedRule>,
}

impl HeuristicOptimizer {
    pub fn new(rules: Vec<BoxedRule>) -> Self {
        Self {
            apply_order: Default::default(),
            rules,
        }
    }

    fn optimize_self_node(&mut self, mut plan: PlanRef) -> PlanRef {
        for rule in &self.rules {
            if let Some(applied) = rule.apply(plan.clone()) {
                plan = applied;
            }
        }
        plan
    }

    fn optimize_inputs(&mut self, plan: PlanRef) -> PlanRef {
        let order_required = plan.inputs_order_required();
        let dists_required = plan.inputs_distribution_required();

        let inputs = plan
            .inputs()
            .into_iter()
            .zip_eq(order_required.into_iter())
            .zip_eq(dists_required.into_iter())
            .map(|((sub_tree, order), dist)| self.pass_with_require(sub_tree, order, dist))
            .collect_vec();
        plan.clone_with_inputs(&inputs)
    }
}

impl PlanPass for HeuristicOptimizer {
    fn pass_with_require(
        &mut self,
        mut plan: PlanRef,
        required_order: &Order,
        required_dist: &Distribution,
    ) -> PlanRef {
        plan = match self.apply_order {
            ApplyOrder::TopDown => {
                plan = self.optimize_self_node(plan);
                self.optimize_inputs(plan)
            }
            ApplyOrder::BottomUp => {
                plan = self.optimize_inputs(plan);
                self.optimize_self_node(plan)
            }
        };
        plan = required_order.enforce_if_not_satisfies(plan);
        required_dist.enforce_if_not_satisfies(plan, required_order)
    }
}

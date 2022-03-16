use itertools::Itertools;

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

    fn optimize_node(&self, mut plan: PlanRef) -> PlanRef {
        for rule in &self.rules {
            if let Some(applied) = rule.apply(plan.clone()) {
                plan = applied;
            }
        }
        plan
    }

    fn optimize_inputs(&self, plan: PlanRef) -> PlanRef {
        let inputs = plan
            .inputs()
            .into_iter()
            .map(|sub_tree| self.optimize(sub_tree))
            .collect_vec();
        plan.clone_with_inputs(&inputs)
    }

    pub fn optimize(&self, mut plan: PlanRef) -> PlanRef {
        match self.apply_order {
            ApplyOrder::TopDown => {
                plan = self.optimize_node(plan);
                self.optimize_inputs(plan)
            }
            ApplyOrder::BottomUp => {
                plan = self.optimize_inputs(plan);
                self.optimize_node(plan)
            }
        }
    }
}

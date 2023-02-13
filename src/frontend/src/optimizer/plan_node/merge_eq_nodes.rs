use std::hash::Hash;

use super::PlanRef;

pub trait Semantics<V: Hash + Eq> {
    fn semantics(&self, inputs: Vec<V>) -> V;
}

impl Semantics<PlanRef> for PlanRef {
    fn semantics(&self, _: Vec<PlanRef>) -> PlanRef {
        self.clone()
    }
}

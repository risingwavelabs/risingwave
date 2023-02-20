use std::collections::HashMap;
use std::hash::Hash;

use super::{LogicalShare, PlanNodeId, PlanRef, PlanTreeNodeUnary};
use crate::utils::{Endo, Layer, Visit};

pub trait Semantics<V: Hash + Eq> {
    fn semantics(&self) -> V;
}

impl Semantics<PlanRef> for PlanRef {
    fn semantics(&self) -> PlanRef {
        self.clone()
    }
}

impl PlanRef {
    pub fn merge_eq_nodes<V: Hash + Eq>(self) -> PlanRef
    where
        PlanRef: Semantics<V>,
    {
        Merger::default().apply(self)
    }
}

struct Merger<V: Hash + Eq> {
    cache: HashMap<V, LogicalShare>,
}

impl<V: Hash + Eq> Default for Merger<V> {
    fn default() -> Self {
        Merger {
            cache: Default::default(),
        }
    }
}

impl<V: Hash + Eq> Endo<PlanRef> for Merger<V>
where
    PlanRef: Semantics<V>,
{
    fn apply(&mut self, t: PlanRef) -> PlanRef {
        let semantics = t.semantics();
        let share = self.cache.get(&semantics).cloned().unwrap_or_else(|| {
            let share = LogicalShare::new(t.map(|i| self.apply(i)));
            self.cache.entry(semantics).or_insert(share).clone()
        });
        share.into()
    }
}

impl PlanRef {
    pub fn prune_share(&self) -> PlanRef {
        let mut counter = Counter::default();
        counter.visit(self);
        counter.apply(self.clone())
    }
}

#[derive(Default)]
struct Counter {
    counts: HashMap<PlanNodeId, u64>,
}

impl Visit<PlanRef> for Counter {
    fn pre(&mut self, t: &PlanRef) {
        t.as_logical_share().map(|s| {
            self.counts
                .entry(s.id())
                .and_modify(|c| *c += 1)
                .or_insert(1)
        });
    }
}

impl Endo<PlanRef> for Counter {
    fn pre(&mut self, t: PlanRef) -> PlanRef {
        match t.as_logical_share() {
            Some(s) if *self.counts.get(&s.id()).unwrap() == 1 => s.input(),
            _ => t,
        }
    }
}

// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::hash::Hash;

use super::{EndoPlan, LogicalShare, PlanNodeId, PlanRef, PlanTreeNodeUnary, VisitPlan};
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
        counter.to_pruner().apply(self.clone())
    }
}

#[derive(Default)]
struct Counter {
    counts: HashMap<PlanNodeId, u64>,
}

impl Counter {
    fn to_pruner<'a>(&'a self) -> Pruner<'a> {
        Pruner {
            counts: &self.counts,
            cache: HashMap::new(),
        }
    }
}

impl VisitPlan for Counter {
    fn visited(&self, t: &PlanRef) -> bool {
        self.counts.get(&t.id()).is_some_and(|c| *c > 1)
    }
}

impl Visit<PlanRef> for Counter {
    fn visit(&mut self, t: &PlanRef) {
        if let Some(s) = t.as_logical_share() {
            self.counts
                .entry(s.id())
                .and_modify(|c| *c += 1)
                .or_insert(1);
        }
        self.dag_visit(t);
    }
}

struct Pruner<'a> {
    counts: &'a HashMap<PlanNodeId, u64>,
    cache: HashMap<PlanRef, PlanRef>,
}

impl EndoPlan for Pruner<'_> {
    fn cache(&mut self) -> &mut HashMap<PlanRef, PlanRef> {
        &mut self.cache
    }
}

impl Endo<PlanRef> for Pruner<'_> {
    fn pre(&mut self, t: PlanRef) -> PlanRef {
        let prunable = |s: &LogicalShare| {
            *self.counts.get(&s.id()).expect("Unprocessed shared node.") == 1
                || s.input().as_logical_scan().is_some()
        };
        t.as_logical_share()
            .cloned()
            .filter(prunable)
            .map_or(t, |s| s.input())
    }

    fn apply(&mut self, t: PlanRef) -> PlanRef {
        self.dag_apply(t)
    }
}

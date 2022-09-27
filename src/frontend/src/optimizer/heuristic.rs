// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt;

use itertools::Itertools;

use crate::optimizer::rule::BoxedRule;
use crate::optimizer::PlanRef;

/// Traverse order of [`HeuristicOptimizer`]
pub enum ApplyOrder {
    TopDown,
    BottomUp,
}

// TODO: we should have a builder of HeuristicOptimizer here
/// A rule-based heuristic optimizer, which traverses every plan nodes and tries to
/// apply each rule on them.
pub struct HeuristicOptimizer<'a> {
    apply_order: &'a ApplyOrder,
    rules: &'a Vec<BoxedRule>,
    stats: Stats,
}

impl<'a> HeuristicOptimizer<'a> {
    pub fn new(apply_order: &'a ApplyOrder, rules: &'a Vec<BoxedRule>) -> Self {
        Self {
            apply_order,
            rules,
            stats: Stats::new(),
        }
    }

    fn optimize_node(&mut self, mut plan: PlanRef) -> PlanRef {
        for rule in self.rules {
            if let Some(applied) = rule.apply(plan.clone()) {
                plan = applied;
                self.stats.count_rule(rule);
            }
        }
        plan
    }

    fn optimize_inputs(&mut self, plan: PlanRef) -> PlanRef {
        let inputs = plan
            .inputs()
            .into_iter()
            .map(|sub_tree| self.optimize(sub_tree))
            .collect_vec();
        plan.clone_with_inputs(&inputs)
    }

    pub fn optimize(&mut self, mut plan: PlanRef) -> PlanRef {
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

    pub fn get_stats(&self) -> &Stats {
        &self.stats
    }
}

pub struct Stats {
    rule_counter: HashMap<String, u32>,
}

impl Stats {
    pub fn new() -> Self {
        Self {
            rule_counter: HashMap::new(),
        }
    }

    pub fn count_rule(&mut self, rule: &BoxedRule) {
        match self.rule_counter.entry(rule.description().to_string()) {
            Entry::Occupied(mut entry) => {
                *entry.get_mut() += 1;
            }
            Entry::Vacant(entry) => {
                entry.insert(1);
            }
        }
    }

    pub fn has_applied_rule(&self) -> bool {
        !self.rule_counter.is_empty()
    }
}

impl fmt::Display for Stats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (rule, count) in self.rule_counter.iter() {
            writeln!(f, "apply {} {} time(s)", rule, count)?;
        }
        Ok(())
    }
}

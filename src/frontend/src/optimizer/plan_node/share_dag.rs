// Copyright 2026 RisingWave Labs
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

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;

use super::{LogicalPlanRef as PlanRef, LogicalShare, PlanTreeNodeUnary, ShareNode};
use crate::optimizer::ShareId;
use crate::optimizer::plan_visitor::{PlanVisitor, ShareParentCounter};

/// A requirement contributed by one parent of a share.
pub trait ShareRequirement: Clone + Debug {
    fn merge(requirements: Vec<Self>) -> Self;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShareDagPhase {
    Collect,
    Rebuild,
    Adapt,
}

/// Shared state for optimizer passes that must collect requirements from every parent before
/// rebuilding a shared input.
#[derive(Clone, Debug)]
pub struct ShareDagContext<R: ShareRequirement> {
    requirements: HashMap<ShareId, Vec<R>>,
    merged_requirements: HashMap<ShareId, R>,
    original_inputs: HashMap<ShareId, PlanRef>,
    discovery_order: Vec<ShareId>,
    parent_counter: ShareParentCounter,
    phase: ShareDagPhase,
    running: bool,
}

impl<R: ShareRequirement> ShareDagContext<R> {
    pub fn new(root: PlanRef) -> Self {
        let mut parent_counter = ShareParentCounter::default();
        parent_counter.visit(root);
        Self {
            requirements: HashMap::new(),
            merged_requirements: HashMap::new(),
            original_inputs: HashMap::new(),
            discovery_order: Vec::new(),
            parent_counter,
            phase: ShareDagPhase::Collect,
            running: false,
        }
    }

    pub fn reset(&mut self, root: PlanRef) {
        *self = Self::new(root);
        self.running = true;
    }

    pub fn is_running(&self) -> bool {
        self.running
    }

    pub fn finish(&mut self) {
        self.running = false;
    }

    pub fn phase(&self) -> ShareDagPhase {
        self.phase
    }

    pub fn set_phase(&mut self, phase: ShareDagPhase) {
        self.phase = phase;
    }

    pub fn parent_num(&self, share: &LogicalShare) -> usize {
        self.parent_counter.get_parent_num(share)
    }

    /// Records one parent requirement. Returns the merged requirement when the last parent has
    /// reported, which is the point where collection may continue through this share's input.
    pub fn record_requirement(&mut self, share: &LogicalShare, requirement: R) -> Option<R> {
        let share_id = share.share_id();
        if let std::collections::hash_map::Entry::Vacant(entry) =
            self.original_inputs.entry(share_id)
        {
            entry.insert(share.input());
            self.discovery_order.push(share_id);
        }

        let requirements = self.requirements.entry(share_id).or_default();
        requirements.push(requirement);
        let parent_num = self.parent_counter.get_parent_num_by_id(share_id);
        assert!(
            requirements.len() <= parent_num,
            "share {share_id:?} received more requirements than parents"
        );
        if requirements.len() != parent_num {
            return None;
        }

        let merged = R::merge(requirements.clone());
        self.merged_requirements
            .try_insert(share_id, merged.clone())
            .expect("a share requirement must be merged once");
        Some(merged)
    }

    pub fn merged_requirement(&self, share_id: ShareId) -> R {
        self.merged_requirements
            .get(&share_id)
            .unwrap_or_else(|| panic!("share {share_id:?} has no merged requirement"))
            .clone()
    }

    pub fn original_input(&self, share_id: ShareId) -> PlanRef {
        self.original_inputs
            .get(&share_id)
            .unwrap_or_else(|| panic!("share {share_id:?} has no captured input"))
            .clone()
    }

    /// Returns shared inputs in dependency order, with the deepest shares first.
    pub fn rebuild_order(&self) -> Vec<ShareId> {
        assert_eq!(
            self.requirements.len(),
            self.merged_requirements.len(),
            "all discovered shares must receive requirements from every parent"
        );

        let mut visiting = HashSet::new();
        let mut visited = HashSet::new();
        let mut order = Vec::with_capacity(self.discovery_order.len());
        for &share_id in &self.discovery_order {
            self.visit_share_dependencies(share_id, &mut visiting, &mut visited, &mut order);
        }
        order
    }

    fn visit_share_dependencies(
        &self,
        share_id: ShareId,
        visiting: &mut HashSet<ShareId>,
        visited: &mut HashSet<ShareId>,
        order: &mut Vec<ShareId>,
    ) {
        if visited.contains(&share_id) {
            return;
        }
        assert!(
            visiting.insert(share_id),
            "cycle detected in share dependencies at {share_id:?}"
        );

        let mut dependencies = Vec::new();
        let mut seen_dependencies = HashSet::new();
        let mut seen_single_parent_shares = HashSet::new();
        self.collect_direct_dependencies(
            self.original_input(share_id),
            &mut dependencies,
            &mut seen_dependencies,
            &mut seen_single_parent_shares,
        );
        for dependency in dependencies {
            self.visit_share_dependencies(dependency, visiting, visited, order);
        }

        visiting.remove(&share_id);
        visited.insert(share_id);
        order.push(share_id);
    }

    fn collect_direct_dependencies(
        &self,
        plan: PlanRef,
        dependencies: &mut Vec<ShareId>,
        seen_dependencies: &mut HashSet<ShareId>,
        seen_single_parent_shares: &mut HashSet<ShareId>,
    ) {
        if let Some(share) = plan.as_logical_share() {
            let share_id = share.share_id();
            if self.merged_requirements.contains_key(&share_id) {
                if seen_dependencies.insert(share_id) {
                    dependencies.push(share_id);
                }
            } else if seen_single_parent_shares.insert(share_id) {
                self.collect_direct_dependencies(
                    share.input(),
                    dependencies,
                    seen_dependencies,
                    seen_single_parent_shares,
                );
            }
            return;
        }

        for input in plan.inputs() {
            self.collect_direct_dependencies(
                input,
                dependencies,
                seen_dependencies,
                seen_single_parent_shares,
            );
        }
    }
}

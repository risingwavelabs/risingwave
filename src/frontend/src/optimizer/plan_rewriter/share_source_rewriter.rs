// Copyright 2025 RisingWave Labs
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

use crate::catalog::SourceId;
use crate::optimizer::PlanVisitor;
use crate::optimizer::plan_node::{
    Logical, LogicalPlanRef as PlanRef, LogicalShare, LogicalSource,
};
use crate::optimizer::plan_rewriter::PlanRewriter;
use crate::optimizer::plan_visitor::{DefaultBehavior, DefaultValue, LogicalPlanVisitor};

#[derive(Debug, Clone, Default)]
pub struct ShareSourceRewriter {
    /// Source id need to be shared.
    share_ids: HashSet<SourceId>,
    /// Source id to share node.
    share_source: HashMap<SourceId, PlanRef>,
}

#[derive(Debug, Clone, Default)]
struct SourceCounter {
    /// Source id to count.
    source_counter: HashMap<SourceId, usize>,
}

impl ShareSourceRewriter {
    pub fn share_source(plan: PlanRef) -> PlanRef {
        // Find which sources occurred more than once.
        let mut source_counter = SourceCounter::default();
        source_counter.visit(plan.clone());

        let mut share_source_rewriter = ShareSourceRewriter {
            share_ids: source_counter
                .source_counter
                .into_iter()
                .filter(|(_, v)| *v > 1)
                .map(|(k, _)| k)
                .collect(),
            share_source: Default::default(),
        };
        // Rewrite source to share source
        plan.rewrite_with(&mut share_source_rewriter)
    }

    fn rewrite_logical_source(&mut self, source: &LogicalSource) -> PlanRef {
        let source_id = match &source.core.catalog {
            Some(s) => s.id,
            None => {
                return source.clone().into();
            }
        };
        if !self.share_ids.contains(&source_id) {
            let source_ref = source.clone().into();
            return source_ref;
        }
        match self.share_source.get(&source_id) {
            None => {
                let source_ref = source.clone().into();
                let share_source = LogicalShare::create(source_ref);
                self.share_source.insert(source_id, share_source.clone());
                share_source
            }
            Some(share_source) => share_source.clone(),
        }
    }
}

impl PlanRewriter<Logical> for ShareSourceRewriter {
    fn rewrite_with_inputs(&mut self, plan: &PlanRef, inputs: Vec<PlanRef>) -> PlanRef {
        if let Some(source) = plan.as_logical_source() {
            self.rewrite_logical_source(source)
        } else {
            plan.clone_root_with_inputs(&inputs)
        }
    }
}

impl LogicalPlanVisitor for SourceCounter {
    type Result = ();

    type DefaultBehavior = impl DefaultBehavior<Self::Result>;

    fn default_behavior() -> Self::DefaultBehavior {
        DefaultValue
    }

    fn visit_logical_source(&mut self, source: &LogicalSource) {
        if let Some(source) = &source.core.catalog {
            self.source_counter
                .entry(source.id)
                .and_modify(|count| *count += 1)
                .or_insert(1);
        }
    }
}

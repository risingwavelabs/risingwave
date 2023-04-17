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

use std::collections::{HashMap, HashSet};

use itertools::Itertools;

use crate::catalog::SourceId;
use crate::optimizer::plan_node::{
    LogicalShare, LogicalSource, PlanNodeId, PlanTreeNode, StreamShare,
};
use crate::optimizer::plan_visitor::{DefaultBehavior, Merge};
use crate::optimizer::{PlanRewriter, PlanVisitor};
use crate::PlanRef;

#[derive(Debug, Clone, Default)]
pub struct ShareSourceRewriter {
    /// Source id need to be shared.
    share_ids: HashSet<SourceId>,
    /// Source id to share node.
    share_source: HashMap<SourceId, PlanRef>,
    /// Original share node plan id to new share node.
    /// Rewriter will rewrite all nodes, but we need to keep the shape of the DAG.
    share_map: HashMap<PlanNodeId, PlanRef>,
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
            share_map: Default::default(),
        };
        // Rewrite source to share source
        share_source_rewriter.rewrite(plan)
    }
}

impl PlanRewriter for ShareSourceRewriter {
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

    fn rewrite_logical_share(&mut self, share: &LogicalShare) -> PlanRef {
        // When we use the plan rewriter, we need to take care of the share operator,
        // because our plan is a DAG rather than a tree.
        match self.share_map.get(&share.id()) {
            None => {
                let new_inputs = share
                    .inputs()
                    .into_iter()
                    .map(|input| self.rewrite(input))
                    .collect_vec();
                let new_share = share.clone_with_inputs(&new_inputs);
                self.share_map.insert(share.id(), new_share.clone());
                new_share
            }
            Some(new_share) => new_share.clone(),
        }
    }

    fn rewrite_stream_share(&mut self, _share: &StreamShare) -> PlanRef {
        // We only access logical node here, so stream share is unreachable.
        unreachable!()
    }
}

impl PlanVisitor<()> for SourceCounter {
    fn default_behavior() -> impl DefaultBehavior<()> {
        Merge(|_, _| ())
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

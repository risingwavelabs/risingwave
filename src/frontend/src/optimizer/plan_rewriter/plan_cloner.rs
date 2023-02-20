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

use itertools::Itertools;

use crate::optimizer::plan_node::{LogicalShare, PlanNodeId, PlanTreeNode, StreamShare};
use crate::optimizer::PlanRewriter;
use crate::PlanRef;

#[derive(Debug, Clone, Default)]
pub struct PlanCloner {
    /// Original share node plan id to new share node.
    /// Rewriter will rewrite all nodes, but we need to keep the shape of the DAG.
    share_map: HashMap<PlanNodeId, PlanRef>,
}

impl PlanCloner {
    pub fn clone_whole_plan(plan: PlanRef) -> PlanRef {
        let mut plan_cloner = PlanCloner {
            share_map: Default::default(),
        };
        plan_cloner.rewrite(plan)
    }
}

impl PlanRewriter for PlanCloner {
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

    fn rewrite_stream_share(&mut self, share: &StreamShare) -> PlanRef {
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
}

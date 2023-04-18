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

use super::{DefaultBehavior, DefaultValue};
use crate::optimizer::plan_node::{LogicalShare, PlanNodeId, PlanTreeNodeUnary};
use crate::optimizer::plan_visitor::PlanVisitor;

#[derive(Debug, Clone, Default)]
pub struct ShareParentCounter {
    /// Plan node id to parent number mapping.
    parent_counter: HashMap<PlanNodeId, usize>,
}

impl ShareParentCounter {
    pub fn get_parent_num(&self, share: &LogicalShare) -> usize {
        *self
            .parent_counter
            .get(&share.id())
            .expect("share must exist")
    }
}

impl PlanVisitor<()> for ShareParentCounter {
    type DefaultBehavior = impl DefaultBehavior<()>;

    fn default_behavior() -> Self::DefaultBehavior {
        DefaultValue
    }

    fn visit_logical_share(&mut self, share: &LogicalShare) {
        let v = self
            .parent_counter
            .entry(share.id())
            .and_modify(|counter| *counter += 1)
            .or_insert(1);
        if *v == 1 {
            self.visit(share.input())
        }
    }
}

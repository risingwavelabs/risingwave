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

mod plan_cloner;
mod share_source_rewriter;

use std::collections::HashMap;

use itertools::Itertools;
pub use plan_cloner::*;
pub use share_source_rewriter::*;

use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::*;

pub trait PlanRewriter<C: ConventionMarker> {
    fn rewrite_with_inputs(&mut self, plan: &PlanRef, inputs: Vec<PlanRef>) -> PlanRef;
}

impl PlanRef {
    pub fn rewrite_with<C: ConventionMarker>(
        &self,
        rewriter: &mut impl PlanRewriter<C>,
    ) -> PlanRef {
        self.expect_convention::<C>();
        let mut share_map = HashMap::new();
        self.rewrite_recursively(rewriter, &mut share_map)
    }

    fn rewrite_recursively<C: ConventionMarker>(
        &self,
        rewriter: &mut impl PlanRewriter<C>,
        share_map: &mut HashMap<PlanNodeId, PlanRef>,
    ) -> PlanRef {
        use risingwave_common::util::recursive::{Recurse, tracker};

        use crate::session::current::notice_to_user;
        tracker!().recurse(|t| {
            if t.depth_reaches(PLAN_DEPTH_THRESHOLD) {
                notice_to_user(PLAN_TOO_DEEP_NOTICE);
            }

            if let Some(share) = self.as_share_node::<C>() {
                let id = self.id();
                return if let Some(share) = share_map.get(&id) {
                    share.clone()
                } else {
                    let input = share.input();
                    let new_input = input.rewrite_recursively(rewriter, share_map);
                    let new_plan = C::ShareNode::new_share(generic::Share::new(new_input));
                    share_map
                        .try_insert(id, new_plan.clone())
                        .expect("non-duplicate");
                    new_plan
                };
            }

            let inputs = self
                .inputs()
                .iter()
                .map(|plan| plan.rewrite_recursively(rewriter, share_map))
                .collect_vec();
            rewriter.rewrite_with_inputs(self, inputs)
        })
    }
}

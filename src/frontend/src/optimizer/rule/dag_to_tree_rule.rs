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

use super::{BoxedRule, Rule};
use crate::optimizer::plan_node::{LogicalShare, PlanTreeNodeUnary};
use crate::optimizer::PlanRef;

pub struct DagToTreeRule {}
impl Rule for DagToTreeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let mut inputs = plan.inputs();
        let mut has_share = false;
        for i in 0..inputs.len() {
            if let Some(logical_share) = inputs[i].as_logical_share() {
                let logical_share: &LogicalShare = logical_share;
                inputs[i] = logical_share.input();
                has_share = true;
            }
        }

        if has_share {
            Some(plan.clone_with_inputs(&inputs))
        } else {
            None
        }
    }
}

impl DagToTreeRule {
    pub fn create() -> BoxedRule {
        Box::new(DagToTreeRule {})
    }
}

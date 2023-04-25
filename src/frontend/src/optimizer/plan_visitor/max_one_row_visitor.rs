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

use super::{DefaultBehavior, Merge};
use crate::optimizer::plan_node::{LogicalApply, PlanTreeNodeBinary};
use crate::optimizer::plan_visitor::PlanVisitor;

pub struct HasMaxOneRowApply();

impl PlanVisitor<bool> for HasMaxOneRowApply {
    type DefaultBehavior = impl DefaultBehavior<bool>;

    fn default_behavior() -> Self::DefaultBehavior {
        Merge(|a, b| a | b)
    }

    fn visit_logical_apply(&mut self, plan: &LogicalApply) -> bool {
        plan.max_one_row() | self.visit(plan.left()) | self.visit(plan.right())
    }
}

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

use super::{DefaultBehavior, Merge, PlanVisitor};
use crate::optimizer::plan_node;

/// Recursively visit the **logical** plan and decide whether it has side effect and cannot be
/// eliminated trivially.
pub struct SideEffectVisitor;

impl PlanVisitor<bool> for SideEffectVisitor {
    fn default_behavior() -> impl DefaultBehavior<bool> {
        Merge(|a, b| a | b)
    }

    fn visit_logical_insert(&mut self, _plan: &plan_node::LogicalInsert) -> bool {
        true
    }

    fn visit_logical_update(&mut self, _plan: &plan_node::LogicalUpdate) -> bool {
        true
    }

    fn visit_logical_delete(&mut self, _plan: &plan_node::LogicalDelete) -> bool {
        true
    }
}

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

use super::{DefaultBehavior, LogicalPlanVisitor, Merge};
use crate::optimizer::LogicalPlanRef;
use crate::optimizer::plan_node::{LogicalSysScan, LogicalValues};
use crate::optimizer::plan_visitor::PlanVisitor;

#[derive(Debug, Clone, Default)]
pub struct SoleSysTableVisitor {
    has_sys_table: bool,
}

impl SoleSysTableVisitor {
    pub fn has_sys_table(plan: LogicalPlanRef) -> bool {
        let mut visitor = SoleSysTableVisitor {
            has_sys_table: false,
        };
        visitor.visit(plan) && visitor.has_sys_table
    }
}

impl LogicalPlanVisitor for SoleSysTableVisitor {
    type Result = bool;

    type DefaultBehavior = impl DefaultBehavior<Self::Result>;

    fn default_behavior() -> Self::DefaultBehavior {
        Merge(|a, b| a & b)
    }

    fn visit_logical_sys_scan(&mut self, _plan: &LogicalSysScan) -> bool {
        self.has_sys_table = true;
        true
    }

    fn visit_logical_values(&mut self, _plan: &LogicalValues) -> Self::Result {
        // sys table together with values is ok
        true
    }
}

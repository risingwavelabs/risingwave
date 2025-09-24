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

use crate::optimizer::plan_node::LogicalIcebergScan;
use crate::optimizer::plan_visitor::{LogicalPlanVisitor, Merge};
use crate::optimizer::{PlanPhaseBatchOptimizedLogical, PlanRoot, PlanVisitor};

/// Visitor to check if Logical Plan contains any LogicalIcebergScan node.
#[derive(Debug, Clone, Default)]
pub struct IcebergScanDetector {
    pub found: bool,
}

impl LogicalPlanVisitor for IcebergScanDetector {
    type DefaultBehavior = Merge<fn(bool, bool) -> bool>;
    type Result = bool;

    fn default_behavior() -> Self::DefaultBehavior {
        Merge(|a, b| a | b)
    }

    fn visit_logical_iceberg_scan(&mut self, _scan: &LogicalIcebergScan) -> Self::Result {
        true
    }
}

impl IcebergScanDetector {
    /// If the plan contains any LogicalIcebergScan node, return true; otherwise, return false.
    pub fn contains_logical_iceberg_scan(
        plan_root: &PlanRoot<PlanPhaseBatchOptimizedLogical>,
    ) -> bool {
        let mut detector = IcebergScanDetector::default();
        detector.visit(plan_root.plan.clone())
    }
}

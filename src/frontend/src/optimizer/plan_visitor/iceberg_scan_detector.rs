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
use crate::optimizer::{LogicalPlanRef, PlanVisitor};

/// Visitor to check if all scans in the plan are Iceberg scans.
#[derive(Debug, Clone, Copy)]
struct IcebergScanDetector;

impl LogicalPlanVisitor for IcebergScanDetector {
    type DefaultBehavior = Merge<fn(bool, bool) -> bool>;
    type Result = bool;

    fn default_behavior() -> Self::DefaultBehavior {
        Merge(|a, b| a & b)
    }

    fn visit_logical_iceberg_scan(&mut self, _: &LogicalIcebergScan) -> Self::Result {
        true
    }

    fn visit_logical_source(
        &mut self,
        plan: &crate::optimizer::plan_node::LogicalSource,
    ) -> Self::Result {
        plan.core.is_iceberg_connector()
    }

    fn visit_logical_values(
        &mut self,
        _: &crate::optimizer::plan_node::LogicalValues,
    ) -> Self::Result {
        true
    }
}

#[easy_ext::ext(LogicalIcebergScanExt)]
pub impl LogicalPlanRef {
    /// Returns `true` if all scans in the plan are Iceberg scans.
    fn all_iceberg_scan(&self) -> bool {
        IcebergScanDetector.visit(self.clone())
    }
}

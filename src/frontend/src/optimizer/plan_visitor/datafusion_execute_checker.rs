// Copyright 2026 RisingWave Labs
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

/// Visitor to check if this plan can be executed by Datafusion.
#[derive(Debug, Clone, Copy)]
struct DatafusionExecuteChecker;

#[derive(Debug, Clone, Copy, Default)]
struct CheckResult {
    have_update_node: bool,
    have_iceberg_scan: bool,
}

impl LogicalPlanVisitor for DatafusionExecuteChecker {
    type DefaultBehavior = Merge<fn(CheckResult, CheckResult) -> CheckResult>;
    type Result = CheckResult;

    fn default_behavior() -> Self::DefaultBehavior {
        Merge(|left, right| CheckResult {
            have_update_node: left.have_update_node || right.have_update_node,
            have_iceberg_scan: left.have_iceberg_scan || right.have_iceberg_scan,
        })
    }

    fn visit_logical_iceberg_scan(&mut self, _: &LogicalIcebergScan) -> Self::Result {
        CheckResult {
            have_iceberg_scan: true,
            ..Default::default()
        }
    }

    fn visit_logical_source(
        &mut self,
        plan: &crate::optimizer::plan_node::LogicalSource,
    ) -> Self::Result {
        let is_iceberg = plan.core.is_iceberg_connector();
        CheckResult {
            have_iceberg_scan: is_iceberg,
            ..Default::default()
        }
    }

    fn visit_logical_insert(
        &mut self,
        _: &crate::optimizer::plan_node::LogicalInsert,
    ) -> Self::Result {
        CheckResult {
            have_update_node: true,
            ..Default::default()
        }
    }

    fn visit_logical_update(
        &mut self,
        _: &crate::optimizer::plan_node::LogicalUpdate,
    ) -> Self::Result {
        CheckResult {
            have_update_node: true,
            ..Default::default()
        }
    }

    fn visit_logical_delete(
        &mut self,
        _: &crate::optimizer::plan_node::LogicalDelete,
    ) -> Self::Result {
        CheckResult {
            have_update_node: true,
            ..Default::default()
        }
    }
}

#[easy_ext::ext(DatafusionExecuteCheckerExt)]
pub impl LogicalPlanRef {
    /// Returns `true` if this plan is able to be executed by Datafusion.
    fn able_to_run_by_datafusion(&self) -> bool {
        let result = DatafusionExecuteChecker.visit(self.clone());
        result.have_iceberg_scan && !result.have_update_node
    }
}

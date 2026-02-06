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

use crate::optimizer::plan_node::{Logical, PlanTreeNode};
use crate::optimizer::plan_visitor::{DefaultValue, LogicalPlanVisitor};
use crate::optimizer::{LogicalPlanRef, PlanVisitor};

/// Visitor to check if this plan can be executed by DataFusion.
#[derive(Debug, Clone, Copy)]
struct DataFusionExecuteChecker;

#[derive(Debug, Clone, Copy, Default)]
pub struct CheckResult {
    pub supported: bool,
    pub have_iceberg_scan: bool,
}

impl DataFusionExecuteChecker {
    /// Recursively checks all input plans and aggregates their [`CheckResult`]s,
    /// combining `supported` with logical AND and `have_iceberg_scan` with logical OR.
    fn check_inputs(&mut self, plan: &impl PlanTreeNode<Logical>) -> CheckResult {
        plan.inputs()
            .into_iter()
            .map(|input| self.visit(input))
            .fold(
                CheckResult {
                    supported: true,
                    have_iceberg_scan: false,
                },
                |mut acc, item| {
                    acc.supported &= item.supported;
                    acc.have_iceberg_scan |= item.have_iceberg_scan;
                    acc
                },
            )
    }
}

impl LogicalPlanVisitor for DataFusionExecuteChecker {
    type DefaultBehavior = DefaultValue;
    type Result = CheckResult;

    fn default_behavior() -> Self::DefaultBehavior {
        DefaultValue
    }

    fn visit_logical_agg(
        &mut self,
        plan: &crate::optimizer::plan_node::LogicalAgg,
    ) -> Self::Result {
        let mut res = self.check_inputs(plan);

        let have_grouping_sets = !plan.grouping_sets().is_empty();
        if have_grouping_sets {
            res.supported = false;
        }
        res
    }

    fn visit_logical_filter(
        &mut self,
        plan: &crate::optimizer::plan_node::LogicalFilter,
    ) -> Self::Result {
        self.check_inputs(plan)
    }

    fn visit_logical_project(
        &mut self,
        plan: &crate::optimizer::plan_node::LogicalProject,
    ) -> Self::Result {
        self.check_inputs(plan)
    }

    fn visit_logical_project_set(
        &mut self,
        plan: &crate::optimizer::plan_node::LogicalProjectSet,
    ) -> Self::Result {
        self.check_inputs(plan)
    }

    fn visit_logical_join(
        &mut self,
        plan: &crate::optimizer::plan_node::LogicalJoin,
    ) -> Self::Result {
        self.check_inputs(plan)
    }

    fn visit_logical_values(
        &mut self,
        plan: &crate::optimizer::plan_node::LogicalValues,
    ) -> Self::Result {
        self.check_inputs(plan)
    }

    fn visit_logical_limit(
        &mut self,
        plan: &crate::optimizer::plan_node::LogicalLimit,
    ) -> Self::Result {
        self.check_inputs(plan)
    }

    fn visit_logical_top_n(
        &mut self,
        plan: &crate::optimizer::plan_node::LogicalTopN,
    ) -> Self::Result {
        let mut res = self.check_inputs(plan);

        let with_ties = plan.limit_attr().with_ties();
        let have_group_key = !plan.group_key().is_empty();
        if with_ties || have_group_key {
            res.supported = false;
        }
        res
    }

    fn visit_logical_union(
        &mut self,
        plan: &crate::optimizer::plan_node::LogicalUnion,
    ) -> Self::Result {
        self.check_inputs(plan)
    }

    fn visit_logical_over_window(
        &mut self,
        plan: &crate::optimizer::plan_node::LogicalOverWindow,
    ) -> Self::Result {
        self.check_inputs(plan)
    }

    fn visit_logical_dedup(
        &mut self,
        plan: &crate::optimizer::plan_node::LogicalDedup,
    ) -> Self::Result {
        self.check_inputs(plan)
    }

    fn visit_logical_iceberg_scan(
        &mut self,
        _: &crate::optimizer::plan_node::LogicalIcebergScan,
    ) -> Self::Result {
        CheckResult {
            supported: true,
            have_iceberg_scan: true,
        }
    }
}

#[easy_ext::ext(DataFusionExecuteCheckerExt)]
pub impl LogicalPlanRef {
    /// Returns `CheckResult` indicating if this plan can be executed by DataFusion.
    fn check_for_datafusion(&self) -> CheckResult {
        DataFusionExecuteChecker.visit(self.clone())
    }
}

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

use risingwave_pb::batch_plan::iceberg_scan_node::IcebergScanType;

use super::prelude::*;
use crate::optimizer::plan_node::{LogicalAgg, LogicalIcebergScan, PlanAggCall};
use crate::optimizer::rule::{ApplyResult, FallibleRule};

pub struct IcebergCountStarRule;
impl FallibleRule<Logical> for IcebergCountStarRule {
    fn apply(&self, plan: PlanRef) -> ApplyResult<PlanRef> {
        let agg: &LogicalAgg = plan.as_logical_agg()?;
        if !agg.group_key().is_empty() || !agg.grouping_sets().is_empty() {
            return ApplyResult::NotApplicable;
        }
        if agg.agg_calls().len() != 1 || agg.agg_calls()[0] != PlanAggCall::count_star() {
            return ApplyResult::NotApplicable;
        }

        let input = &agg.core().input;
        let iceberg_scan: &LogicalIcebergScan = input.as_logical_iceberg_scan()?;
        if iceberg_scan.iceberg_scan_type()? != IcebergScanType::DataScan {
            return ApplyResult::NotApplicable;
        }

        ApplyResult::Ok(
            LogicalIcebergScan::new_count_star_with_logical_iceberg_scan(iceberg_scan).into(),
        )
    }
}

impl IcebergCountStarRule {
    pub fn create() -> BoxedRule {
        Box::new(IcebergCountStarRule)
    }
}

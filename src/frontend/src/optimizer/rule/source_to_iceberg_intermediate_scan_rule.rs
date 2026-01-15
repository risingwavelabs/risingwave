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

//! This rule converts a `LogicalSource` with an Iceberg connector to a
//! `LogicalIcebergIntermediateScan`. The intermediate scan node is used to
//! accumulate predicates and column pruning information before being
//! materialized to the final `LogicalIcebergScan` with delete file anti-joins.
//!
//! This is the first step in the Iceberg scan optimization pipeline:
//! 1. `LogicalSource` -> `LogicalIcebergIntermediateScan` (this rule)
//! 2. Predicate pushdown and column pruning on `LogicalIcebergIntermediateScan`
//! 3. `LogicalIcebergIntermediateScan` -> `LogicalIcebergScan` (materialization rule)

use super::prelude::{PlanRef, *};
use crate::optimizer::plan_node::utils::to_iceberg_time_travel_as_of;
use crate::optimizer::plan_node::{Logical, LogicalIcebergIntermediateScan};
use crate::optimizer::rule::{ApplyResult, FallibleRule};

pub struct SourceToIcebergIntermediateScanRule;

impl FallibleRule<Logical> for SourceToIcebergIntermediateScanRule {
    fn apply(&self, plan: PlanRef) -> ApplyResult<PlanRef> {
        let Some(source) = plan.as_logical_source() else {
            return ApplyResult::NotApplicable;
        };

        if !source.core.is_iceberg_connector() {
            return ApplyResult::NotApplicable;
        }

        #[cfg(madsim)]
        return ApplyResult::Err(
            crate::error::ErrorCode::BindError(
                "iceberg_scan can't be used in the madsim mode".to_string(),
            )
            .into(),
        );

        #[cfg(not(madsim))]
        {
            // If time travel is not specified, we use current timestamp to get the latest snapshot
            let timezone = plan.ctx().get_session_timezone();
            let time_travel_info = to_iceberg_time_travel_as_of(&source.core.as_of, &timezone)?
                .unwrap_or_else(|| {
                    use risingwave_connector::source::iceberg::IcebergTimeTravelInfo;
                    IcebergTimeTravelInfo::TimestampMs(plan.ctx().timestamp_ms())
                });

            let intermediate_scan = LogicalIcebergIntermediateScan::new(source, time_travel_info);
            ApplyResult::Ok(intermediate_scan.into())
        }
    }
}

impl SourceToIcebergIntermediateScanRule {
    pub fn create() -> BoxedRule {
        Box::new(SourceToIcebergIntermediateScanRule)
    }
}

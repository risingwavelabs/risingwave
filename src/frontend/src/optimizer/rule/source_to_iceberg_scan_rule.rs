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

use risingwave_connector::source::ConnectorProperties;

use super::prelude::{PlanRef, *};
use crate::optimizer::plan_node::{Logical, LogicalIcebergScan, LogicalSource};
use crate::optimizer::rule::{ApplyResult, FallibleRule};

pub struct SourceToIcebergScanRule {}
impl FallibleRule<Logical> for SourceToIcebergScanRule {
    fn apply(&self, plan: PlanRef) -> ApplyResult<PlanRef> {
        let source: &LogicalSource = match plan.as_logical_source() {
            Some(s) => s,
            None => return ApplyResult::NotApplicable,
        };
        if source.core.is_iceberg_connector() {
            if let ConnectorProperties::Iceberg(_prop) = ConnectorProperties::extract(
                source
                    .core
                    .catalog
                    .as_ref()
                    .unwrap()
                    .with_properties
                    .clone(),
                false,
            )? {
            } else {
                return ApplyResult::NotApplicable;
            };

            #[cfg(madsim)]
            return ApplyResult::Err(
                crate::error::ErrorCode::BindError(
                    "iceberg_scan can't be used in the madsim mode".to_string(),
                )
                .into(),
            );
            #[cfg(not(madsim))]
            {

                use crate::optimizer::plan_node::LogicalImmIcebergScan;

                let data_iceberg_scan: PlanRef =
                    LogicalImmIcebergScan::new(source).into();
                ApplyResult::Ok(data_iceberg_scan)
            }
        } else {
            ApplyResult::NotApplicable
        }
    }
}

impl SourceToIcebergScanRule {
    pub fn create() -> BoxedRule {
        Box::new(SourceToIcebergScanRule {})
    }
}

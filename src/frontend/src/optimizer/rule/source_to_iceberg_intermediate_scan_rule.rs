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

//! This rule converts a `LogicalSource` with an Iceberg connector to a
//! `LogicalIcebergIntermediateScan`. The intermediate scan node is used to
//! accumulate predicates and column pruning information before being
//! materialized to the final `LogicalIcebergScan` with delete file anti-joins.
//!
//! This is the first step in the Iceberg scan optimization pipeline:
//! 1. `LogicalSource` -> `LogicalIcebergIntermediateScan` (this rule)
//! 2. Predicate pushdown and column pruning on `LogicalIcebergIntermediateScan`
//! 3. `LogicalIcebergIntermediateScan` -> `LogicalIcebergScan` (materialization rule)

use std::collections::HashMap;

#[cfg(not(madsim))]
use risingwave_connector::source::ConnectorProperties;
#[cfg(not(madsim))]
use risingwave_connector::source::iceberg::IcebergTimeTravelInfo;

use super::prelude::{PlanRef, *};
use crate::error::Result;
#[cfg(not(madsim))]
use crate::optimizer::plan_node::LogicalSource;
#[cfg(not(madsim))]
use crate::optimizer::plan_node::LogicalValues;
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
            let mut time_travel_info = to_iceberg_time_travel_as_of(&source.core.as_of, &timezone)?;
            if time_travel_info.is_none() {
                time_travel_info =
                    fetch_current_snapshot_id(&mut plan.ctx().iceberg_snapshot_id_map(), source)?
                        .map(IcebergTimeTravelInfo::Version);
            }
            let Some(time_travel_info) = time_travel_info else {
                return ApplyResult::Ok(
                    LogicalValues::new(vec![], plan.schema().clone(), plan.ctx()).into(),
                );
            };
            let intermediate_scan = LogicalIcebergIntermediateScan::new(source, time_travel_info);
            ApplyResult::Ok(intermediate_scan.into())
        }
    }
}

#[cfg(not(madsim))]
fn fetch_current_snapshot_id(
    map: &mut HashMap<String, Option<i64>>,
    source: &LogicalSource,
) -> Result<Option<i64>> {
    let catalog = source.source_catalog().ok_or_else(|| {
        crate::error::ErrorCode::InternalError(
            "Iceberg source must have a valid source catalog".to_owned(),
        )
    })?;
    let name = catalog.name.as_str();
    if let Some(&snapshot_id) = map.get(name) {
        return Ok(snapshot_id);
    }

    let ConnectorProperties::Iceberg(prop) =
        ConnectorProperties::extract(catalog.with_properties.clone(), false)?
    else {
        return Err(crate::error::ErrorCode::InternalError(
            "Iceberg source must have Iceberg connector properties".to_owned(),
        )
        .into());
    };

    let snapshot_id = tokio::task::block_in_place(|| {
        crate::utils::FRONTEND_RUNTIME.block_on(async {
            prop.load_table()
                .await
                .map(|table| table.metadata().current_snapshot_id())
        })
    })?;
    map.insert(name.to_owned(), snapshot_id);
    Ok(snapshot_id)
}

impl SourceToIcebergIntermediateScanRule {
    pub fn create() -> BoxedRule {
        Box::new(SourceToIcebergIntermediateScanRule)
    }
}

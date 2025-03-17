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

use risingwave_common::catalog::{RW_TIMESTAMP_COLUMN_ID, RW_TIMESTAMP_COLUMN_NAME};

use super::{DefaultBehavior, Merge};
use crate::PlanRef;
use crate::optimizer::plan_node::StreamTableScan;
use crate::optimizer::plan_visitor::PlanVisitor;

#[derive(Debug, Clone, Default)]
pub struct RwTimestampValidator {}

impl RwTimestampValidator {
    pub fn select_rw_timestamp_in_stream_query(plan: PlanRef) -> bool {
        RwTimestampValidator::default().visit(plan)
    }
}

impl PlanVisitor for RwTimestampValidator {
    type Result = bool;

    type DefaultBehavior = impl DefaultBehavior<Self::Result>;

    fn default_behavior() -> Self::DefaultBehavior {
        Merge(|a, b| a | b)
    }

    fn visit_stream_table_scan(&mut self, stream_table_scan: &StreamTableScan) -> bool {
        stream_table_scan
            .core()
            .column_descs()
            .iter()
            .any(|c| c.column_id == RW_TIMESTAMP_COLUMN_ID && c.name == RW_TIMESTAMP_COLUMN_NAME)
    }
}

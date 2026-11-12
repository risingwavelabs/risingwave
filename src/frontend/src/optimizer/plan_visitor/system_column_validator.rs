// Copyright 2024 RisingWave Labs
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

use risingwave_common::catalog::{
    RW_TIMESTAMP_COLUMN_ID, RW_TIMESTAMP_COLUMN_NAME, RW_VNODE_COLUMN_ID, RW_VNODE_COLUMN_NAME,
};

use super::{DefaultBehavior, Merge, StreamPlanVisitor};
use crate::optimizer::plan_node::{StreamPlanRef as PlanRef, StreamTableScan};
use crate::optimizer::plan_visitor::PlanVisitor;

#[derive(Debug, Clone, Default)]
pub struct SystemColumnValidator {}

impl SystemColumnValidator {
    pub fn unsupported_column_in_stream_query(plan: PlanRef) -> Option<&'static str> {
        SystemColumnValidator::default().visit(plan)
    }
}

impl StreamPlanVisitor for SystemColumnValidator {
    type Result = Option<&'static str>;

    type DefaultBehavior = impl DefaultBehavior<Self::Result>;

    fn default_behavior() -> Self::DefaultBehavior {
        Merge(|a: Option<&'static str>, b| a.or(b))
    }

    fn visit_stream_table_scan(
        &mut self,
        stream_table_scan: &StreamTableScan,
    ) -> Option<&'static str> {
        stream_table_scan
            .core()
            .column_descs()
            .iter()
            .find_map(|c| {
                if c.column_id == RW_TIMESTAMP_COLUMN_ID {
                    Some(RW_TIMESTAMP_COLUMN_NAME)
                } else if c.column_id == RW_VNODE_COLUMN_ID {
                    Some(RW_VNODE_COLUMN_NAME)
                } else {
                    None
                }
            })
    }
}

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

use super::{DefaultBehavior, Merge};
use crate::optimizer::BatchPlanRoot;
use crate::optimizer::plan_node::{BatchSysSeqScan, LogicalSysScan, StreamTableScan};
use crate::optimizer::plan_visitor::PlanVisitor;

#[derive(Debug, Clone, Default)]
pub struct SysTableVisitor {}

impl SysTableVisitor {
    pub fn has_sys_table(plan: &BatchPlanRoot) -> bool {
        let mut visitor = SysTableVisitor {};
        visitor.visit(plan.plan.clone())
    }
}

impl PlanVisitor for SysTableVisitor {
    type Result = bool;

    type DefaultBehavior = impl DefaultBehavior<Self::Result>;

    fn default_behavior() -> Self::DefaultBehavior {
        Merge(|a, b| a | b)
    }

    fn visit_batch_sys_seq_scan(&mut self, _batch_seq_scan: &BatchSysSeqScan) -> bool {
        true
    }

    fn visit_logical_sys_scan(&mut self, _logical_scan: &LogicalSysScan) -> bool {
        true
    }

    // Sys scan not allowed for streaming.
    fn visit_stream_table_scan(&mut self, _stream_table_scan: &StreamTableScan) -> bool {
        false
    }
}

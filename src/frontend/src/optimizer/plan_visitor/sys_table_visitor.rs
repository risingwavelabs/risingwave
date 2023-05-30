// Copyright 2023 RisingWave Labs
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
use crate::optimizer::plan_node::{BatchSeqScan, LogicalScan, StreamTableScan};
use crate::optimizer::plan_visitor::PlanVisitor;
use crate::PlanRef;

#[derive(Debug, Clone, Default)]
pub struct SysTableVisitor {}

impl SysTableVisitor {
    pub fn has_sys_table(plan: PlanRef) -> bool {
        let mut visitor = SysTableVisitor {};
        visitor.visit(plan)
    }
}

impl PlanVisitor<bool> for SysTableVisitor {
    type DefaultBehavior = impl DefaultBehavior<bool>;

    fn default_behavior() -> Self::DefaultBehavior {
        Merge(|a, b| a | b)
    }

    fn visit_batch_seq_scan(&mut self, batch_seq_scan: &BatchSeqScan) -> bool {
        batch_seq_scan.logical().is_sys_table
    }

    fn visit_logical_scan(&mut self, logical_scan: &LogicalScan) -> bool {
        logical_scan.is_sys_table()
    }

    fn visit_stream_table_scan(&mut self, stream_table_scan: &StreamTableScan) -> bool {
        stream_table_scan.logical().is_sys_table
    }
}

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

use crate::optimizer::plan_node::{
    BatchSeqScan, LogicalScan, PlanTreeNodeBinary, StreamTableScan, StreamTemporalJoin,
};
use crate::optimizer::plan_visitor::PlanVisitor;
use crate::PlanRef;

#[derive(Debug, Clone, Default)]
pub struct TemporalJoinValidator {}

impl TemporalJoinValidator {
    pub fn exist_dangling_temporal_scan(plan: PlanRef) -> bool {
        let mut decider = TemporalJoinValidator {};
        decider.visit(plan)
    }
}

impl PlanVisitor<bool> for TemporalJoinValidator {
    fn merge(a: bool, b: bool) -> bool {
        a | b
    }

    fn visit_stream_table_scan(&mut self, stream_table_scan: &StreamTableScan) -> bool {
        stream_table_scan.logical().for_system_time_as_of_now()
    }

    fn visit_batch_seq_scan(&mut self, batch_seq_scan: &BatchSeqScan) -> bool {
        batch_seq_scan.logical().for_system_time_as_of_now()
    }

    fn visit_logical_scan(&mut self, logical_scan: &LogicalScan) -> bool {
        logical_scan.for_system_time_as_of_now()
    }

    fn visit_stream_temporal_join(&mut self, stream_temporal_join: &StreamTemporalJoin) -> bool {
        self.visit(stream_temporal_join.left())
    }
}

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

use crate::optimizer::plan_node::{BatchSeqScan, BatchValues};
use crate::optimizer::plan_visitor::PlanVisitor;
use crate::PlanRef;

#[derive(Debug, Clone, Default)]
pub struct ExecutionModeDecider {}

impl ExecutionModeDecider {
    pub fn run_in_local_mode(plan: PlanRef) -> bool {
        let mut decider = ExecutionModeDecider {};
        decider.visit(plan)
    }
}

impl PlanVisitor<bool> for ExecutionModeDecider {
    fn merge(a: bool, b: bool) -> bool {
        a & b
    }

    fn visit_batch_seq_scan(&mut self, batch_seq_scan: &BatchSeqScan) -> bool {
        !batch_seq_scan.scan_ranges().is_empty()
            && batch_seq_scan
                .scan_ranges()
                .iter()
                .all(|x| x.has_eq_conds() || x.two_side_bound())
    }

    fn visit_batch_values(&mut self, _batch_values: &BatchValues) -> bool {
        true
    }
}

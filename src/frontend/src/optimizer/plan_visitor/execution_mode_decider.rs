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
use crate::optimizer::plan_node::{BatchLimit, BatchSeqScan, BatchValues, PlanTreeNodeUnary};
use crate::optimizer::plan_visitor::PlanVisitor;
use crate::PlanRef;

#[derive(Debug, Clone, Default)]
pub struct ExecutionModeDecider {}

impl ExecutionModeDecider {
    /// If the plan should run in local mode, return true; otherwise, return false.
    pub fn run_in_local_mode(plan: PlanRef) -> bool {
        let mut decider = ExecutionModeDecider {};
        decider.visit(plan)
    }
}

impl PlanVisitor<bool> for ExecutionModeDecider {
    type DefaultBehavior = impl DefaultBehavior<bool>;

    fn default_behavior() -> Self::DefaultBehavior {
        Merge(|a, b| a & b)
    }

    /// Point select, index lookup and two side bound range scan.
    /// select * from t where id = 1
    /// select * from t where k = 1
    /// select * from t where id between 1 and 5
    fn visit_batch_seq_scan(&mut self, batch_seq_scan: &BatchSeqScan) -> bool {
        !batch_seq_scan.scan_ranges().is_empty()
            && batch_seq_scan
                .scan_ranges()
                .iter()
                .all(|x| x.has_eq_conds() || x.two_side_bound())
    }

    /// Simple value select.
    /// select 1
    fn visit_batch_values(&mut self, _batch_values: &BatchValues) -> bool {
        true
    }

    /// Limit scan.
    /// select * from t limit 1
    /// select * from t order by k limit 1
    fn visit_batch_limit(&mut self, batch_limit: &BatchLimit) -> bool {
        if let Some(batch_seq_scan) = batch_limit.input().as_batch_seq_scan()
            && batch_seq_scan.scan_ranges().is_empty()
            && batch_limit.limit() + batch_limit.offset() < 100{
            true
        } else {
            self.visit(batch_limit.input())
        }
    }
}

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

use std::collections::HashSet;

use risingwave_common::catalog::TableId;

use super::{DefaultBehavior, DefaultValue};
use crate::optimizer::plan_node::{BatchLogSeqScan, BatchLookupJoin};
use crate::optimizer::plan_visitor::PlanVisitor;
use crate::PlanRef;

#[derive(Debug, Clone, Default)]
pub struct ReadStorageTableVisitor {
    tables: HashSet<TableId>,
}

impl ReadStorageTableVisitor {
    pub fn collect(plan: PlanRef) -> HashSet<TableId> {
        let mut visitor = Self::default();
        visitor.visit(plan);
        visitor.tables
    }
}

impl PlanVisitor for ReadStorageTableVisitor {
    type Result = ();

    type DefaultBehavior = impl DefaultBehavior<Self::Result>;

    fn default_behavior() -> Self::DefaultBehavior {
        DefaultValue
    }

    fn visit_batch_seq_scan(&mut self, plan: &crate::optimizer::plan_node::BatchSeqScan) {
        self.tables.insert(plan.core().table_desc.table_id);
    }

    fn visit_batch_log_seq_scan(&mut self, plan: &BatchLogSeqScan) -> Self::Result {
        self.tables.insert(plan.core().table_desc.table_id);
    }

    fn visit_batch_lookup_join(&mut self, plan: &BatchLookupJoin) -> Self::Result {
        self.tables.insert(plan.right_table_desc().table_id);
    }
}

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

use std::collections::HashSet;

use risingwave_common::catalog::TableId;

use crate::optimizer::plan_node::{BatchSource, StreamTableScan};
use crate::optimizer::plan_visitor::PlanVisitor;
use crate::PlanRef;

#[derive(Debug, Clone, Default)]
pub struct RelationCollectorVisitor {
    relations: HashSet<TableId>,
}

impl RelationCollectorVisitor {
    fn new_with(relations: HashSet<TableId>) -> Self {
        Self { relations }
    }

    pub fn collect_with(relations: HashSet<TableId>, plan: PlanRef) -> HashSet<TableId> {
        let mut visitor = Self::new_with(relations);
        visitor.visit(plan);
        visitor.relations
    }
}

impl PlanVisitor<()> for RelationCollectorVisitor {
    fn merge(_: (), _: ()) {}

    fn visit_batch_seq_scan(&mut self, plan: &crate::optimizer::plan_node::BatchSeqScan) {
        self.relations
            .insert(plan.logical().table_desc().table_id.table_id.into());
    }

    fn visit_stream_table_scan(&mut self, plan: &StreamTableScan) -> () {
        self.relations.insert(plan.logical().table_desc().table_id);
    }

    fn visit_batch_source(&mut self, plan: &BatchSource) -> () {
        if let Some(catalog) = plan.logical().source_catalog() {
            self.relations.insert(catalog.id.into());
        }
    }
}

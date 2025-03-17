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

use std::collections::HashSet;

use risingwave_common::catalog::TableId;

use super::{DefaultBehavior, DefaultValue};
use crate::PlanRef;
use crate::optimizer::plan_node::{BatchSource, LogicalScan, StreamSource, StreamTableScan};
use crate::optimizer::plan_visitor::PlanVisitor;

/// TODO(rc): maybe we should rename this to `DependencyCollectorVisitor`.
#[derive(Debug, Clone, Default)]
pub struct RelationCollectorVisitor {
    relations: HashSet<TableId>,
}

impl RelationCollectorVisitor {
    fn new_with(relations: HashSet<TableId>) -> Self {
        Self { relations }
    }

    /// `collect_with` will collect all the relations in the plan with some default ones, which are
    /// collected during the binding phase. Note that during visit the collected relations might be
    /// duplicated with the default ones. The collection is necessary, because implicit dependencies
    /// on indices can only be discovered after plan is built.
    pub fn collect_with(relations: HashSet<TableId>, plan: PlanRef) -> HashSet<TableId> {
        let mut visitor = Self::new_with(relations);
        visitor.visit(plan);
        visitor.relations
    }
}

impl PlanVisitor for RelationCollectorVisitor {
    type Result = ();

    type DefaultBehavior = impl DefaultBehavior<Self::Result>;

    fn default_behavior() -> Self::DefaultBehavior {
        DefaultValue
    }

    fn visit_batch_seq_scan(&mut self, plan: &crate::optimizer::plan_node::BatchSeqScan) {
        self.relations.insert(plan.core().table_desc.table_id);
    }

    fn visit_logical_scan(&mut self, plan: &LogicalScan) {
        self.relations.insert(plan.table_desc().table_id);
    }

    fn visit_stream_table_scan(&mut self, plan: &StreamTableScan) {
        let logical = plan.core();
        self.relations.insert(logical.table_desc.table_id);
    }

    fn visit_batch_source(&mut self, plan: &BatchSource) {
        if let Some(catalog) = plan.source_catalog() {
            self.relations.insert(catalog.id.into());
        }
    }

    fn visit_stream_source(&mut self, plan: &StreamSource) {
        if let Some(catalog) = plan.source_catalog() {
            self.relations.insert(catalog.id.into());
        }
    }
}

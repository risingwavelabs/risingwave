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
use risingwave_common::bail_not_implemented;

use crate::Planner;
use crate::binder::Relation;
use crate::error::Result;
use crate::optimizer::plan_node::{LogicalChangeLog, LogicalPlanRef as PlanRef};

impl Planner {
    pub(super) fn plan_changelog(&mut self, relation: Relation) -> Result<PlanRef> {
        let vnode_count = if let Relation::BaseTable(base_table) = &relation {
            base_table.table_catalog.vnode_count()
        } else {
            bail_not_implemented!("changelog only support base table");
        };
        let root = self.plan_relation(relation)?;
        let plan = LogicalChangeLog::create(root, vnode_count);
        Ok(plan)
    }
}

// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::Result;

use crate::binder::{BoundBaseTable, BoundJoin, Relation};
use crate::optimizer::plan_node::{LogicalJoin, LogicalScan, PlanRef};
use crate::planner::Planner;

impl Planner {
    pub(super) fn plan_relation(&mut self, relation: Relation) -> Result<PlanRef> {
        match relation {
            Relation::BaseTable(t) => self.plan_base_table(*t),
            // TODO: order is ignored in the subquery
            Relation::Subquery(q) => Ok(self.plan_query(q.query)?.as_subplan()),
            Relation::Join(join) => self.plan_join(*join),
        }
    }

    pub(super) fn plan_base_table(&mut self, base_table: BoundBaseTable) -> Result<PlanRef> {
        let (column_ids, fields) = base_table
            .columns
            .iter()
            .map(|c| {
                (
                    c.column_id,
                    Field::with_name(c.data_type.clone(), c.name.clone()),
                )
            })
            .unzip();
        let schema = Schema::new(fields);
        LogicalScan::create(
            base_table.name,
            base_table.table_id,
            column_ids,
            schema,
            self.ctx(),
        )
    }

    pub(super) fn plan_join(&mut self, join: BoundJoin) -> Result<PlanRef> {
        let left = self.plan_relation(join.left)?;
        let right = self.plan_relation(join.right)?;
        // TODO: Support more join types.
        let join_type = risingwave_pb::plan::JoinType::Inner;
        let on_clause = join.cond;
        Ok(LogicalJoin::create(left, right, join_type, on_clause))
    }
}

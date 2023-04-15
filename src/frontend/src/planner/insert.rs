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

use fixedbitset::FixedBitSet;
use risingwave_common::error::Result;

use crate::binder::BoundInsert;
use crate::optimizer::plan_node::{LogicalInsert, LogicalProject, PlanRef};
use crate::optimizer::property::{Order, RequiredDist};
use crate::optimizer::PlanRoot;
use crate::planner::Planner;

impl Planner {
    pub(super) fn plan_insert(&mut self, insert: BoundInsert) -> Result<PlanRoot> {
        let mut input = self.plan_query(insert.source)?.into_subplan();
        if !insert.cast_exprs.is_empty() {
            input = LogicalProject::create(input, insert.cast_exprs);
        }
        let returning = !insert.returning_list.is_empty();
        let mut plan: PlanRef = LogicalInsert::create(
            input,
            insert.table_name.clone(),
            insert.table_id,
            insert.table_version_id,
            insert.column_indices,
            insert.default_columns,
            insert.row_id_index,
            returning,
        )?
        .into();
        // If containing RETURNING, add one logicalproject node
        if returning {
            plan = LogicalProject::create(plan, insert.returning_list);
        }
        // For insert, frontend will only schedule one task so do not need this to be single.
        let dist = RequiredDist::Any;
        let mut out_fields = FixedBitSet::with_capacity(plan.schema().len());
        out_fields.insert_range(..);
        let out_names = if returning {
            insert.returning_schema.expect("If returning list is not empty, should provide returning schema in BoundInsert.").names()
        } else {
            plan.schema().names()
        };
        let root = PlanRoot::new(plan, dist, Order::any(), out_fields, out_names);
        Ok(root)
    }
}

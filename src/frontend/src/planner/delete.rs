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

use super::Planner;
use crate::binder::BoundDelete;
use crate::optimizer::plan_node::{LogicalDelete, LogicalFilter, LogicalProject};
use crate::optimizer::property::{Order, RequiredDist};
use crate::optimizer::{PlanRef, PlanRoot};

impl Planner {
    pub(super) fn plan_delete(&mut self, delete: BoundDelete) -> Result<PlanRoot> {
        let scan = self.plan_base_table(delete.table)?;
        let input = if let Some(expr) = delete.selection {
            LogicalFilter::create_with_expr(scan, expr)
        } else {
            scan
        };
        let returning = !delete.returning_list.is_empty();
        let mut plan: PlanRef = LogicalDelete::create(
            input,
            delete.table_name.clone(),
            delete.table_id,
            delete.table_version_id,
            returning,
        )?
        .into();

        if returning {
            plan = LogicalProject::create(plan, delete.returning_list);
        }

        // For delete, frontend will only schedule one task so do not need this to be single.
        let dist = RequiredDist::Any;
        let mut out_fields = FixedBitSet::with_capacity(plan.schema().len());
        out_fields.insert_range(..);
        let out_names = if returning {
            delete.returning_schema.expect("If returning list is not empty, should provide returning schema in BoundDelete.").names()
        } else {
            plan.schema().names()
        };

        let root = PlanRoot::new(plan, dist, Order::any(), out_fields, out_names);
        Ok(root)
    }
}

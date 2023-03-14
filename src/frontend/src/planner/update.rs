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

use super::select::LogicalFilter;
use super::Planner;
use crate::binder::BoundUpdate;
use crate::optimizer::plan_node::{LogicalProject, LogicalUpdate};
use crate::optimizer::property::{Order, RequiredDist};
use crate::optimizer::{PlanRef, PlanRoot};

impl Planner {
    pub(super) fn plan_update(&mut self, update: BoundUpdate) -> Result<PlanRoot> {
        let scan = self.plan_relation(update.table)?;
        let input = if let Some(expr) = update.selection {
            LogicalFilter::create_with_expr(scan, expr)
        } else {
            scan
        };
        let returning = !update.returning_list.is_empty();
        let mut plan: PlanRef = LogicalUpdate::create(
            input,
            update.table_name.clone(),
            update.table_id,
            update.table_version_id,
            update.exprs,
            returning,
        )?
        .into();

        if returning {
            plan = LogicalProject::create(plan, update.returning_list);
        }

        // For update, frontend will only schedule one task so do not need this to be single.
        let dist = RequiredDist::Any;
        let mut out_fields = FixedBitSet::with_capacity(plan.schema().len());
        out_fields.insert_range(..);
        let out_names = if returning {
            update.returning_schema.expect("If returning list is not empty, should provide returning schema in BoundDelete.").names()
        } else {
            plan.schema().names()
        };

        let root = PlanRoot::new(plan, dist, Order::any(), out_fields, out_names);
        Ok(root)
    }
}

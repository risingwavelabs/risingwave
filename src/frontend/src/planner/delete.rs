// Copyright 2022 RisingWave Labs
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

use super::Planner;
use crate::binder::BoundDelete;
use crate::error::Result;
use crate::optimizer::plan_node::{LogicalDelete, LogicalProject, generic};
use crate::optimizer::property::{Order, RequiredDist};
use crate::optimizer::{LogicalPlanRef as PlanRef, LogicalPlanRoot, PlanRoot};
use crate::utils::ColIndexMapping;

impl Planner {
    pub(super) fn plan_delete(&mut self, delete: BoundDelete) -> Result<LogicalPlanRoot> {
        let table_catalog = &delete.table.table_catalog;
        let dml_output_indices: Vec<usize> = table_catalog
            .columns()
            .iter()
            .enumerate()
            .filter_map(|(i, c)| c.can_dml().then_some(i))
            .collect();
        let table_to_dml = ColIndexMapping::with_remaining_columns(
            &dml_output_indices,
            table_catalog.columns().len(),
        );
        let pk_indices: Vec<usize> = table_catalog
            .pk()
            .iter()
            .map(|order| {
                table_to_dml
                    .try_map(order.column_index)
                    .expect("pk column should be part of dml input schema")
            })
            .collect();

        let scan = self.plan_base_table(&delete.table)?;
        let input = if let Some(expr) = delete.selection {
            self.plan_where(scan, expr)?
        } else {
            scan
        };
        let input = if dml_output_indices.len() != table_catalog.columns().len() {
            LogicalProject::with_out_col_idx(input, dml_output_indices.iter().copied()).into()
        } else {
            input
        };
        let returning = !delete.returning_list.is_empty();
        let mut plan: PlanRef = LogicalDelete::from(generic::Delete::new(
            input,
            delete.table_name.clone(),
            delete.table_id,
            delete.table_version_id,
            pk_indices,
            returning,
        ))
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

        let root = PlanRoot::new_with_logical_plan(plan, dist, Order::any(), out_fields, out_names);
        Ok(root)
    }
}

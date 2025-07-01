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

use fixedbitset::FixedBitSet;

use crate::binder::BoundQuery;
use crate::error::Result;
use crate::optimizer::plan_node::{LogicalLimit, LogicalTopN};
use crate::optimizer::property::{Order, RequiredDist};
use crate::optimizer::{LogicalPlanRoot, PlanRoot};
use crate::planner::Planner;

pub const LIMIT_ALL_COUNT: u64 = u64::MAX / 2;

impl Planner {
    /// Plan a [`BoundQuery`]. Need to bind before planning.
    ///
    /// Works for both batch query and streaming query (`CREATE MATERIALIZED VIEW`).
    pub fn plan_query(&mut self, query: BoundQuery) -> Result<LogicalPlanRoot> {
        let out_names = query.schema().names();
        let BoundQuery {
            body,
            order,
            limit,
            offset,
            with_ties,
            extra_order_exprs,
        } = query;

        let extra_order_exprs_len = extra_order_exprs.len();
        let mut plan = self.plan_set_expr(body, extra_order_exprs, &order)?;
        let mut order = Order {
            column_orders: order,
        };

        if limit.is_some() || offset.is_some() {
            // Optimize order key if using it for TopN / Limit.
            // Both are singleton dist, so we can leave dist_key_indices as empty.
            let func_dep = plan.functional_dependency();
            order = func_dep.minimize_order_key(order, &[]);

            let limit = limit.unwrap_or(LIMIT_ALL_COUNT);

            let offset = offset.unwrap_or_default();
            plan = if order.column_orders.is_empty() {
                // Should be rejected by parser.
                assert!(!with_ties);
                // Create a logical limit if with limit/offset but without order-by
                LogicalLimit::create(plan, limit, offset)
            } else {
                // Create a logical top-n if with limit/offset and order-by
                LogicalTopN::create(plan, limit, offset, order.clone(), with_ties, vec![])?
            }
        }
        let mut out_fields = FixedBitSet::with_capacity(plan.schema().len());
        out_fields.insert_range(..plan.schema().len() - extra_order_exprs_len);
        if let Some(field) = plan.schema().fields.first()
            && field.name == "projected_row_id"
        {
            // Do not output projected_row_id hidden column.
            out_fields.set(0, false);
        }
        let root =
            PlanRoot::new_with_logical_plan(plan, RequiredDist::Any, order, out_fields, out_names);
        Ok(root)
    }
}

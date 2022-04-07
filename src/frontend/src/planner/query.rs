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

use fixedbitset::FixedBitSet;
use risingwave_common::error::Result;

use crate::binder::BoundQuery;
use crate::optimizer::plan_node::LogicalLimit;
use crate::optimizer::property::{Distribution, Order};
use crate::optimizer::PlanRoot;
use crate::planner::Planner;

pub const LIMIT_ALL_COUNT: usize = usize::MAX / 2;

impl Planner {
    /// Plan a [`BoundQuery`]. Need to bind before planning.
    pub fn plan_query(&mut self, query: BoundQuery) -> Result<PlanRoot> {
        let (mut plan, map) = self.plan_set_expr(query.body)?;
        // A logical limit is added if limit, offset or both are specified
        if query.limit.is_some() || query.offset.is_some() {
            plan = LogicalLimit::create(
                plan,
                query.limit.unwrap_or(LIMIT_ALL_COUNT),
                query.offset.unwrap_or_default(),
            )
        }
        // plan order and limit here
        let order = Order {
            field_order: query.order,
        };
        let dist = Distribution::Single;
        let mut out_fields = FixedBitSet::with_capacity(plan.schema().len());
        out_fields.insert_range(..);
        let root = PlanRoot::new_with_map(plan, dist, order, out_fields, map);
        Ok(root)
    }
}

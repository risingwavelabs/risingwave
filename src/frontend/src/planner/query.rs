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
use itertools::Itertools;
use risingwave_common::catalog::{ColumnDesc, ColumnId};
use risingwave_common::error::Result;

use crate::binder::{BoundQuery, BoundSelect, BoundSetExpr};
use crate::expr::{Expr, ExprImpl};
use crate::optimizer::plan_node::LogicalLimit;
use crate::optimizer::property::{Distribution, Order};
use crate::optimizer::PlanRoot;
use crate::planner::Planner;

pub const LIMIT_ALL_COUNT: usize = usize::MAX / 2;

impl Planner {
    /// Plan a [`BoundQuery`]. Need to bind before planning.
    pub fn plan_query(&mut self, query: BoundQuery) -> Result<PlanRoot> {
        self.select_items = match &query.body {
            BoundSetExpr::Select(select) => self.extract_select_items(select)?,
            BoundSetExpr::Values(_) => {
                vec![]
            }
        };
        let mut plan = self.plan_set_expr(query.body)?;

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
        let root =
            PlanRoot::new_with_items(plan, dist, order, out_fields, self.select_items.clone());
        Ok(root)
    }

    /// Extract the `column_desc` of `select_items` and change column name to `alias_name`.
    fn extract_select_items(&mut self, select: &BoundSelect) -> Result<Vec<ColumnDesc>> {
        let table: Vec<ColumnDesc> = {
            match &select.from {
                Some(relation) => relation.extract_column_descs(),
                None => vec![],
            }
        };
        let column_descs = select
            .select_items
            .iter()
            .zip_eq(select.aliases.iter())
            .enumerate()
            .map(|(id, (expr, alias))| {
                let default_desc = ColumnDesc {
                    data_type: expr.return_type(),
                    column_id: ColumnId::new(id as i32),
                    name: "".to_string(),
                    field_descs: vec![],
                    type_name: "".to_string(),
                };
                let mut desc = match expr {
                    ExprImpl::InputRef(input) => match table.get(input.index()) {
                        Some(column) => column.clone(),
                        None => default_desc,
                    },
                    _ => default_desc,
                };
                let name = alias.clone().unwrap_or(match desc.name.is_empty() {
                    true => desc.name.clone(),
                    false => format!("expr#{}", id),
                });
                desc.change_prefix_name(desc.name.clone(), name);
                Ok(desc.clone())
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(column_descs)
    }
}

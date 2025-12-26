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

use risingwave_common::util::sort_util::ColumnOrder;

use super::prelude::{PlanRef, *};
use crate::optimizer::plan_node::{LogicalProject, LogicalTopN, PlanTreeNodeUnary};
use crate::optimizer::property::Order;

/// Transpose `LogicalTopN` and `LogicalProject` when the project is a pure projection.
pub struct TopNProjectTransposeRule {}

impl Rule<Logical> for TopNProjectTransposeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let top_n: &LogicalTopN = plan.as_logical_top_n()?;
        let project: LogicalProject = top_n.input().as_logical_project()?.to_owned();
        let projection = project.try_as_projection()?;

        let new_order = Order {
            column_orders: top_n
                .topn_order()
                .column_orders
                .iter()
                .map(|order| {
                    let mapped_idx = projection.get(order.column_index).copied()?;
                    Some(ColumnOrder::new(mapped_idx, order.order_type))
                })
                .collect::<Option<Vec<_>>>()?,
        };
        let new_group_key = top_n
            .group_key()
            .iter()
            .map(|idx| projection.get(*idx).copied())
            .collect::<Option<Vec<_>>>()?;

        let limit_attr = top_n.limit_attr();
        let new_top_n = LogicalTopN::new(
            project.input(),
            limit_attr.limit(),
            top_n.offset(),
            limit_attr.with_ties(),
            new_order,
            new_group_key,
        );
        Some(project.clone_with_input(new_top_n.into()).into())
    }
}

impl TopNProjectTransposeRule {
    pub fn create() -> BoxedRule {
        Box::new(TopNProjectTransposeRule {})
    }
}

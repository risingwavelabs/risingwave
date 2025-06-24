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

use itertools::Itertools;

use super::{BoxedRule, Rule};
use crate::expr::ExprImpl;
use crate::optimizer::PlanRef;
use crate::optimizer::plan_node::PlanTreeNodeUnary;

pub struct TopNPushDownRule {}

impl Rule for TopNPushDownRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let top_n = plan.as_logical_top_n()?;

        if top_n.offset() != 0 {
            return Some(plan);
        }

        let col_orders = &top_n.topn_order().column_orders;
        if col_orders.is_empty() {
            return Some(plan);
        }

        let project_input = top_n.input();
        let project = project_input.as_logical_project()?;

        for expr in project.exprs() {
            if expr.has_agg_call()
                || expr.has_subquery()
                || expr.has_user_defined_function()
                || expr.has_window_function()
                || expr.has_table_function()
                || expr.has_now()
                || expr.is_parameter()
            {
                return Some(plan);
            }
        }

        let can_pushdown = col_orders.iter().all(|field_order| {
            matches!(
                &project.exprs()[field_order.column_index],
                ExprImpl::InputRef(_)
            )
        });
        if !can_pushdown {
            return Some(plan);
        }

        let new_orders = top_n
            .topn_order()
            .clone()
            .column_orders
            .into_iter()
            .map(|mut o| {
                if let ExprImpl::InputRef(input_ref) = &project.exprs()[o.column_index] {
                    o.column_index = input_ref.index as usize;
                } else {
                    unreachable!("we only push down when expr is InputRef");
                }
                o
            })
            .collect_vec();

        let mut new_topn_order = top_n.topn_order().clone();
        new_topn_order.column_orders = new_orders;

        let input = project.input().clone();
        let new_topn = top_n.clone_with_input_and_order(input, new_topn_order);

        Some(project.clone_with_input(new_topn.into()).into())
    }
}

impl TopNPushDownRule {
    pub fn create() -> BoxedRule {
        Box::new(TopNPushDownRule {})
    }
}

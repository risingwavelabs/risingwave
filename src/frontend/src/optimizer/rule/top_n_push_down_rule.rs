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
use crate::{expr::ExprImpl, optimizer::{plan_node::PlanTreeNodeUnary, PlanRef}};

pub struct TopNPushDownRule {}

impl Rule for TopNPushDownRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        // 只匹配 LogicalTopN
        let top_n = plan.as_logical_top_n()?;

        // 1. offset != 0 时，不下推
        if top_n.offset() != 0 {
            return Some(plan);
        }

        // 2. 没有排序列时，不下推
        let col_orders = &top_n.topn_order().column_orders;
        if col_orders.is_empty() {
            return Some(plan);
        }

        // 3. 必须是 Project 的输入
        let project_input = top_n.input();
        let project = project_input.as_logical_project()?;

        // 4. 如果 Project 中有不支持下推的表达式，也不下推
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

        // 5. 排序列必须映射到 Project 输出的 InputRef
        let can_pushdown = col_orders.iter().all(|field_order| {
            matches!(&project.exprs()[field_order.column_index], ExprImpl::InputRef(_))
        });
        if !can_pushdown {
            return Some(plan);
        }

        // 6. 构造新的 TopN：下推到 Project 的输入
        let new_orders = top_n
            .topn_order()
            .clone() // 先 clone 一个 Order
            .column_orders
            .into_iter()
            .map(|mut fo| {
                // fo.column_index 是 Project 输出的下标
                if let ExprImpl::InputRef(ir) = &project.exprs()[fo.column_index] {
                    fo.column_index = ir.index as usize; // 替换成 Scan 输出的下标
                } else {
                    unreachable!("we only push down when expr is InputRef");
                }
                fo
            })
            .collect_vec();

        // 2. 构造新的 Order
        let mut new_topn_order = top_n.topn_order().clone();
        new_topn_order.column_orders = new_orders;

        // 3. 用 project.input()（底层输入）构造新的 TopN
        let input = project.input().clone();
        let new_topn = top_n.clone_with_input_and_order(input, new_topn_order);

        // 4. 将新的 TopN 插回 Project 上层
        Some(project.clone_with_input(new_topn.into()).into())
    }
}

impl TopNPushDownRule {
    pub fn create() -> BoxedRule {
        Box::new(TopNPushDownRule {})
    }
}
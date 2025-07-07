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

use std::assert_matches::assert_matches;

use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_pb::common::PbDistanceType;

use crate::PlanRef;
use crate::expr::{Expr, ExprImpl, ExprType};
use crate::optimizer::plan_node::generic::{TopNLimit, VectorSearch};
use crate::optimizer::plan_node::{LogicalVectorSearch, PlanTreeNodeUnary};
use crate::optimizer::rule::{BoxedRule, Rule};

pub struct TopNToVectorSearchRule;

impl TopNToVectorSearchRule {
    pub fn create() -> BoxedRule {
        Box::new(TopNToVectorSearchRule)
    }
}

impl Rule for TopNToVectorSearchRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let top_n = plan.as_logical_top_n()?;
        if !top_n.group_key().is_empty() {
            // vector index applies for only singleton top n
            return None;
        }
        let TopNLimit::Simple(limit) = top_n.limit_attr() else {
            // vector index applies for only simple top n
            return None;
        };
        // vector index applies for only top n with one order column
        let [order]: &[ColumnOrder; 1] = top_n
            .topn_order()
            .column_orders
            .as_slice()
            .try_into()
            .ok()?;
        if order.order_type.is_descending() || order.order_type.nulls_are_smallest() {
            // vector index applies for only ascending order with nulls last
            return None;
        }

        let input = top_n.input();
        let projection = input.as_logical_project()?;

        let order_expr = &projection.exprs()[order.column_index];
        let ExprImpl::FunctionCall(call) = order_expr else {
            return None;
        };
        let distance_type = match call.func_type() {
            ExprType::L2Distance => PbDistanceType::L2,
            _ => {
                return None;
            }
        };
        assert_eq!(
            call.inputs().len(),
            2,
            "vector distance function should have exactly two arguments",
        );

        let [left, right]: &[_; 2] = call.inputs().try_into().unwrap();
        assert_matches!(left.return_type(), DataType::Vector(_));
        assert_matches!(right.return_type(), DataType::Vector(_));

        let core = VectorSearch {
            top_n: limit,
            left: left.clone(),
            right: right.clone(),
            cols_before_vector_distance: projection.exprs()[0..order.column_index].to_vec(),
            cols_after_vector_distance: projection.exprs()[order.column_index + 1..].to_vec(),
            input: projection.input(),
            distance_type,
        };

        Some(LogicalVectorSearch::with_core(core).into())
    }
}

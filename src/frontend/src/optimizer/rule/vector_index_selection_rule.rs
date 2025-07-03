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
use tracing::debug;

use crate::PlanRef;
use crate::expr::{ExprImpl, ExprType};
use crate::optimizer::plan_node::PlanTreeNodeUnary;
use crate::optimizer::plan_node::generic::TopNLimit;
use crate::optimizer::rule::Rule;

pub struct VectorIndexSelectionRule;

impl Rule for VectorIndexSelectionRule {
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
        let projection_input = projection.input();
        let scan = projection_input.as_logical_scan()?;
        if scan.vector_indexes().is_empty() {
            // vector index applies only when the scan has vector indexes
            return None;
        }

        let order_expr = &projection.exprs()[order.column_index];
        let ExprImpl::FunctionCall(call) = order_expr else {
            return None;
        };
        let ExprType::L2Distance = call.func_type() else {
            return None;
        };
        assert_eq!(
            call.inputs().len(),
            2,
            "L2Distance function should have exactly two arguments",
        );

        let [left, right]: &[_; 2] = call.inputs().try_into().unwrap();
        let todo = 0;
        let (input_ref, literal) = match (left, right) {
            (ExprImpl::InputRef(input), ExprImpl::Literal(literal)) => (input, literal),
            (ExprImpl::Literal(literal), ExprImpl::InputRef(input)) => (input, literal),
            _ => return None,
        };

        let todo = 0;
        assert_matches!(input_ref.data_type, DataType::Vector(_));
        let primary_table = scan.table_catalog();
        let primary_table_input_column_idx = scan.output_col_idx()[input_ref.index];
        debug!(?primary_table_input_column_idx, output_col_idx = ?scan.output_col_idx(), "primary table");
        let mut matched_vector_index = None;
        'next_vector_index: for vector_index in scan.vector_indexes() {
            debug!(?vector_index, "checking vector index");
            let todo = 0;
            match &vector_index.vector_item {
                ExprImpl::InputRef(input_ref) => {
                    if input_ref.index != primary_table_input_column_idx {
                        continue;
                    }
                }
                _ => {
                    continue;
                }
            }
            let mut vector_index_output_info_columns = vec![];
            for output_col_idx in scan.output_col_idx() {
                if *output_col_idx == input_ref.index {
                    continue;
                }
                if let Some(vector_index_col_id) = vector_index
                    .primary_to_secondary_mapping
                    .get(output_col_idx)
                {
                    // minus 1 because the first column is the vector column
                    vector_index_output_info_columns.push(*vector_index_col_id - 1);
                } else {
                    continue 'next_vector_index;
                }
            }
            matched_vector_index = Some((vector_index.clone(), vector_index_output_info_columns));
        }
        let (vector_index, vector_index_output_info_columns) = matched_vector_index?;
        debug!(
            ?vector_index,
            ?vector_index_output_info_columns,
            "matched vector index"
        );

        let todo = 0;
        let vector_literal = literal
            .get_data()
            .as_ref()
            .expect("todo non-empty?")
            .as_vector()
            .clone();

        debug!(?input, ?literal, "vector index selection rule");

        None
    }
}

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

use std::collections::{BTreeMap, HashSet};

use risingwave_common::util::sort_util::{ColumnOrder, OrderType};

use super::prelude::{PlanRef, *};
use crate::optimizer::plan_node::{LogicalScan, LogicalTopN, PlanTreeNodeUnary};
use crate::optimizer::property::Order;

pub struct TopNOnIndexRule {}

impl Rule<Logical> for TopNOnIndexRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let top_n: &LogicalTopN = plan.as_logical_top_n()?;
        let input = top_n.input();
        let scan = input.as_logical_scan()?;
        let order = top_n.topn_order();
        if order.is_any() || scan.table_indexes().is_empty() {
            return None;
        }

        let expanded_scan = (scan.output_col_idx() != scan.required_col_idx())
            .then(|| scan.clone_with_output_indices(scan.required_col_idx().clone()));
        let match_scan = expanded_scan.as_ref().unwrap_or(scan);
        let output_col_map = Self::output_col_map(match_scan);
        let prefix = Self::build_eq_const_prefix(match_scan, &output_col_map);
        let pk_order = Order::new(
            scan.primary_key()
                .iter()
                .map_while(|key| {
                    output_col_map
                        .get(&key.column_index)
                        .map(|&idx| ColumnOrder::new(idx, key.order_type))
                })
                .skip_while(|key| prefix.contains(key))
                .collect(),
        );
        if pk_order.satisfies(order) {
            return None;
        }

        let mut longest_prefix_len = 0;
        let mut selected_scan = None;
        for (index, fixed_prefix) in match_scan.indexes_satisfy_order_with_prefix(order, &prefix) {
            if fixed_prefix.len() >= longest_prefix_len {
                longest_prefix_len = fixed_prefix.len();
                if let Some(index_scan) = scan.to_index_scan_if_index_covered(index) {
                    selected_scan = Some(index_scan);
                }
            }
        }
        selected_scan.map(|scan| top_n.clone_with_input(scan.into()).into())
    }
}

impl TopNOnIndexRule {
    pub fn create() -> BoxedRule {
        Box::new(TopNOnIndexRule {})
    }

    fn output_col_map(scan: &LogicalScan) -> BTreeMap<usize, usize> {
        scan.output_col_idx()
            .iter()
            .cloned()
            .enumerate()
            .map(|(id, col)| (col, id))
            .collect()
    }

    fn build_eq_const_prefix(
        scan: &LogicalScan,
        output_col_map: &BTreeMap<usize, usize>,
    ) -> HashSet<ColumnOrder> {
        let input_refs = scan.predicate().get_eq_const_input_refs();
        input_refs
            .into_iter()
            .flat_map(|input_ref| {
                output_col_map
                    .get(&input_ref.index)
                    .into_iter()
                    .flat_map(|&output_idx| {
                        OrderType::all()
                            .into_iter()
                            .map(move |order_type| ColumnOrder {
                                column_index: output_idx,
                                order_type,
                            })
                    })
            })
            .collect()
    }
}

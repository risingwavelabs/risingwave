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
        let logical_top_n: &LogicalTopN = plan.as_logical_top_n()?;
        let logical_scan: LogicalScan = logical_top_n.input().as_logical_scan()?.to_owned();
        let order = logical_top_n.topn_order();
        if order.column_orders.is_empty() {
            return None;
        }
        if let Some(p) = self.try_on_pk(logical_top_n, logical_scan.clone(), order) {
            Some(p)
        } else {
            self.try_on_index(logical_top_n, logical_scan, order)
        }
    }
}

impl TopNOnIndexRule {
    pub fn create() -> BoxedRule {
        Box::new(TopNOnIndexRule {})
    }

    fn try_on_index(
        &self,
        logical_top_n: &LogicalTopN,
        logical_scan: LogicalScan,
        required_order: &Order,
    ) -> Option<PlanRef> {
        let prefix = Self::build_prefix_from_scan_predicates(&logical_scan);
        let order_satisfied_index =
            logical_scan.indexes_satisfy_order_with_prefix(required_order, &prefix);
        let mut longest_prefix: Option<Order> = None;
        let mut selected_index = None;
        for (index, prefix) in order_satisfied_index {
            if prefix.len() >= longest_prefix.as_ref().map_or(0, |p| p.len()) {
                longest_prefix = Some(prefix.clone());
                if let Some(index_scan) = logical_scan.to_index_scan_if_index_covered(index) {
                    selected_index = Some(
                        logical_top_n
                            .clone_with_input_and_prefix(index_scan.into(), prefix)
                            .into(),
                    );
                }
            }
        }
        selected_index
    }

    fn try_on_pk(
        &self,
        logical_top_n: &LogicalTopN,
        logical_scan: LogicalScan,
        order: &Order,
    ) -> Option<PlanRef> {
        let prefix = Self::build_prefix_from_scan_predicates(&logical_scan);
        let output_col_map = logical_scan
            .output_col_idx()
            .iter()
            .cloned()
            .enumerate()
            .map(|(id, col)| (col, id))
            .collect::<BTreeMap<_, _>>();
        let unmatched_idx = output_col_map.len();
        let primary_key = logical_scan.primary_key();
        let primary_key_order = Order {
            column_orders: primary_key
                .iter()
                .map(|o| {
                    ColumnOrder::new(
                        *output_col_map
                            .get(&o.column_index)
                            .unwrap_or(&unmatched_idx),
                        o.order_type,
                    )
                })
                .collect::<Vec<_>>(),
        };
        let mut pk_orders_iter = primary_key_order.column_orders.iter().cloned().peekable();
        let fixed_prefix = {
            let mut fixed_prefix = vec![];
            loop {
                match pk_orders_iter.peek() {
                    Some(order) if prefix.contains(order) => {
                        let order = pk_orders_iter.next().unwrap();
                        fixed_prefix.push(order);
                    }
                    _ => break,
                }
            }
            Order {
                column_orders: fixed_prefix,
            }
        };
        let remaining_orders = Order {
            column_orders: pk_orders_iter.collect(),
        };
        if !remaining_orders.satisfies(order) {
            return None;
        }
        Some(
            logical_top_n
                .clone_with_input_and_prefix(logical_scan.into(), fixed_prefix)
                .into(),
        )
    }

    fn build_prefix_from_scan_predicates(logical_scan: &LogicalScan) -> HashSet<ColumnOrder> {
        let scan_predicates = logical_scan.predicate();
        let input_refs = scan_predicates.get_eq_const_input_refs();
        input_refs
            .into_iter()
            .flat_map(|input_ref| {
                [
                    ColumnOrder {
                        column_index: input_ref.index,
                        order_type: OrderType::ascending_nulls_first(),
                    },
                    ColumnOrder {
                        column_index: input_ref.index,
                        order_type: OrderType::ascending_nulls_last(),
                    },
                    ColumnOrder {
                        column_index: input_ref.index,
                        order_type: OrderType::descending_nulls_first(),
                    },
                    ColumnOrder {
                        column_index: input_ref.index,
                        order_type: OrderType::descending_nulls_last(),
                    },
                ]
            })
            .collect()
    }
}

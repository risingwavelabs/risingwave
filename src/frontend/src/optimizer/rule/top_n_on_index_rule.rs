//  Copyright 2025 RisingWave Labs
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

use std::collections::{BTreeMap, HashSet};

use risingwave_common::util::sort_util::{ColumnOrder, OrderType};

use super::prelude::{PlanRef, *};
use crate::optimizer::plan_node::{LogicalProject, LogicalScan, LogicalTopN, PlanTreeNodeUnary};
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
        let output_len = logical_scan.output_col_idx().len();
        let scan_for_index = if logical_scan.output_col_idx() == logical_scan.required_col_idx() {
            logical_scan.clone()
        } else {
            // `required_col_idx` is built by appending predicate columns to `output_col_idx`,
            // so `output_col_idx` is always a prefix of `required_col_idx`.
            logical_scan.clone_with_output_indices(logical_scan.required_col_idx().clone())
        };
        let scan_predicates = scan_for_index.predicate();
        let output_col_map = scan_for_index
            .output_col_idx()
            .iter()
            .cloned()
            .enumerate()
            .map(|(id, col)| (col, id))
            .collect::<BTreeMap<_, _>>();
        let input_refs = scan_predicates.get_eq_const_input_refs();
        let prefix = input_refs
            .into_iter()
            .flat_map(|input_ref| {
                output_col_map
                    .get(&input_ref.index)
                    .into_iter()
                    .flat_map(|&output_idx| {
                        [
                            ColumnOrder {
                                column_index: output_idx,
                                order_type: OrderType::ascending_nulls_first(),
                            },
                            ColumnOrder {
                                column_index: output_idx,
                                order_type: OrderType::ascending_nulls_last(),
                            },
                            ColumnOrder {
                                column_index: output_idx,
                                order_type: OrderType::descending_nulls_first(),
                            },
                            ColumnOrder {
                                column_index: output_idx,
                                order_type: OrderType::descending_nulls_last(),
                            },
                        ]
                    })
            })
            .collect();
        let order_satisfied_index =
            scan_for_index.indexes_satisfy_order_with_prefix(required_order, &prefix);
        let mut longest_prefix: Option<Order> = None;
        let mut selected_index = None;
        for (index, prefix) in order_satisfied_index {
            if prefix.len() >= longest_prefix.as_ref().map_or(0, |p| p.len()) {
                longest_prefix = Some(prefix.clone());
                if let Some(index_scan) = scan_for_index.to_index_scan_if_index_covered(index) {
                    let top_n =
                        logical_top_n.clone_with_input_and_prefix(index_scan.into(), prefix);
                    selected_index = Some(if scan_for_index.output_col_idx().len() == output_len {
                        top_n.into()
                    } else {
                        LogicalProject::with_out_col_idx(top_n.into(), 0..output_len).into()
                    });
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
        let output_len = logical_scan.output_col_idx().len();
        let scan_for_pk = if logical_scan.output_col_idx() == logical_scan.required_col_idx() {
            logical_scan.clone()
        } else {
            // `required_col_idx` is built by appending predicate columns to `output_col_idx`,
            // so `output_col_idx` is always a prefix of `required_col_idx`.
            logical_scan.clone_with_output_indices(logical_scan.required_col_idx().clone())
        };
        let scan_predicates = scan_for_pk.predicate();
        let output_col_map = scan_for_pk
            .output_col_idx()
            .iter()
            .cloned()
            .enumerate()
            .map(|(id, col)| (col, id))
            .collect::<BTreeMap<_, _>>();
        let input_refs = scan_predicates.get_eq_const_input_refs();
        let prefix: HashSet<ColumnOrder> = input_refs
            .into_iter()
            .flat_map(|input_ref| {
                output_col_map
                    .get(&input_ref.index)
                    .into_iter()
                    .flat_map(|&output_idx| {
                        [
                            ColumnOrder {
                                column_index: output_idx,
                                order_type: OrderType::ascending_nulls_first(),
                            },
                            ColumnOrder {
                                column_index: output_idx,
                                order_type: OrderType::ascending_nulls_last(),
                            },
                            ColumnOrder {
                                column_index: output_idx,
                                order_type: OrderType::descending_nulls_first(),
                            },
                            ColumnOrder {
                                column_index: output_idx,
                                order_type: OrderType::descending_nulls_last(),
                            },
                        ]
                    })
            })
            .collect();
        let unmatched_idx = output_col_map.len();
        let primary_key = scan_for_pk.primary_key();
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
        let top_n =
            logical_top_n.clone_with_input_and_prefix(scan_for_pk.clone().into(), fixed_prefix);
        Some(if scan_for_pk.output_col_idx().len() == output_len {
            top_n.into()
        } else {
            LogicalProject::with_out_col_idx(top_n.into(), 0..output_len).into()
        })
    }
}

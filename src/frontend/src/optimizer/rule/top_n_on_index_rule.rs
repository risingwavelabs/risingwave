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

use std::collections::BTreeMap;

use risingwave_common::util::sort_util::{ColumnOrder, OrderType};

use super::{BoxedRule, Rule};
use crate::optimizer::PlanRef;
use crate::optimizer::plan_node::{LogicalScan, LogicalTopN, PlanTreeNodeUnary};
use crate::optimizer::property::Order;

pub struct TopNOnIndexRule {}

impl Rule for TopNOnIndexRule {
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
        let scan_predicates = logical_scan.predicate();
        let input_refs = scan_predicates.get_eq_const_input_refs();
        let prefix = input_refs
            .into_iter()
            .flat_map(|input_ref| vec![
                ColumnOrder {
                    column_index: input_ref.index,
                    order_type: OrderType::ascending(),
                },
                ColumnOrder {
                    column_index: input_ref.index,
                    order_type: OrderType::descending(),
                },
            ])
            .collect();
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
        if primary_key_order.satisfies(order) {
            Some(logical_top_n.clone_with_input(logical_scan.into()).into())
        } else {
            None
        }
    }
}

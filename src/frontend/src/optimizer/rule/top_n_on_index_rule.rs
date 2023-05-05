//  Copyright 2023 RisingWave Labs
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

use risingwave_common::util::sort_util::ColumnOrder;

use super::{BoxedRule, Rule};
use crate::optimizer::plan_node::{LogicalScan, LogicalTopN, PlanTreeNodeUnary};
use crate::optimizer::property::Order;
use crate::optimizer::PlanRef;

pub struct TopNOnIndexRule {}

impl Rule for TopNOnIndexRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let logical_top_n: &LogicalTopN = plan.as_logical_top_n()?;
        let logical_scan: LogicalScan = logical_top_n.input().as_logical_scan()?.to_owned();
        if !logical_scan.predicate().always_true() {
            return None;
        }
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
        let order_satisfied_index = logical_scan.indexes_satisfy_order(required_order);
        for index in order_satisfied_index {
            if let Some(mut index_scan) = logical_scan.to_index_scan_if_index_covered(index) {
                index_scan.set_chunk_size(
                    ((u32::MAX as u64)
                        .min(logical_top_n.limit_attr().limit() + logical_top_n.offset()))
                        as u32,
                );
                return Some(logical_top_n.clone_with_input(index_scan.into()).into());
            }
        }

        None
    }

    fn try_on_pk(
        &self,
        logical_top_n: &LogicalTopN,
        mut logical_scan: LogicalScan,
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
                .into_iter()
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
            logical_scan.set_chunk_size(
                ((u32::MAX as u64).min(logical_top_n.limit_attr().limit() + logical_top_n.offset()))
                    as u32,
            );
            Some(logical_top_n.clone_with_input(logical_scan.into()).into())
        } else {
            None
        }
    }
}

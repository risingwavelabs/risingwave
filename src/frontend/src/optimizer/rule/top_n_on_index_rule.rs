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

use risingwave_common::util::sort_util::OrderType;

use super::{BoxedRule, Rule};
use crate::optimizer::plan_node::{LogicalLimit, LogicalScan, LogicalTopN, PlanTreeNodeUnary};
use crate::optimizer::property::{Direction, FieldOrder, Order};
use crate::optimizer::PlanRef;

pub struct TopNOnIndexRule {}

impl Rule for TopNOnIndexRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let logical_top_n: &LogicalTopN = plan.as_logical_top_n()?;
        let logical_scan: LogicalScan = logical_top_n.input().as_logical_scan()?.to_owned();
        let order = logical_top_n.topn_order();
        if order.field_order.is_empty() {
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
        order: &Order,
    ) -> Option<PlanRef> {
        let index = logical_scan.indexes().iter().find(|idx| {
            Order {
                field_order: idx
                    .index_table
                    .pk()
                    .to_vec()
            }
            .supersets(order)
        })?;

        let p2s_mapping = index.primary_to_secondary_mapping();

        let mut index_scan = if logical_scan
            .required_col_idx()
            .iter()
            .all(|x| p2s_mapping.contains_key(x))
        {
            Some(logical_scan.to_index_scan(
                &index.name,
                index.index_table.table_desc().into(),
                p2s_mapping,
            ))
        } else {
            None
        }?;

        index_scan.set_chunk_size(
            ((u32::MAX as u64).min(logical_top_n.limit() + logical_top_n.offset())) as u32,
        );

        let logical_limit = LogicalLimit::create(
            index_scan.into(),
            logical_top_n.limit(),
            logical_top_n.offset(),
        );
        Some(logical_limit)
    }

    fn try_on_pk(
        &self,
        logical_top_n: &LogicalTopN,
        mut logical_scan: LogicalScan,
        order: &Order,
    ) -> Option<PlanRef> {
        let primary_key = logical_scan.primary_key();
        let primary_key_order = Order {
            field_order: primary_key
                .into_iter()
                .map(|op| FieldOrder {
                    index: op.column_idx,
                    direct: if op.order_type == OrderType::Ascending {
                        Direction::Asc
                    } else {
                        Direction::Desc
                    },
                })
                .collect::<Vec<_>>(),
        };
        if primary_key_order.supersets(order) {
            logical_scan.set_chunk_size(
                ((u32::MAX as u64).min(logical_top_n.limit() + logical_top_n.offset())) as u32,
            );
            let logical_limit = LogicalLimit::create(
                logical_scan.into(),
                logical_top_n.limit(),
                logical_top_n.offset(),
            );
            Some(logical_limit)
        } else {
            None
        }
    }
}

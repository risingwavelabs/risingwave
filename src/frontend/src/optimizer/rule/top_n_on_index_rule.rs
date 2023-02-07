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

use super::{BoxedRule, Rule};
use crate::optimizer::plan_node::{LogicalLimit, LogicalScan, LogicalTopN, PlanTreeNodeUnary};
use crate::optimizer::property::{FieldOrder, Order};
use crate::optimizer::PlanRef;

pub struct TopNOnIndexRule {}

impl Rule for TopNOnIndexRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let logical_topn: &LogicalTopN = plan.as_logical_top_n()?;
        let logical_scan: LogicalScan = logical_topn.input().as_logical_scan()?.to_owned();
        let order = logical_topn.topn_order();
        if order.field_order.is_empty() {
            return None;
        }
        let index = logical_scan.indexes().iter().find(|idx| {
            Order {
                field_order: idx
                    .index_table
                    .pk()
                    .iter()
                    .map(|idx_item| FieldOrder {
                        index: idx_item.index,
                        direct: idx_item.direct,
                    })
                    .collect(),
            }
            .satisfies(order)
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
            ((u32::MAX as u64).min(logical_topn.limit() + logical_topn.offset())) as usize,
        );

        let logical_limit = LogicalLimit::create(
            index_scan.into(),
            logical_topn.limit(),
            logical_topn.offset(),
        );
        Some(logical_limit)
    }
}

impl TopNOnIndexRule {
    pub fn create() -> BoxedRule {
        Box::new(TopNOnIndexRule {})
    }
}

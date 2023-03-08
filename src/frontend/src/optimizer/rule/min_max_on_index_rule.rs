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

use itertools::Itertools;
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::Direction;
use risingwave_expr::expr::AggKind;

use super::{BoxedRule, Rule};
use crate::expr::{ExprImpl, ExprType, FunctionCall, InputRef};
use crate::optimizer::plan_node::{
    LogicalAgg, LogicalFilter, LogicalLimit, LogicalScan, PlanAggCall, PlanTreeNodeUnary,
};
use crate::optimizer::property::{FieldOrder, Order};
use crate::optimizer::PlanRef;
use crate::utils::Condition;

pub struct MinMaxOnIndexRule {}

impl Rule for MinMaxOnIndexRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let logical_agg: &LogicalAgg = plan.as_logical_agg()?;
        if !logical_agg.group_key().is_empty() {
            return None;
        }
        let calls = logical_agg.agg_calls();
        if calls.is_empty() {
            return None;
        }
        let first_call = calls.iter().exactly_one().ok()?;

        if matches!(first_call.agg_kind, AggKind::Min | AggKind::Max)
            && !first_call.distinct
            && first_call.filter.always_true()
            && first_call.order_by.is_empty()
        {
            let logical_scan: LogicalScan = logical_agg.input().as_logical_scan()?.to_owned();
            let kind = calls.first()?.agg_kind;
            if !logical_scan.predicate().always_true() {
                return None;
            }
            let output_col_map = logical_scan
                .output_col_idx()
                .iter()
                .cloned()
                .enumerate()
                .map(|(id, col)| (col, id))
                .collect::<BTreeMap<_, _>>();
            let order = Order {
                field_order: vec![FieldOrder {
                    index: calls.first()?.inputs.first()?.index(),
                    direct: if kind == AggKind::Min {
                        Direction::Ascending
                    } else {
                        Direction::Descending
                    },
                }],
            };
            if let Some(p) =
                self.try_on_index(logical_agg, logical_scan.clone(), &order, &output_col_map)
            {
                Some(p)
            } else {
                self.try_on_pk(logical_agg, logical_scan, &order, &output_col_map)
            }
        } else {
            None
        }
    }
}

impl MinMaxOnIndexRule {
    pub fn create() -> BoxedRule {
        Box::new(MinMaxOnIndexRule {})
    }

    fn try_on_index(
        &self,
        logical_agg: &LogicalAgg,
        logical_scan: LogicalScan,
        order: &Order,
        output_col_map: &BTreeMap<usize, usize>,
    ) -> Option<PlanRef> {
        let unmatched_idx = output_col_map.len();
        let index = logical_scan.indexes().iter().find(|idx| {
            let s2p_mapping = idx.secondary_to_primary_mapping();
            Order {
                field_order: idx
                    .index_table
                    .pk()
                    .iter()
                    .map(|idx_item| FieldOrder {
                        index: *output_col_map
                            .get(
                                s2p_mapping
                                    .get(&idx_item.index)
                                    .expect("should be in s2p mapping"),
                            )
                            .unwrap_or(&unmatched_idx),
                        direct: idx_item.direct,
                    })
                    .collect(),
            }
            .satisfies(order)
        })?;

        let p2s_mapping = index.primary_to_secondary_mapping();

        let index_scan = if logical_scan
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

        let non_null_filter = LogicalFilter::create_with_expr(
            index_scan.into(),
            FunctionCall::new_unchecked(
                ExprType::IsNotNull,
                vec![ExprImpl::InputRef(Box::new(InputRef::new(
                    0,
                    logical_agg.schema().fields[0].data_type.clone(),
                )))],
                DataType::Boolean,
            )
            .into(),
        );

        let limit = LogicalLimit::create(non_null_filter, 1, 0);

        let formatting_agg = LogicalAgg::new(
            vec![PlanAggCall {
                agg_kind: logical_agg.agg_calls().first()?.agg_kind,
                return_type: logical_agg.schema().fields[0].data_type.clone(),
                inputs: vec![InputRef::new(
                    0,
                    logical_agg.schema().fields[0].data_type.clone(),
                )],
                order_by: vec![],
                distinct: false,
                filter: Condition {
                    conjunctions: vec![],
                },
            }],
            vec![],
            limit,
        );

        Some(formatting_agg.into())
    }

    fn try_on_pk(
        &self,
        logical_agg: &LogicalAgg,
        logical_scan: LogicalScan,
        order: &Order,
        output_col_map: &BTreeMap<usize, usize>,
    ) -> Option<PlanRef> {
        let unmatched_idx = output_col_map.len();
        let primary_key = logical_scan.primary_key();
        let primary_key_order = Order {
            field_order: primary_key
                .into_iter()
                .map(|op| FieldOrder {
                    index: *output_col_map.get(&op.column_idx).unwrap_or(&unmatched_idx),
                    direct: op.order_type.direction(),
                })
                .collect::<Vec<_>>(),
        };
        if primary_key_order.satisfies(order) {
            let non_null_filter = LogicalFilter::create_with_expr(
                logical_scan.into(),
                FunctionCall::new_unchecked(
                    ExprType::IsNotNull,
                    vec![ExprImpl::InputRef(Box::new(InputRef::new(
                        0,
                        logical_agg.schema().fields[0].data_type.clone(),
                    )))],
                    DataType::Boolean,
                )
                .into(),
            );

            let limit = LogicalLimit::create(non_null_filter, 1, 0);

            let formatting_agg = LogicalAgg::new(
                vec![PlanAggCall {
                    agg_kind: logical_agg.agg_calls().first()?.agg_kind,
                    return_type: logical_agg.schema().fields[0].data_type.clone(),
                    inputs: vec![InputRef::new(
                        0,
                        logical_agg.schema().fields[0].data_type.clone(),
                    )],
                    order_by: vec![],
                    distinct: false,
                    filter: Condition {
                        conjunctions: vec![],
                    },
                }],
                vec![],
                limit,
            );

            Some(formatting_agg.into())
        } else {
            None
        }
    }
}

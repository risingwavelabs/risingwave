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
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_expr::function::aggregate::AggKind;

use super::{BoxedRule, Rule};
use crate::expr::{ExprImpl, ExprType, FunctionCall, InputRef};
use crate::optimizer::plan_node::generic::Agg;
use crate::optimizer::plan_node::{
    LogicalAgg, LogicalFilter, LogicalLimit, LogicalScan, PlanAggCall, PlanTreeNodeUnary,
};
use crate::optimizer::property::Order;
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
            let order = Order {
                column_orders: vec![ColumnOrder::new(
                    calls.first()?.inputs.first()?.index(),
                    if kind == AggKind::Min {
                        OrderType::ascending()
                    } else {
                        OrderType::descending()
                    },
                )],
            };
            if let Some(p) = self.try_on_index(logical_agg, logical_scan.clone(), &order) {
                Some(p)
            } else {
                self.try_on_pk(logical_agg, logical_scan, &order)
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
        required_order: &Order,
    ) -> Option<PlanRef> {
        let order_satisfied_index = logical_scan.indexes_satisfy_order(required_order);
        for index in order_satisfied_index {
            if let Some(index_scan) = logical_scan.to_index_scan_if_index_covered(index) {
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

                let formatting_agg = Agg::new(
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

                return Some(formatting_agg.into());
            }
        }

        None
    }

    fn try_on_pk(
        &self,
        logical_agg: &LogicalAgg,
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

            let formatting_agg = Agg::new(
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

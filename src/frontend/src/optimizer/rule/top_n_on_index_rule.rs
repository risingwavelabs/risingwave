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

use std::collections::BTreeMap;

use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::ColumnOrder;

use super::prelude::{PlanRef, *};
use crate::expr::{Expr, ExprImpl, InputRef, Literal};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{LogicalProject, LogicalScan, LogicalTopN, PlanTreeNodeUnary};
use crate::optimizer::property::Order;
use crate::utils::ColIndexMapping;

pub struct TopNOnIndexRule {}

impl Rule<Logical> for TopNOnIndexRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let top_n: &LogicalTopN = plan.as_logical_top_n()?;
        let input = top_n.input();
        let scan = input.as_logical_scan()?;
        let order = top_n.topn_order();
        if order.is_any() || (scan.table_indexes().is_empty() && scan.predicate().always_true()) {
            return None;
        }

        let expanded_scan = (scan.output_col_idx() != scan.required_col_idx())
            .then(|| scan.clone_with_output_indices(scan.required_col_idx().clone()));
        let match_scan = expanded_scan.as_ref().unwrap_or(scan);
        let output_col_map = Self::output_col_map(match_scan);
        let constants: BTreeMap<_, _> = scan
            .predicate()
            .conjunctions
            .iter()
            .filter_map(ExprImpl::as_eq_const)
            .map(|(col, value)| (output_col_map[&col.index], value))
            .collect();
        let is_fixed = |idx: usize| constants.contains_key(&idx);
        let pk_order = Order::new(
            scan.primary_key()
                .iter()
                .map_while(|key| {
                    output_col_map
                        .get(&key.column_index)
                        .map(|&idx| ColumnOrder::new(idx, key.order_type))
                })
                .collect(),
        );
        if let Some(fixed_prefix) = pk_order.fixed_prefix_for(order, is_fixed) {
            return Self::project_fixed_prefix(top_n, scan, fixed_prefix, &constants);
        }

        let mut longest_prefix_len = 0;
        let mut selected_scan = None;
        for (index, fixed_prefix) in match_scan.indexes_satisfy_order_with_prefix(order, is_fixed) {
            if fixed_prefix.len() >= longest_prefix_len {
                longest_prefix_len = fixed_prefix.len();
                if let Some(index_scan) = scan.to_index_scan_if_index_covered(index) {
                    selected_scan = Some((index_scan, fixed_prefix));
                }
            }
        }
        let (scan, prefix) = selected_scan?;
        Some(
            Self::project_fixed_prefix(top_n, &scan, &prefix.column_orders, &constants)
                .unwrap_or_else(|| top_n.clone_with_input(scan.into()).into()),
        )
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

    fn project_fixed_prefix(
        top_n: &LogicalTopN,
        scan: &LogicalScan,
        prefix: &[ColumnOrder],
        constants: &BTreeMap<usize, ExprImpl>,
    ) -> Option<PlanRef> {
        let mut literals = BTreeMap::new();
        for key in prefix {
            let idx = key.column_index;
            if idx >= scan.output_col_idx().len()
                || top_n.group_key().contains(&idx)
                || top_n
                    .topn_order()
                    .column_orders
                    .iter()
                    .any(|key| key.column_index == idx)
            {
                continue;
            }
            let ty = &scan.schema()[idx].data_type;
            if let Some(literal) = Self::exact_literal(&constants[&idx], ty) {
                literals.insert(idx, literal);
            }
        }
        if literals.is_empty() {
            return None;
        }

        let mut retained = Vec::new();
        let mut output_cols = Vec::new();
        let mut exprs = Vec::new();
        for (idx, &table_col) in scan.output_col_idx().iter().enumerate() {
            if let Some(literal) = literals.remove(&idx) {
                exprs.push(literal);
            } else {
                exprs.push(
                    InputRef::new(retained.len(), scan.schema()[idx].data_type.clone()).into(),
                );
                retained.push(idx);
                output_cols.push(table_col);
            }
        }
        let mapping = ColIndexMapping::with_remaining_columns(&retained, exprs.len());
        let scan = scan.clone_with_output_indices(output_cols);
        let (top_n, _) = top_n.rewrite_with_input(scan.into(), mapping);
        Some(LogicalProject::create(top_n.into(), exprs))
    }

    fn exact_literal(value: &ExprImpl, ty: &DataType) -> Option<ExprImpl> {
        if !matches!(
            ty,
            DataType::Boolean
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Int256
                | DataType::Serial
                | DataType::Varchar
                | DataType::Bytea
                | DataType::Date
                | DataType::Time
                | DataType::Timestamp
                | DataType::Timestamptz
        ) {
            return None;
        }
        let value = if &value.return_type() == ty {
            value.clone()
        } else if ty.is_int() && value.return_type().is_int() {
            value.clone().cast_explicit(ty).ok()?
        } else {
            return None;
        };
        let datum = value.try_fold_const()?.ok()??;
        Some(Literal::new(Some(datum), ty.clone()).into())
    }
}

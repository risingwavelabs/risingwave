// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp::min;

use risingwave_common::types::{
    DataType, Decimal, IntervalUnit, NaiveDateTimeWrapper, NaiveDateWrapper, NaiveTimeWrapper,
};

use super::{BoxedRule, Rule};
use crate::expr::{ExprImpl, ExprType, ExprVisitor, FunctionCall};
use crate::optimizer::plan_node::LogicalScan;
use crate::optimizer::PlanRef;
use crate::utils::Condition;

/// index selection cost matrix
///
/// `column_idx`  0    1     2    3    4
///
/// Equal      | 1  | 1  | 1  | 1  | 1  |
///
/// In         | 10 | 8  | 5  | 5  | 5  |    // take the minimum value with actual in number
///
/// Range      | 500| 50 | 20 | 10 | 10 |
///
/// All        |10000| 100| 50 | 20 | 10 |
///
/// total cost = cost(match type of 0 idx)
///             * cost(match type of 1 idx)
///             * ... cost(match type of the last idx)
///
/// For Example:
/// index(a, b, c)
/// for a = 1 and b = 1 and c = 1, its cost is 1 = Equal0 * Equal1 * Equal2 = 1
/// for a in (xxx) and b = 1 and c = 1, its cost is In0 * Equal1 * Equal2 = 10
/// for a = 1 and b in (xxx), its cost is Equal0 * In1 * All2 = 1 * 8 * 50 = 400
/// for a between xxx and yyy, its cost is Range0 = 500
/// for a = 1 and b between xxx and yyy, its cost is Equal0 * Range1 = 50
/// for a = 1, its cost is 100 = Equal0 * All1 = 100
/// for no condition, its cost is All0 = 10000
///
/// With the assumption that the most effective part of a index is its prefix,
/// cost decreases as `column_idx` increasing.
/// for index length > 5, we just ignore the rest.

const INDEX_MAX_LEN: usize = 5;
const INDEX_COST_MATRIX: [[usize; INDEX_MAX_LEN]; 4] = [
    [1, 1, 1, 1, 1],
    [10, 8, 5, 5, 5],
    [100, 50, 20, 10, 10],
    [10000, 100, 50, 20, 20],
];

pub struct IndexSelectionRule {}
impl Rule for IndexSelectionRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let logical_scan: &LogicalScan = plan.as_logical_scan()?;
        let indexes = logical_scan.indexes();
        if indexes.is_empty() {
            return None;
        }

        let mut primary_table_scan_io_estimator = TableScanIoEstimator::new(logical_scan);
        let primary_cost = primary_table_scan_io_estimator.estimate(logical_scan.predicate());

        let mut final_plan = logical_scan.clone();
        let mut min_cost = primary_cost.clone();

        let required_col_idx = logical_scan.required_col_idx();
        for index in indexes {
            let p2s_mapping = index.primary_to_secondary_mapping();
            if required_col_idx.iter().all(|x| p2s_mapping.contains_key(x)) {
                // covering index selection
                let index_scan = logical_scan.to_index_scan(
                    &index.name,
                    index.index_table.table_desc().into(),
                    &p2s_mapping,
                );

                let mut index_table_scan_io_estimator = TableScanIoEstimator::new(&index_scan);
                let index_cost = index_table_scan_io_estimator.estimate(index_scan.predicate());
                if index_cost.le(&min_cost) {
                    min_cost = index_cost;
                    final_plan = index_scan;
                }
            } else {
                // TODO: non-covering index selection
            }
        }

        if min_cost == primary_cost {
            None
        } else {
            Some(final_plan.into())
        }
    }
}

struct TableScanIoEstimator<'a> {
    table_scan: &'a LogicalScan,
    row_size: usize,
}

impl<'a> TableScanIoEstimator<'a> {
    pub fn new(table_scan: &'a LogicalScan) -> Self {
        Self {
            table_scan,
            row_size: table_scan
                .table_desc()
                .columns
                .iter()
                .map(|x| TableScanIoEstimator::estimate_data_type_size(&x.data_type))
                .reduce(|x, y| x + y)
                .unwrap(),
        }
    }

    pub fn estimate_data_type_size(data_type: &DataType) -> usize {
        use std::mem::size_of;

        match data_type {
            DataType::Boolean => size_of::<bool>(),
            DataType::Int16 => size_of::<i16>(),
            DataType::Int32 => size_of::<i32>(),
            DataType::Int64 => size_of::<i64>(),
            DataType::Float32 => size_of::<f32>(),
            DataType::Float64 => size_of::<f64>(),
            DataType::Decimal => size_of::<Decimal>(),
            DataType::Date => size_of::<NaiveDateWrapper>(),
            DataType::Time => size_of::<NaiveTimeWrapper>(),
            DataType::Timestamp => size_of::<NaiveDateTimeWrapper>(),
            DataType::Timestampz => size_of::<i64>(),
            DataType::Interval => size_of::<IntervalUnit>(),
            DataType::Varchar => 20,
            DataType::Struct { .. } => 20,
            DataType::List { .. } => 20,
        }
    }

    pub fn estimate(&mut self, predicate: &Condition) -> IndexCost {
        // try to deal with OR condition
        if predicate.conjunctions.len() == 1 {
            self.visit_expr(&predicate.conjunctions[0])
        } else {
            self.estimate_conjunctions(&predicate.conjunctions)
        }
    }

    fn estimate_conjunctions(&mut self, conjunctions: &[ExprImpl]) -> IndexCost {
        let order_column_indices = self.table_scan.table_desc().order_column_indices();

        let mut new_conjunctions = conjunctions.to_owned();

        let mut match_item_vec = vec![];

        for column_idx in order_column_indices {
            let match_item = self.match_index_column(column_idx, &mut new_conjunctions);
            // seeing range, we don't need to match anymore.
            let should_break = match match_item {
                MatchItem::Equal | MatchItem::In(_) => false,
                MatchItem::Range | MatchItem::All => true,
            };
            match_item_vec.push(match_item);
            if should_break {
                break;
            }
        }

        let index_cost = match_item_vec
            .iter()
            .enumerate()
            .take(INDEX_MAX_LEN)
            .map(|(i, match_item)| match match_item {
                MatchItem::Equal => INDEX_COST_MATRIX[0][i],
                MatchItem::In(num) => min(INDEX_COST_MATRIX[1][i], *num),
                MatchItem::Range => INDEX_COST_MATRIX[2][i],
                MatchItem::All => INDEX_COST_MATRIX[3][i],
            })
            .reduce(|x, y| x * y)
            .unwrap();

        IndexCost::new(index_cost * self.row_size)
    }

    fn match_index_column(
        &mut self,
        column_idx: usize,
        conjunctions: &mut Vec<ExprImpl>,
    ) -> MatchItem {
        // Equal
        for (i, expr) in conjunctions.iter().enumerate() {
            if let Some((input_ref, _const_expr)) = expr.as_eq_const()
                && input_ref.index == column_idx {
                    conjunctions.remove(i);
                    return MatchItem::Equal;
            }
        }

        // In
        for (i, expr) in conjunctions.iter().enumerate() {
            if let Some((input_ref, in_const_list)) = expr.as_in_const_list()
                && input_ref.index == column_idx {
                conjunctions.remove(i);
                return MatchItem::In(in_const_list.len());
            }
        }

        // Range
        for (i, expr) in conjunctions.iter().enumerate() {
            if let Some((input_ref, _op, _const_expr)) = expr.as_comparison_const()
                && input_ref.index == column_idx {
                conjunctions.remove(i);
                return MatchItem::Range;
            }
        }

        MatchItem::All
    }
}

enum MatchItem {
    Equal,
    In(usize),
    Range,
    All,
}

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
struct IndexCost(usize);

impl Default for IndexCost {
    fn default() -> Self {
        Self(IndexCost::maximum())
    }
}

impl IndexCost {
    fn new(cost: usize) -> IndexCost {
        Self(min(cost, IndexCost::maximum()))
    }

    fn maximum() -> usize {
        100000
    }

    fn add(&self, other: &IndexCost) -> IndexCost {
        IndexCost::new(self.0 + other.0)
    }

    fn le(&self, other: &IndexCost) -> bool {
        self.0 < other.0
    }
}

impl ExprVisitor<IndexCost> for TableScanIoEstimator<'_> {
    fn visit_function_call(&mut self, func_call: &FunctionCall) -> IndexCost {
        match func_call.get_expr_type() {
            ExprType::Or => func_call
                .inputs()
                .iter()
                .map(|x| self.visit_expr(x))
                .reduce(|x, y| x.add(&y))
                .unwrap(),
            ExprType::And => self.estimate_conjunctions(func_call.inputs()),
            _ => {
                let single = vec![ExprImpl::FunctionCall(func_call.clone().into())];
                self.estimate_conjunctions(&single)
            }
        }
    }
}

impl IndexSelectionRule {
    pub fn create() -> BoxedRule {
        Box::new(IndexSelectionRule {})
    }
}

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
use std::collections::HashMap;

use itertools::Itertools;
use risingwave_common::session_config::QueryMode;
use risingwave_common::types::{
    DataType, Decimal, IntervalUnit, NaiveDateTimeWrapper, NaiveDateWrapper, NaiveTimeWrapper,
};
use risingwave_pb::plan_common::JoinType;

use super::{BoxedRule, Rule};
use crate::catalog::IndexCatalog;
use crate::expr::{Expr, ExprImpl, ExprRewriter, ExprType, ExprVisitor, FunctionCall, InputRef};
use crate::optimizer::plan_node::{
    LogicalJoin, LogicalScan, PlanTreeNodeBinary, PredicatePushdown,
};
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
/// All        |10000| 100| 30 | 20 | 10 |
///
/// total cost = cost(match type of 0 idx)
///             * cost(match type of 1 idx)
///             * ... cost(match type of the last idx)
///
/// For Example:
/// index order key (a, b, c)
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
/// for index order key length > 5, we just ignore the rest.

const INDEX_MAX_LEN: usize = 5;
const INDEX_COST_MATRIX: [[usize; INDEX_MAX_LEN]; 4] = [
    [1, 1, 1, 1, 1],
    [10, 8, 5, 5, 5],
    [500, 50, 20, 10, 10],
    [10000, 100, 30, 20, 20],
];
const LOOKUP_COST_CONST: usize = 3;

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

        let mut final_plan: PlanRef = logical_scan.clone().into();
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
                    final_plan = index_scan.into();
                }
            } else {
                // non-covering index selection
                // only enable non-covering index selection when lookup join is enabled
                let config = logical_scan.base.ctx.inner().session_ctx.config();
                if config.get_batch_enable_lookup_join()
                    && config.get_query_mode() == QueryMode::Local
                {
                    let (index_lookup, lookup_cost) =
                        self.gen_index_lookup(logical_scan, index, p2s_mapping);
                    if lookup_cost.le(&min_cost) {
                        min_cost = lookup_cost;
                        final_plan = index_lookup;
                    }
                }
            }
        }

        // TODO: support merge index

        if min_cost == primary_cost {
            None
        } else {
            Some(final_plan)
        }
    }
}

struct Rewriter {
    p2s_mapping: HashMap<usize, usize>,
    offset: usize,
}
impl ExprRewriter for Rewriter {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        // transform primary predicate to index predicate if it can
        if self.p2s_mapping.contains_key(&input_ref.index) {
            InputRef::new(
                *self.p2s_mapping.get(&input_ref.index()).unwrap(),
                input_ref.return_type(),
            )
            .into()
        } else {
            InputRef::new(input_ref.index() + self.offset, input_ref.return_type()).into()
        }
    }
}

impl IndexSelectionRule {
    fn gen_index_lookup(
        &self,
        logical_scan: &LogicalScan,
        index: &IndexCatalog,
        p2s_mapping: HashMap<usize, usize>,
    ) -> (PlanRef, IndexCost) {
        // 1. logical_scan ->  logical_join
        //                      /        \
        //                index_scan   primary_table_scan
        let predicate = logical_scan.predicate().clone();
        let offset = index.index_item.len();
        let mut rewriter = Rewriter {
            p2s_mapping,
            offset,
        };
        let new_predicate = predicate.rewrite_expr(&mut rewriter);

        let index_scan = LogicalScan::create(
            index.index_table.name.clone(),
            false,
            index.index_table.table_desc().into(),
            vec![],
            logical_scan.ctx(),
        );

        let primary_table_scan = LogicalScan::create(
            index.primary_table.name.clone(),
            false,
            index.primary_table.table_desc().into(),
            vec![],
            logical_scan.ctx(),
        );

        let conjunctions = index
            .primary_table_order_key_ref_to_index_table()
            .iter()
            .zip_eq(index.primary_table.order_key.iter())
            .map(|(x, y)| {
                ExprImpl::FunctionCall(Box::new(FunctionCall::new_unchecked(
                    ExprType::Equal,
                    vec![
                        ExprImpl::InputRef(Box::new(InputRef::new(
                            x.index,
                            index.index_table.columns[x.index].data_type().clone(),
                        ))),
                        ExprImpl::InputRef(Box::new(InputRef::new(
                            y.index + index.index_item.len(),
                            index.primary_table.columns[y.index].data_type().clone(),
                        ))),
                    ],
                    DataType::Boolean,
                )))
            })
            .chain(new_predicate.into_iter())
            .collect_vec();
        let on = Condition { conjunctions };
        let join = LogicalJoin::new(
            index_scan.into(),
            primary_table_scan.into(),
            JoinType::Inner,
            on,
        );

        // 2. push down predicate, so we can calculate the cost of index lookup
        let join_ref = join.predicate_pushdown(Condition::true_cond());

        let join_with_predicate_push_down =
            join_ref.as_logical_join().expect("must be a logical join");
        let new_join_left = join_with_predicate_push_down.left();
        let index_scan_with_predicate: &LogicalScan = new_join_left
            .as_logical_scan()
            .expect("must be a logical scan");

        // 3. calculate the cost
        let mut index_table_scan_io_estimator =
            TableScanIoEstimator::new(index_scan_with_predicate);
        let index_cost =
            index_table_scan_io_estimator.estimate(index_scan_with_predicate.predicate());
        // lookup cost = index cost * LOOKUP_COST_CONST
        let lookup_cost = index_cost.mul(&IndexCost::new(LOOKUP_COST_CONST));

        // 4. keep the same schema with original logical_scan
        let scan_output_col_idx = logical_scan.output_col_idx();
        let lookup_join = join_ref.prune_col(
            &scan_output_col_idx
                .iter()
                .map(|&col_idx| col_idx + offset)
                .collect_vec(),
        );

        (lookup_join, lookup_cost)
    }
}

struct TableScanIoEstimator<'a> {
    table_scan: &'a LogicalScan,
    row_size: usize,
}

impl<'a> TableScanIoEstimator<'a> {
    pub fn new(table_scan: &'a LogicalScan) -> Self {
        // 5 for table_id + 1 for vnode + 8 for epoch
        let row_meta_field_estimate_size = 14_usize;
        let table_desc = table_scan.table_desc();
        Self {
            table_scan,
            row_size: row_meta_field_estimate_size
                + table_desc
                    .columns
                    .iter()
                    // add order key twice for its appearance both in key and value
                    .chain(
                        table_desc
                            .order_key
                            .iter()
                            .map(|x| &table_desc.columns[x.column_idx]),
                    )
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

        IndexCost::new(index_cost).mul(&IndexCost::new(self.row_size))
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
        10000000
    }

    fn add(&self, other: &IndexCost) -> IndexCost {
        IndexCost::new(
            self.0
                .checked_add(other.0)
                .unwrap_or_else(IndexCost::maximum),
        )
    }

    fn mul(&self, other: &IndexCost) -> IndexCost {
        IndexCost::new(
            self.0
                .checked_mul(other.0)
                .unwrap_or_else(IndexCost::maximum),
        )
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

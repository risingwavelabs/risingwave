// Copyright 2025 RisingWave Labs
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

//! # Index selection cost matrix
//!
//! |`column_idx`| 0   |  1 | 2  | 3  | 4  | remark |
//! |-----------|-----|----|----|----|----|---|
//! |Equal      | 1   | 1  | 1  | 1  | 1  | |
//! |In         | 10  | 8  | 5  | 5  | 5  | take the minimum value with actual in number |
//! |Range(Two) | 600 | 50 | 20 | 10 | 10 | `RangeTwoSideBound` like a between 1 and 2 |
//! |Range(One) | 1400| 70 | 25 | 15 | 10 | `RangeOneSideBound` like a > 1, a >= 1, a < 1|
//! |All        | 4000| 100| 30 | 20 | 10 | |
//!
//! ```text
//! index cost = cost(match type of 0 idx)
//! * cost(match type of 1 idx)
//! * ... cost(match type of the last idx)
//! ```
//!
//! ## Example
//!
//! Given index order key (a, b, c)
//!
//! - For `a = 1 and b = 1 and c = 1`, its cost is 1 = Equal0 * Equal1 * Equal2 = 1
//! - For `a in (xxx) and b = 1 and c = 1`, its cost is In0 * Equal1 * Equal2 = 10
//! - For `a = 1 and b in (xxx)`, its cost is Equal0 * In1 * All2 = 1 * 8 * 50 = 400
//! - For `a between xxx and yyy`, its cost is Range(Two)0 = 600
//! - For `a = 1 and b between xxx and yyy`, its cost is Equal0 * Range(Two)1 = 50
//! - For `a = 1 and b > 1`, its cost is Equal0 * Range(One)1 = 70
//! - For `a = 1`, its cost is 100 = Equal0 * All1 = 100
//! - For no condition, its cost is All0 = 4000
//!
//! With the assumption that the most effective part of a index is its prefix,
//! cost decreases as `column_idx` increasing.
//!
//! For index order key length > 5, we just ignore the rest.

use std::cmp::min;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

use itertools::Itertools;
use risingwave_common::catalog::Schema;
use risingwave_common::types::{
    DataType, Date, Decimal, Int256, Interval, Serial, Time, Timestamp, Timestamptz,
};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::plan_common::JoinType;
use risingwave_sqlparser::ast::AsOf;

use super::{BoxedRule, Rule};
use crate::catalog::IndexCatalog;
use crate::expr::{
    Expr, ExprImpl, ExprRewriter, ExprType, ExprVisitor, FunctionCall, InputRef, to_conjunctions,
    to_disjunctions,
};
use crate::optimizer::PlanRef;
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{
    ColumnPruningContext, LogicalJoin, LogicalScan, LogicalUnion, PlanTreeNode, PlanTreeNodeBinary,
    PredicatePushdown, PredicatePushdownContext, generic,
};
use crate::utils::Condition;

const INDEX_MAX_LEN: usize = 5;
const INDEX_COST_MATRIX: [[usize; INDEX_MAX_LEN]; 5] = [
    [1, 1, 1, 1, 1],
    [10, 8, 5, 5, 5],
    [600, 50, 20, 10, 10],
    [1400, 70, 25, 15, 10],
    [4000, 100, 30, 20, 20],
];
const LOOKUP_COST_CONST: usize = 3;
const MAX_COMBINATION_SIZE: usize = 3;
const MAX_CONJUNCTION_SIZE: usize = 8;

pub struct IndexSelectionRule {}

impl Rule for IndexSelectionRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let logical_scan: &LogicalScan = plan.as_logical_scan()?;
        let indexes = logical_scan.indexes();
        if indexes.is_empty() {
            return None;
        }
        let primary_table_row_size = TableScanIoEstimator::estimate_row_size(logical_scan);
        let primary_cost = min(
            self.estimate_table_scan_cost(logical_scan, primary_table_row_size),
            self.estimate_full_table_scan_cost(logical_scan, primary_table_row_size),
        );

        // If it is a primary lookup plan, avoid checking other indexes.
        if primary_cost.primary_lookup {
            return None;
        }

        let mut final_plan: PlanRef = logical_scan.clone().into();
        let mut min_cost = primary_cost.clone();

        for index in indexes {
            if let Some(index_scan) = logical_scan.to_index_scan_if_index_covered(index) {
                let index_cost = self.estimate_table_scan_cost(
                    &index_scan,
                    TableScanIoEstimator::estimate_row_size(&index_scan),
                );

                if index_cost.le(&min_cost) {
                    min_cost = index_cost;
                    final_plan = index_scan.into();
                }
            } else {
                // non-covering index selection
                let (index_lookup, lookup_cost) = self.gen_index_lookup(logical_scan, index);
                if lookup_cost.le(&min_cost) {
                    min_cost = lookup_cost;
                    final_plan = index_lookup;
                }
            }
        }

        if let Some((merge_index, merge_index_cost)) = self.index_merge_selection(logical_scan)
            && merge_index_cost.le(&min_cost)
        {
            min_cost = merge_index_cost;
            final_plan = merge_index;
        }

        if min_cost == primary_cost {
            None
        } else {
            Some(final_plan)
        }
    }
}

struct IndexPredicateRewriter<'a> {
    p2s_mapping: &'a BTreeMap<usize, usize>,
    function_mapping: &'a HashMap<FunctionCall, usize>,
    offset: usize,
    covered_by_index: bool,
}

impl<'a> IndexPredicateRewriter<'a> {
    fn new(
        p2s_mapping: &'a BTreeMap<usize, usize>,
        function_mapping: &'a HashMap<FunctionCall, usize>,
        offset: usize,
    ) -> Self {
        Self {
            p2s_mapping,
            function_mapping,
            offset,
            covered_by_index: true,
        }
    }

    fn covered_by_index(&self) -> bool {
        self.covered_by_index
    }
}

impl ExprRewriter for IndexPredicateRewriter<'_> {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        // transform primary predicate to index predicate if it can
        if self.p2s_mapping.contains_key(&input_ref.index) {
            InputRef::new(
                *self.p2s_mapping.get(&input_ref.index()).unwrap(),
                input_ref.return_type(),
            )
            .into()
        } else {
            self.covered_by_index = false;
            InputRef::new(input_ref.index() + self.offset, input_ref.return_type()).into()
        }
    }

    fn rewrite_function_call(&mut self, func_call: FunctionCall) -> ExprImpl {
        if let Some(index) = self.function_mapping.get(&func_call) {
            return InputRef::new(*index, func_call.return_type()).into();
        }

        let (func_type, inputs, ret) = func_call.decompose();
        let inputs = inputs
            .into_iter()
            .map(|expr| self.rewrite_expr(expr))
            .collect();
        FunctionCall::new_unchecked(func_type, inputs, ret).into()
    }
}

impl IndexSelectionRule {
    fn gen_index_lookup(
        &self,
        logical_scan: &LogicalScan,
        index: &IndexCatalog,
    ) -> (PlanRef, IndexCost) {
        // 1. logical_scan ->  logical_join
        //                      /        \
        //                index_scan   primary_table_scan
        let index_scan = LogicalScan::create(
            index.index_table.name.clone(),
            index.index_table.clone(),
            vec![],
            logical_scan.ctx(),
            logical_scan.as_of().clone(),
            index.index_table.cardinality,
        );
        // We use `schema.len` instead of `index_item.len` here,
        // because schema contains system columns like `_rw_timestamp` column which is not represented in the index item.
        let offset = index_scan.table_catalog().columns().len();

        let primary_table_scan = LogicalScan::create(
            index.primary_table.name.clone(),
            index.primary_table.clone(),
            vec![],
            logical_scan.ctx(),
            logical_scan.as_of().clone(),
            index.primary_table.cardinality,
        );

        let predicate = logical_scan.predicate().clone();
        let mut rewriter = IndexPredicateRewriter::new(
            index.primary_to_secondary_mapping(),
            index.function_mapping(),
            offset,
        );
        let new_predicate = predicate.rewrite_expr(&mut rewriter);

        let conjunctions = index
            .primary_table_pk_ref_to_index_table()
            .iter()
            .zip_eq_fast(index.primary_table.pk.iter())
            .map(|(x, y)| {
                Self::create_null_safe_equal_expr(
                    x.column_index,
                    index.index_table.columns[x.column_index]
                        .data_type()
                        .clone(),
                    y.column_index + offset,
                    index.primary_table.columns[y.column_index]
                        .data_type()
                        .clone(),
                )
            })
            .chain(new_predicate)
            .collect_vec();
        let on = Condition { conjunctions };
        let join: PlanRef = LogicalJoin::new(
            index_scan.into(),
            primary_table_scan.into(),
            JoinType::Inner,
            on,
        )
        .into();

        // 2. push down predicate, so we can calculate the cost of index lookup
        let join_ref = join.predicate_pushdown(
            Condition::true_cond(),
            &mut PredicatePushdownContext::new(join.clone()),
        );

        let join_with_predicate_push_down =
            join_ref.as_logical_join().expect("must be a logical join");
        let new_join_left = join_with_predicate_push_down.left();
        let index_scan_with_predicate: &LogicalScan = new_join_left
            .as_logical_scan()
            .expect("must be a logical scan");

        // 3. calculate index cost, index lookup use primary table to estimate row size.
        let index_cost = self.estimate_table_scan_cost(
            index_scan_with_predicate,
            TableScanIoEstimator::estimate_row_size(logical_scan),
        );
        // lookup cost = index cost * LOOKUP_COST_CONST
        let lookup_cost = index_cost.mul(&IndexCost::new(LOOKUP_COST_CONST, false));

        // 4. keep the same schema with original logical_scan
        let scan_output_col_idx = logical_scan.output_col_idx();
        let lookup_join = join_ref.prune_col(
            &scan_output_col_idx
                .iter()
                .map(|&col_idx| col_idx + offset)
                .collect_vec(),
            &mut ColumnPruningContext::new(join_ref.clone()),
        );

        (lookup_join, lookup_cost)
    }

    /// Index Merge Selection
    /// Deal with predicate like a = 1 or b = 1
    /// Merge index scans from a table, currently merge is union semantic.
    fn index_merge_selection(&self, logical_scan: &LogicalScan) -> Option<(PlanRef, IndexCost)> {
        let predicate = logical_scan.predicate().clone();
        // Index merge is kind of index lookup join so use primary table row size to estimate index
        // cost.
        let primary_table_row_size = TableScanIoEstimator::estimate_row_size(logical_scan);
        // 1. choose lowest cost index merge path
        let paths = self.gen_paths(
            &predicate.conjunctions,
            logical_scan,
            primary_table_row_size,
        );
        let (index_access, index_access_cost) =
            self.choose_min_cost_path(&paths, primary_table_row_size)?;

        // 2. lookup primary table
        // the schema of index_access is the order key of primary table .
        let schema: &Schema = index_access.schema();
        let index_access_len = schema.len();

        let mut shift_input_ref_rewriter = ShiftInputRefRewriter {
            offset: index_access_len,
        };
        let new_predicate = predicate.rewrite_expr(&mut shift_input_ref_rewriter);

        let primary_table_desc = logical_scan.table_desc();

        let primary_table_scan = LogicalScan::create(
            logical_scan.table_name().to_owned(),
            logical_scan.table_catalog(),
            vec![],
            logical_scan.ctx(),
            logical_scan.as_of().clone(),
            logical_scan.table_cardinality(),
        );

        let conjunctions = primary_table_desc
            .pk
            .iter()
            .enumerate()
            .map(|(x, y)| {
                Self::create_null_safe_equal_expr(
                    x,
                    schema.fields[x].data_type.clone(),
                    y.column_index + index_access_len,
                    primary_table_desc.columns[y.column_index].data_type.clone(),
                )
            })
            .chain(new_predicate)
            .collect_vec();

        let on = Condition { conjunctions };
        let join: PlanRef =
            LogicalJoin::new(index_access, primary_table_scan.into(), JoinType::Inner, on).into();

        // 3 push down predicate
        let join_ref = join.predicate_pushdown(
            Condition::true_cond(),
            &mut PredicatePushdownContext::new(join.clone()),
        );

        // 4. keep the same schema with original logical_scan
        let scan_output_col_idx = logical_scan.output_col_idx();
        let lookup_join = join_ref.prune_col(
            &scan_output_col_idx
                .iter()
                .map(|&col_idx| col_idx + index_access_len)
                .collect_vec(),
            &mut ColumnPruningContext::new(join_ref.clone()),
        );

        Some((
            lookup_join,
            index_access_cost.mul(&IndexCost::new(LOOKUP_COST_CONST, false)),
        ))
    }

    /// Generate possible paths that can be used to access.
    /// The schema of output is the order key of primary table, so it can be used to lookup primary
    /// table later.
    /// Method `gen_paths` handles the complex condition recursively which may contains nested `AND`
    /// and `OR`. However, Method `gen_index_path` handles one arm of an OR clause which is a
    /// basic unit for index selection.
    fn gen_paths(
        &self,
        conjunctions: &[ExprImpl],
        logical_scan: &LogicalScan,
        primary_table_row_size: usize,
    ) -> Vec<PlanRef> {
        let mut result = vec![];
        for expr in conjunctions {
            // it's OR clause!
            if let ExprImpl::FunctionCall(function_call) = expr
                && function_call.func_type() == ExprType::Or
            {
                let mut index_to_be_merged = vec![];

                let disjunctions = to_disjunctions(expr.clone());
                let (map, others) = self.clustering_disjunction(disjunctions);
                let iter = map
                    .into_iter()
                    .map(|(column_index, expr)| (Some(column_index), expr))
                    .chain(others.into_iter().map(|expr| (None, expr)));
                for (column_index, expr) in iter {
                    let mut index_paths = vec![];
                    let conjunctions = to_conjunctions(expr);
                    index_paths.extend(
                        self.gen_index_path(column_index, &conjunctions, logical_scan)
                            .into_iter(),
                    );
                    // complex condition, recursively gen paths
                    if conjunctions.len() > 1 {
                        index_paths.extend(
                            self.gen_paths(&conjunctions, logical_scan, primary_table_row_size)
                                .into_iter(),
                        );
                    }

                    match self.choose_min_cost_path(&index_paths, primary_table_row_size) {
                        None => {
                            // One arm of OR clause can't use index, bail out
                            index_to_be_merged.clear();
                            break;
                        }
                        Some((path, _)) => index_to_be_merged.push(path),
                    }
                }

                if let Some(path) = self.merge(index_to_be_merged) {
                    result.push(path)
                }
            }
        }

        result
    }

    /// Clustering disjunction or expr by column index. If expr is complex, classify them as others.
    ///
    /// a = 1, b = 2, b = 3 -> map: [a, (a = 1)], [b, (b = 2 or b = 3)], others: []
    ///
    /// a = 1, (b = 2 and c = 3) -> map: [a, (a = 1)], others:
    ///
    /// (a > 1 and a < 8) or (c > 1 and c < 8)
    /// -> map: [], others: [(a > 1 and a < 8), (c > 1 and c < 8)]
    fn clustering_disjunction(
        &self,
        disjunctions: Vec<ExprImpl>,
    ) -> (HashMap<usize, ExprImpl>, Vec<ExprImpl>) {
        let mut map: HashMap<usize, ExprImpl> = HashMap::new();
        let mut others = vec![];
        for expr in disjunctions {
            let idx = {
                if let Some((input_ref, _const_expr)) = expr.as_eq_const() {
                    Some(input_ref.index)
                } else if let Some((input_ref, _in_const_list)) = expr.as_in_const_list() {
                    Some(input_ref.index)
                } else if let Some((input_ref, _op, _const_expr)) = expr.as_comparison_const() {
                    Some(input_ref.index)
                } else {
                    None
                }
            };

            if let Some(idx) = idx {
                match map.entry(idx) {
                    Occupied(mut entry) => {
                        let expr2: ExprImpl = entry.get().to_owned();
                        let or_expr = ExprImpl::FunctionCall(
                            FunctionCall::new_unchecked(
                                ExprType::Or,
                                vec![expr, expr2],
                                DataType::Boolean,
                            )
                            .into(),
                        );
                        entry.insert(or_expr);
                    }
                    Vacant(entry) => {
                        entry.insert(expr);
                    }
                };
            } else {
                others.push(expr);
                continue;
            }
        }

        (map, others)
    }

    /// Given a conjunctions from one arm of an OR clause (basic unit to index selection), generate
    /// all matching index path (including primary index) for the relation.
    /// `column_index` (refers to primary table) is a hint can be used to prune index.
    /// Steps:
    /// 1. Take the combination of `conjunctions` to extract the potential clauses.
    /// 2. For each potential clauses, generate index path if it can.
    fn gen_index_path(
        &self,
        column_index: Option<usize>,
        conjunctions: &[ExprImpl],
        logical_scan: &LogicalScan,
    ) -> Vec<PlanRef> {
        // Assumption: use at most `MAX_COMBINATION_SIZE` clauses, we can determine which is the
        // best index.
        let mut combinations = vec![];
        for i in 1..min(conjunctions.len(), MAX_COMBINATION_SIZE) + 1 {
            combinations.extend(
                conjunctions
                    .iter()
                    .take(min(conjunctions.len(), MAX_CONJUNCTION_SIZE))
                    .combinations(i),
            );
        }

        let mut result = vec![];

        for index in logical_scan.indexes() {
            if let Some(column_index) = column_index {
                assert_eq!(conjunctions.len(), 1);
                let p2s_mapping = index.primary_to_secondary_mapping();
                match p2s_mapping.get(&column_index) {
                    None => continue, // not found, prune this index
                    Some(&idx) => {
                        if index.index_table.pk()[0].column_index != idx {
                            // not match, prune this index
                            continue;
                        }
                    }
                }
            }

            // try secondary index
            for conj in &combinations {
                let condition = Condition {
                    conjunctions: conj.iter().map(|&x| x.to_owned()).collect(),
                };
                if let Some(index_access) = self.build_index_access(
                    index.clone(),
                    condition,
                    logical_scan.ctx().clone(),
                    logical_scan.as_of().clone(),
                ) {
                    result.push(index_access);
                }
            }
        }

        // try primary index
        let primary_table_desc = logical_scan.table_desc();
        if let Some(idx) = column_index {
            assert_eq!(conjunctions.len(), 1);
            if primary_table_desc.pk[0].column_index != idx {
                return result;
            }
        }

        let primary_access = generic::TableScan::new(
            logical_scan.table_name().to_owned(),
            primary_table_desc
                .pk
                .iter()
                .map(|x| x.column_index)
                .collect_vec(),
            logical_scan.table_catalog(),
            vec![],
            logical_scan.ctx(),
            Condition {
                conjunctions: conjunctions.to_vec(),
            },
            logical_scan.as_of().clone(),
            logical_scan.table_cardinality(),
        );

        result.push(primary_access.into());

        result
    }

    /// build index access if predicate (refers to primary table) is covered by index
    fn build_index_access(
        &self,
        index: Rc<IndexCatalog>,
        predicate: Condition,
        ctx: OptimizerContextRef,
        as_of: Option<AsOf>,
    ) -> Option<PlanRef> {
        let mut rewriter = IndexPredicateRewriter::new(
            index.primary_to_secondary_mapping(),
            index.function_mapping(),
            0,
        );
        let new_predicate = predicate.rewrite_expr(&mut rewriter);

        // check condition is covered by index.
        if !rewriter.covered_by_index() {
            return None;
        }

        Some(
            generic::TableScan::new(
                index.index_table.name.clone(),
                index
                    .primary_table_pk_ref_to_index_table()
                    .iter()
                    .map(|x| x.column_index)
                    .collect_vec(),
                index.index_table.clone(),
                vec![],
                ctx,
                new_predicate,
                as_of,
                index.index_table.cardinality,
            )
            .into(),
        )
    }

    fn merge(&self, paths: Vec<PlanRef>) -> Option<PlanRef> {
        if paths.is_empty() {
            return None;
        }

        let new_paths = paths
            .iter()
            .flat_map(|path| {
                if let Some(union) = path.as_logical_union() {
                    union.inputs().to_vec()
                } else if let Some(_scan) = path.as_logical_scan() {
                    vec![path.clone()]
                } else {
                    unreachable!();
                }
            })
            .sorted_by(|a, b| {
                // sort inputs to make plan deterministic
                a.as_logical_scan()
                    .expect("expect to be a logical scan")
                    .table_name()
                    .cmp(
                        b.as_logical_scan()
                            .expect("expect to be a logical scan")
                            .table_name(),
                    )
            })
            .collect_vec();

        Some(LogicalUnion::create(false, new_paths))
    }

    fn choose_min_cost_path(
        &self,
        paths: &[PlanRef],
        primary_table_row_size: usize,
    ) -> Option<(PlanRef, IndexCost)> {
        paths
            .iter()
            .map(|path| {
                if let Some(scan) = path.as_logical_scan() {
                    let cost = self.estimate_table_scan_cost(scan, primary_table_row_size);
                    (scan.clone().into(), cost)
                } else if let Some(union) = path.as_logical_union() {
                    let cost = union
                        .inputs()
                        .iter()
                        .map(|input| {
                            self.estimate_table_scan_cost(
                                input.as_logical_scan().expect("expect to be a scan"),
                                primary_table_row_size,
                            )
                        })
                        .reduce(|a, b| a.add(&b))
                        .unwrap();
                    (union.clone().into(), cost)
                } else {
                    unreachable!()
                }
            })
            .min_by(|(_, cost1), (_, cost2)| Ord::cmp(cost1, cost2))
    }

    fn estimate_table_scan_cost(&self, scan: &LogicalScan, row_size: usize) -> IndexCost {
        let mut table_scan_io_estimator = TableScanIoEstimator::new(scan, row_size);
        table_scan_io_estimator.estimate(scan.predicate())
    }

    fn estimate_full_table_scan_cost(&self, scan: &LogicalScan, row_size: usize) -> IndexCost {
        let mut table_scan_io_estimator = TableScanIoEstimator::new(scan, row_size);
        table_scan_io_estimator.estimate(&Condition::true_cond())
    }

    fn create_null_safe_equal_expr(
        left: usize,
        left_data_type: DataType,
        right: usize,
        right_data_type: DataType,
    ) -> ExprImpl {
        ExprImpl::FunctionCall(Box::new(FunctionCall::new_unchecked(
            ExprType::IsNotDistinctFrom,
            vec![
                ExprImpl::InputRef(Box::new(InputRef::new(left, left_data_type))),
                ExprImpl::InputRef(Box::new(InputRef::new(right, right_data_type))),
            ],
            DataType::Boolean,
        )))
    }
}

struct TableScanIoEstimator<'a> {
    table_scan: &'a LogicalScan,
    row_size: usize,
    cost: Option<IndexCost>,
}

impl<'a> TableScanIoEstimator<'a> {
    pub fn new(table_scan: &'a LogicalScan, row_size: usize) -> Self {
        Self {
            table_scan,
            row_size,
            cost: None,
        }
    }

    pub fn estimate_row_size(table_scan: &LogicalScan) -> usize {
        // 5 for table_id + 1 for vnode + 8 for epoch
        let row_meta_field_estimate_size = 14_usize;
        let table_desc = table_scan.table_desc();
        row_meta_field_estimate_size
            + table_desc
                .columns
                .iter()
                // add order key twice for its appearance both in key and value
                .chain(
                    table_desc
                        .pk
                        .iter()
                        .map(|x| &table_desc.columns[x.column_index]),
                )
                .map(|x| TableScanIoEstimator::estimate_data_type_size(&x.data_type))
                .sum::<usize>()
    }

    fn estimate_data_type_size(data_type: &DataType) -> usize {
        use std::mem::size_of;

        match data_type {
            DataType::Boolean => size_of::<bool>(),
            DataType::Int16 => size_of::<i16>(),
            DataType::Int32 => size_of::<i32>(),
            DataType::Int64 => size_of::<i64>(),
            DataType::Serial => size_of::<Serial>(),
            DataType::Float32 => size_of::<f32>(),
            DataType::Float64 => size_of::<f64>(),
            DataType::Decimal => size_of::<Decimal>(),
            DataType::Date => size_of::<Date>(),
            DataType::Time => size_of::<Time>(),
            DataType::Timestamp => size_of::<Timestamp>(),
            DataType::Timestamptz => size_of::<Timestamptz>(),
            DataType::Interval => size_of::<Interval>(),
            DataType::Int256 => Int256::size(),
            DataType::Varchar => 20,
            DataType::Bytea => 20,
            DataType::Jsonb => 20,
            DataType::Struct { .. } => 20,
            DataType::List { .. } => 20,
            DataType::Map(_) => 20,
            DataType::Vector(_) => todo!("VECTOR_PLACEHOLDER"),
        }
    }

    pub fn estimate(&mut self, predicate: &Condition) -> IndexCost {
        // try to deal with OR condition
        if predicate.conjunctions.len() == 1 {
            self.visit_expr(&predicate.conjunctions[0]);
            self.cost.take().unwrap_or_default()
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
                MatchItem::RangeOneSideBound | MatchItem::RangeTwoSideBound | MatchItem::All => {
                    true
                }
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
                MatchItem::RangeTwoSideBound => INDEX_COST_MATRIX[2][i],
                MatchItem::RangeOneSideBound => INDEX_COST_MATRIX[3][i],
                MatchItem::All => INDEX_COST_MATRIX[4][i],
            })
            .reduce(|x, y| x * y)
            .unwrap();

        // If `index_cost` equals 1, it is a primary lookup
        let primary_lookup = index_cost == 1;

        IndexCost::new(index_cost, primary_lookup)
            .mul(&IndexCost::new(self.row_size, primary_lookup))
    }

    fn match_index_column(
        &mut self,
        column_idx: usize,
        conjunctions: &mut Vec<ExprImpl>,
    ) -> MatchItem {
        // Equal
        for (i, expr) in conjunctions.iter().enumerate() {
            if let Some((input_ref, _const_expr)) = expr.as_eq_const()
                && input_ref.index == column_idx
            {
                conjunctions.remove(i);
                return MatchItem::Equal;
            }
        }

        // In
        for (i, expr) in conjunctions.iter().enumerate() {
            if let Some((input_ref, in_const_list)) = expr.as_in_const_list()
                && input_ref.index == column_idx
            {
                conjunctions.remove(i);
                return MatchItem::In(in_const_list.len());
            }
        }

        // Range
        let mut left_side_bound = false;
        let mut right_side_bound = false;
        let mut i = 0;
        while i < conjunctions.len() {
            let expr = &conjunctions[i];
            if let Some((input_ref, op, _const_expr)) = expr.as_comparison_const()
                && input_ref.index == column_idx
            {
                conjunctions.remove(i);
                match op {
                    ExprType::LessThan | ExprType::LessThanOrEqual => right_side_bound = true,
                    ExprType::GreaterThan | ExprType::GreaterThanOrEqual => left_side_bound = true,
                    _ => unreachable!(),
                };
            } else {
                i += 1;
            }
        }

        if left_side_bound && right_side_bound {
            MatchItem::RangeTwoSideBound
        } else if left_side_bound || right_side_bound {
            MatchItem::RangeOneSideBound
        } else {
            MatchItem::All
        }
    }
}

enum MatchItem {
    Equal,
    In(usize),
    RangeTwoSideBound,
    RangeOneSideBound,
    All,
}

#[derive(PartialEq, Eq, Hash, Clone, Debug, PartialOrd, Ord)]
struct IndexCost {
    cost: usize,
    primary_lookup: bool,
}

impl Default for IndexCost {
    fn default() -> Self {
        Self {
            cost: IndexCost::maximum(),
            primary_lookup: false,
        }
    }
}

impl IndexCost {
    fn new(cost: usize, primary_lookup: bool) -> IndexCost {
        Self {
            cost: min(cost, IndexCost::maximum()),
            primary_lookup,
        }
    }

    fn maximum() -> usize {
        10000000
    }

    fn add(&self, other: &IndexCost) -> IndexCost {
        IndexCost::new(
            self.cost
                .checked_add(other.cost)
                .unwrap_or_else(IndexCost::maximum),
            self.primary_lookup && other.primary_lookup,
        )
    }

    fn mul(&self, other: &IndexCost) -> IndexCost {
        IndexCost::new(
            self.cost
                .checked_mul(other.cost)
                .unwrap_or_else(IndexCost::maximum),
            self.primary_lookup && other.primary_lookup,
        )
    }

    fn le(&self, other: &IndexCost) -> bool {
        self.cost < other.cost
    }
}

impl ExprVisitor for TableScanIoEstimator<'_> {
    fn visit_function_call(&mut self, func_call: &FunctionCall) {
        let cost = match func_call.func_type() {
            ExprType::Or => func_call
                .inputs()
                .iter()
                .map(|x| {
                    let mut estimator = TableScanIoEstimator::new(self.table_scan, self.row_size);
                    estimator.visit_expr(x);
                    estimator.cost.take().unwrap_or_default()
                })
                .reduce(|x, y| x.add(&y))
                .unwrap(),
            ExprType::And => self.estimate_conjunctions(func_call.inputs()),
            _ => {
                let single = vec![ExprImpl::FunctionCall(func_call.clone().into())];
                self.estimate_conjunctions(&single)
            }
        };
        self.cost = Some(cost);
    }
}

struct ShiftInputRefRewriter {
    offset: usize,
}
impl ExprRewriter for ShiftInputRefRewriter {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        InputRef::new(input_ref.index() + self.offset, input_ref.return_type()).into()
    }
}

impl IndexSelectionRule {
    pub fn create() -> BoxedRule {
        Box::new(IndexSelectionRule {})
    }
}

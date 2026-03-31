// Copyright 2022 RisingWave Labs
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

//! Streaming index selection rule for backfill.
//!
//! Unlike the batch [`IndexSelectionRule`](IndexSelectionRule), this rule:
//! - Only considers **covering** indexes (no lookup join in streaming backfill)
//! - Handles IN predicates by expanding into a `LogicalUnion` of `LogicalScan`s,
//!   each with a branch-local equality predicate for correct post-backfill routing

use super::index_selection_rule::TableScanIoEstimator;
use super::prelude::PlanRef;
use crate::expr::{ExprImpl, ExprType, FunctionCall, InputRef, Literal};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{LogicalScan, LogicalUnion};
use crate::optimizer::rule::IndexSelectionRule;
use crate::utils::Condition;

pub struct StreamingIndexSelectionRule;

impl StreamingIndexSelectionRule {
    /// Rewrites a `LogicalScan` for backfill.
    ///
    /// Applies two optimizations in order:
    /// 1. **Covering index selection**: picks the lowest-cost covering index
    /// 2. **IN expansion**: splits IN predicates into a `LogicalUnion` of `LogicalScan`s,
    ///    each with a single-value equality predicate
    ///
    /// Returns `Some(PlanRef)` which is either a `LogicalScan` (index only) or a
    /// `LogicalUnion` of `LogicalScan`s (IN expansion, possibly on an index).
    pub fn rewrite(logical_scan: &LogicalScan) -> Option<PlanRef> {
        // Step 1: Try covering index selection.
        let best_scan =
            Self::select_covering_index(logical_scan).unwrap_or_else(|| logical_scan.clone());

        // Step 2: Try IN expansion on the (possibly index-backed) scan.
        if let Some(union) = Self::try_expand_in(&best_scan) {
            return Some(union);
        }

        // If we selected a better index (different from original), return it.
        if best_scan.table().id != logical_scan.table().id {
            return Some(best_scan.into());
        }

        None
    }

    /// Picks the lowest-cost covering index for a streaming scan.
    fn select_covering_index(logical_scan: &LogicalScan) -> Option<LogicalScan> {
        let indexes = logical_scan.table_indexes();
        if indexes.is_empty() {
            return None;
        }

        let rule = IndexSelectionRule {};
        let primary_table_row_size =
            TableScanIoEstimator::estimate_row_size(logical_scan);
        let primary_cost = std::cmp::min(
            rule.estimate_table_scan_cost(logical_scan, primary_table_row_size),
            rule.estimate_full_table_scan_cost(logical_scan, primary_table_row_size),
        );

        if primary_cost.primary_lookup {
            return None;
        }

        let mut best_scan: Option<LogicalScan> = None;
        let mut min_cost = primary_cost;

        for index in indexes {
            if let Some(index_scan) = logical_scan.to_index_scan_if_index_covered(index) {
                let index_cost = rule.estimate_table_scan_cost(
                    &index_scan,
                    TableScanIoEstimator::estimate_row_size(
                        &index_scan,
                    ),
                );
                if index_cost.le(&min_cost) {
                    min_cost = index_cost;
                    best_scan = Some(index_scan);
                }
            }
            // Skip non-covering indexes — no lookup join for backfill.
        }

        best_scan
    }

    /// Expands an IN predicate into a `LogicalUnion` of `LogicalScan`s.
    ///
    /// Each branch gets a single-value equality predicate (e.g., `a = 1`) plus residual
    /// conditions. Only applies when all scan ranges are non-trivial.
    fn try_expand_in(logical_scan: &LogicalScan) -> Option<PlanRef> {
        let (scan_ranges, residual) = logical_scan
            .predicate()
            .clone()
            .split_to_scan_ranges(
                logical_scan.table(),
                logical_scan
                    .base
                    .ctx()
                    .session_ctx()
                    .config()
                    .max_split_range_gap() as u64,
            )
            .ok()?;

        if scan_ranges.len() <= 1 || !scan_ranges.iter().all(|r| !r.is_full_table_scan()) {
            return None;
        }

        let pk = &logical_scan.table().pk;
        let table_cols = &logical_scan.table().columns;

        let branches: Vec<PlanRef> = scan_ranges
            .into_iter()
            .map(|range| {
                // Build a branch predicate from this scan range's eq_conds.
                // InputRef indices are in table-column space.
                let mut conjunctions: Vec<ExprImpl> = range
                    .eq_conds
                    .iter()
                    .enumerate()
                    .filter_map(|(i, datum)| {
                        let table_col_idx = pk[i].column_index;
                        let col_type = table_cols[table_col_idx].data_type().clone();
                        let input_ref = InputRef::new(table_col_idx, col_type.clone());
                        let literal = Literal::new(datum.clone(), col_type);
                        FunctionCall::new(
                            ExprType::Equal,
                            vec![input_ref.into(), literal.into()],
                        )
                        .ok()
                        .map(ExprImpl::from)
                    })
                    .collect();
                // Add residual conditions so each branch is self-contained.
                conjunctions.extend(residual.conjunctions.iter().cloned());
                let branch_predicate = Condition { conjunctions };
                logical_scan.clone_with_predicate(branch_predicate).into()
            })
            .collect();

        Some(LogicalUnion::create(true, branches))
    }
}

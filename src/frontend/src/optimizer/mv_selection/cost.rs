// Copyright 2026 RisingWave Labs
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

//! Chooses among the rewrites of a query block by how many rows they read.
//!
//! The rows a scan reads are estimated as the row count of the table times the fraction that
//! index selection expects the predicate of the scan to leave. A rewrite is worth it only if the
//! scan of the view reads fewer rows than the scans it replaces.

use risingwave_common::catalog::TableId;
use risingwave_pb::hummock::HummockVersionStats;

use crate::optimizer::plan_node::{LogicalPlanRef as PlanRef, LogicalScan};
use crate::optimizer::rule::estimate_scan_selectivity;

/// Estimates the rows a scan reads. Returns `None` if the table has no statistics yet.
pub fn scan_rows(scan: &LogicalScan, stats: &HummockVersionStats) -> Option<f64> {
    let table_stats = stats.table_stats.get(&scan.table().id.as_raw_id())?;
    Some(table_stats.total_key_count.max(0) as f64 * estimate_scan_selectivity(scan))
}

/// Estimates the rows read by all table scans of a plan. Returns `None` if any of the tables
/// has no statistics yet.
pub fn plan_rows(plan: &PlanRef, stats: &HummockVersionStats) -> Option<f64> {
    if let Some(scan) = plan.as_logical_scan() {
        return scan_rows(scan, stats);
    }
    plan.inputs()
        .iter()
        .try_fold(0.0, |sum, input| Some(sum + plan_rows(input, stats)?))
}

/// A rewrite of a query block.
pub struct Choice<T> {
    pub view_id: TableId,
    /// The rows read by the scans that the view replaces.
    pub original_rows: Option<f64>,
    /// The rows read by the scan of the view.
    pub view_rows: Option<f64>,
    pub rewrite: T,
}

impl<T> Choice<T> {
    /// `Some` if the rewrite may be used: by how many rows it reads less, or `None` if that is
    /// unknown.
    fn saving(&self) -> Option<Option<f64>> {
        match (self.original_rows, self.view_rows) {
            // Without statistics of the base tables there is nothing to compare with. This is
            // also the case of tables that were never written to.
            (None, _) => Some(None),
            // The view may be huge, or still waiting for its first statistics.
            (Some(_), None) => None,
            (Some(original), Some(view)) => (view < original).then_some(Some(original - view)),
        }
    }

    /// Why the rewrite is not used, for `EXPLAIN (TRACE)`.
    pub fn rejection(&self) -> Option<String> {
        match (self.saving(), self.original_rows, self.view_rows) {
            (Some(_), _, _) => None,
            (None, Some(original), Some(view)) => Some(format!(
                "it reads an estimated {view:.0} rows, the scans it replaces {original:.0}"
            )),
            (None, _, _) => Some("it has no statistics yet".to_owned()),
        }
    }
}

/// Picks the rewrite that saves the most rows. Rewrites whose saving is unknown are considered
/// only if no saving is known, and then the view with the smallest id wins, which keeps plans
/// deterministic when there are no statistics at all.
pub fn select<T>(choices: Vec<Choice<T>>) -> Option<Choice<T>> {
    let mut best: Option<(Option<f64>, Choice<T>)> = None;
    for choice in choices {
        let Some(saving) = choice.saving() else {
            continue;
        };
        let better = match &best {
            None => true,
            Some((best_saving, best_choice)) => match (saving, best_saving) {
                (Some(saving), Some(best_saving)) => {
                    saving > *best_saving
                        || (saving == *best_saving && choice.view_id < best_choice.view_id)
                }
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => choice.view_id < best_choice.view_id,
            },
        };
        if better {
            best = Some((saving, choice));
        }
    }
    best.map(|(_, choice)| choice)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn choice(view_id: u32, original_rows: Option<f64>, view_rows: Option<f64>) -> Choice<u32> {
        Choice {
            view_id: view_id.into(),
            original_rows,
            view_rows,
            rewrite: view_id,
        }
    }

    fn selected(choices: Vec<Choice<u32>>) -> Option<u32> {
        select(choices).map(|choice| choice.rewrite)
    }

    #[test]
    fn test_select() {
        // A view that does not read less is not used.
        assert_eq!(selected(vec![choice(1, Some(10.0), Some(10.0))]), None);
        // The largest saving wins, whatever the order and the ids.
        assert_eq!(
            selected(vec![
                choice(1, Some(1000.0), Some(900.0)),
                choice(2, Some(1000.0), Some(50.0)),
                choice(3, Some(1000.0), Some(2000.0)),
            ]),
            Some(2)
        );
        // A view without statistics is not used when the base tables have them.
        assert_eq!(selected(vec![choice(1, Some(1000.0), None)]), None);
        // Without any statistics every view is usable, and the smallest id wins.
        assert_eq!(
            selected(vec![choice(7, None, None), choice(3, None, Some(5.0))]),
            Some(3)
        );
        // A known saving is preferred over an unknown one.
        assert_eq!(
            selected(vec![
                choice(1, None, None),
                choice(2, Some(100.0), Some(1.0))
            ]),
            Some(2)
        );
    }
}

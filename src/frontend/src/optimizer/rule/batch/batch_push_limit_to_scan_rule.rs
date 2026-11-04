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

use itertools::Itertools;
use risingwave_pb::batch_plan::iceberg_scan_node::IcebergScanType;

use super::prelude::*;
use crate::optimizer::plan_node::generic::PhysicalPlanRef;
use crate::optimizer::plan_node::{BatchIcebergScan, BatchLimit, BatchSeqScan, PlanTreeNodeUnary};

pub struct BatchPushLimitToScanRule {}

// Pushing a limit into an Iceberg scan collapses file tasks into one split.
// Keep large scans parallel and let the upper BatchLimit enforce exact results.
const ICEBERG_LIMIT_PUSHDOWN_THRESHOLD: u64 = 1_000_000;

impl Rule<Batch> for BatchPushLimitToScanRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let limit: &BatchLimit = plan.as_batch_limit()?;
        let limit_input = limit.input();

        let pushed_limit = limit.limit().checked_add(limit.offset())?;
        if let Some(new_input) = push_limit_to_full_text_search(&limit_input, pushed_limit) {
            return Some(limit.clone_with_input(new_input).into());
        }
        if let Some(scan) = limit_input.as_batch_seq_scan() {
            if scan.limit().is_some() {
                return None;
            }
            let new_scan = BatchSeqScan::new_with_dist(
                scan.core().clone(),
                scan.base.distribution().clone(),
                scan.scan_ranges().iter().cloned().collect_vec(),
                Some(pushed_limit),
            );
            return Some(limit.clone_with_input(new_scan.into()).into());
        }

        let scan: &BatchIcebergScan = limit_input.as_batch_iceberg_scan()?;
        if scan.limit().is_some() {
            return None;
        }
        if scan.iceberg_scan_type() != IcebergScanType::DataScan {
            return None;
        }
        if pushed_limit > ICEBERG_LIMIT_PUSHDOWN_THRESHOLD {
            return None;
        }
        let new_scan = scan.clone_with_limit(Some(pushed_limit));
        Some(limit.clone_with_input(new_scan.into()).into())
    }
}

impl BatchPushLimitToScanRule {
    pub fn create() -> BoxedRule {
        Box::new(BatchPushLimitToScanRule {})
    }
}

/// Pushes `limit` down to a full-text search through operators that output exactly one row for
/// each input row. The search returns only keys of rows existing at the query epoch, so its
/// inner lookup join into the primary table, without other conditions, is one of them. This does
/// not hold for a primary table whose rows expire by retention or a TTL watermark: expired rows
/// are removed without deletes, so the index still returns their keys.
fn push_limit_to_full_text_search(plan: &PlanRef, limit: u64) -> Option<PlanRef> {
    if let Some(search) = plan.as_batch_full_text_search() {
        if search.limit().is_some_and(|existing| existing <= limit) {
            return None;
        }
        return Some(search.clone_with_limit(limit).into());
    }
    if let Some(project) = plan.as_batch_project() {
        let input = push_limit_to_full_text_search(&project.input(), limit)?;
        return Some(project.clone_with_input(input).into());
    }
    if let Some(exchange) = plan.as_batch_exchange() {
        let input = push_limit_to_full_text_search(&exchange.input(), limit)?;
        return Some(exchange.clone_with_input(input).into());
    }
    if let Some(join) = plan.as_batch_lookup_join()
        && join.is_inner_eq_join()
    {
        let join_input = join.input();
        let search_input = match join_input.as_batch_exchange() {
            Some(exchange) => exchange.input(),
            None => join_input.clone(),
        };
        let search = search_input.as_batch_full_text_search()?;
        let table = join.right_table();
        if search.primary_table_id() != table.id
            || table.retention_seconds.is_some()
            || table.clean_watermark_index_in_pk.is_some()
            || !table.clean_watermark_indices.is_empty()
        {
            return None;
        }
        let input = push_limit_to_full_text_search(&join_input, limit)?;
        return Some(join.clone_with_input(input).into());
    }
    None
}

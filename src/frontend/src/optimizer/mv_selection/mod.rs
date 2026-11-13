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

//! Answers batch queries from materialized views.
//!
//! A query is split into blocks: maximal select-project-join regions, each optionally topped
//! by one aggregation. A block and the definition of a materialized view are both brought into
//! a normal form that does not depend on the plan shape ([`summary`], [`extract`]). A view can
//! answer a block if the block is contained in the view, and what the block needs on top of
//! the view is the compensation ([`matcher`], [`compensate`]). Which views are usable at all
//! is decided once per catalog version ([`definition`], [`cache`]), and among several views
//! the one that saves the most rows is used ([`cost`]).
//!
//! A materialized view is aligned with its inputs on every barrier, so within a database it
//! is as fresh as the tables it is computed from.

mod cache;
mod compensate;
mod cost;
mod definition;
mod extract;
mod matcher;
mod summary;

use std::collections::HashSet;

pub use cache::CandidateCache;
use cache::{Candidate, Candidates};
use compensate::{RewrittenBlock, build_plan};
use cost::Choice;
use extract::{BlockExtractor, ExtractedBlock, map_lower_blocks};
use matcher::match_block;
use risingwave_common::catalog::TableId;

use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::plan_node::LogicalPlanRef as PlanRef;
use crate::optimizer::plan_node::generic::GenericPlanRef;

/// A block is rewritten again after every rewrite, to use several views or a view over a view.
/// Every rewrite replaces inputs by a view that depends on them, so this ends on its own; the
/// limit only guards against a bug.
const MAX_REWRITES_PER_BLOCK: usize = 16;

/// Rewrites the blocks of a batch query to read materialized views where that is cheaper.
/// Returns `None` if nothing is rewritten.
///
/// The plan must have gone through `LogicalOptimizer::normalize_for_match`, as the definitions
/// of the views do.
pub fn rewrite_with_materialized_views(plan: &PlanRef) -> Option<PlanRef> {
    let ctx = plan.ctx();
    let session = ctx.session_ctx().clone();
    // `UPDATE` and `DELETE` read the rows to change from their target table. `INSERT ... SELECT`
    // could be rewritten, but is left alone to keep the rule simple.
    if has_dml(plan) {
        return None;
    }
    // A snapshot fixed before planning may be older than a view, which then has no data, or
    // not all of it, at that snapshot.
    if session.config().query_epoch().0.is_some() || session.has_pinned_snapshot() {
        return None;
    }

    let mut pass = Pass {
        extractor: BlockExtractor::new(session.config().timezone()),
        ctx,
        traced: HashSet::new(),
    };
    let mut tables = HashSet::new();
    collect_tables(plan, &mut tables);
    // Most queries read no table that any view is built on, and stop here.
    if pass.candidates(&tables).usable.is_empty() {
        return None;
    }
    pass.rewrite_block(plan)
}

fn has_dml(plan: &PlanRef) -> bool {
    plan.as_logical_insert().is_some()
        || plan.as_logical_update().is_some()
        || plan.as_logical_delete().is_some()
        || plan.inputs().iter().any(has_dml)
}

fn collect_tables(plan: &PlanRef, tables: &mut HashSet<TableId>) {
    if let Some(scan) = plan.as_logical_scan() {
        tables.insert(scan.table().id);
    }
    for input in plan.inputs() {
        collect_tables(&input, tables);
    }
}

struct Pass {
    ctx: OptimizerContextRef,
    extractor: BlockExtractor,
    /// Unusable views that `EXPLAIN (TRACE)` has already been told about.
    traced: HashSet<String>,
}

impl Pass {
    fn trace(&self, message: impl FnOnce() -> String) {
        if self.ctx.is_explain_trace() {
            self.ctx
                .trace(format!("Materialized view selection: {}", message()));
        }
    }

    fn candidates(&mut self, tables: &HashSet<TableId>) -> Candidates {
        let candidates = self
            .ctx
            .session_ctx()
            .env()
            .mv_selection_cache()
            .candidates(self.ctx.session_ctx(), tables);
        for (view, reason) in &candidates.unusable {
            if self.traced.insert(view.clone()) {
                self.trace(|| format!("{view} is not usable: {reason}"));
            }
        }
        candidates
    }

    /// Rewrites the block rooted at `plan` and the blocks below it. Returns `None` if nothing
    /// is rewritten.
    fn rewrite_block(&mut self, plan: &PlanRef) -> Option<PlanRef> {
        // Bottom-up, so that this block is summarized over the rewritten blocks below it.
        let lower = map_lower_blocks(plan, &mut |block| self.rewrite_block(block));
        let mut rewritten = lower.is_some();
        let mut plan = lower.unwrap_or_else(|| plan.clone());
        for _ in 0..MAX_REWRITES_PER_BLOCK {
            let Some(next) = self.rewrite_once(&plan) else {
                break;
            };
            plan = next;
            rewritten = true;
        }
        rewritten.then_some(plan)
    }

    /// Replaces inputs of the block rooted at `plan` by the best materialized view, if any.
    fn rewrite_once(&mut self, plan: &PlanRef) -> Option<PlanRef> {
        let block = self.extractor.extract(plan)?;
        let mut tables = HashSet::new();
        block.summary.collect_tables(&mut tables);
        if tables.is_empty() {
            return None;
        }
        let candidates = self.candidates(&tables).usable;
        if candidates.is_empty() {
            return None;
        }

        let catalog = self.ctx.session_ctx().env().catalog_reader().read_guard();
        let stats = catalog.table_stats();
        let mut choices = vec![];
        for candidate in &candidates {
            let Some(rewrite) = self.rewrite_with(plan, &block, candidate) else {
                continue;
            };
            let covered = (0..block.original_inputs.len())
                .filter(|input| !rewrite.extra_inputs.contains(input));
            let original_rows = covered
                .map(|input| cost::plan_rows(&block.original_inputs[input], stats))
                .try_fold(0.0, |sum, rows| Some(sum + rows?));
            let choice = Choice {
                view_id: candidate.table.id,
                original_rows,
                view_rows: cost::scan_rows(&rewrite.block.view_scan, stats),
                rewrite: (candidate, rewrite.block.plan),
            };
            if let Some(reason) = choice.rejection() {
                self.trace(|| format!("{} is not used: {reason}", candidate.table.name));
            }
            choices.push(choice);
        }
        let (candidate, plan) = cost::select(choices)?.rewrite;
        self.trace(|| format!("{} is used", candidate.table.name));
        Some(plan)
    }

    fn rewrite_with(
        &self,
        plan: &PlanRef,
        block: &ExtractedBlock,
        candidate: &Candidate,
    ) -> Option<Rewrite> {
        let view = &candidate.table;
        let compensation = match match_block(&block.summary, &block.input_cols, &candidate.summary)
        {
            Ok(compensation) => compensation,
            Err(failure) => {
                self.trace(|| format!("{} does not match: {failure}", view.name));
                return None;
            }
        };
        let view_columns: Vec<usize> = view
            .columns()
            .iter()
            .enumerate()
            .filter(|(_, column)| !column.is_hidden())
            .map(|(idx, _)| idx)
            .collect();
        let rewritten = build_plan(
            &compensation,
            view,
            &candidate.indexes,
            &view_columns,
            block,
            plan,
        )?;
        Some(Rewrite {
            block: rewritten,
            extra_inputs: compensation.extra_inputs,
        })
    }
}

struct Rewrite {
    block: RewrittenBlock,
    /// The inputs of the block that the view does not replace.
    extra_inputs: Vec<usize>,
}

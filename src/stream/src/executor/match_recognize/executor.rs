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

//! Streaming `MATCH_RECOGNIZE` executor (v1, event-time).
//!
//! Scope: append-only input, `ONE ROW PER MATCH`,
//! `AFTER MATCH SKIP {PAST LAST ROW | TO NEXT ROW | TO {FIRST | LAST} <var>}`,
//! `MEASURES` with per-variable navigation (`FIRST`/`LAST`/bare `var.col`) and `CLASSIFIER()`.
//!
//! Event-time model: rows are buffered per partition (in any arrival order). Matching is driven by
//! the watermark on the leading `ORDER BY` column: when the watermark advances to `w`, every row
//! with `order_key <= w` is final (no earlier row can still arrive), so the buffer is sorted by
//! order key and the `<= w` prefix is matched. A match is emitted once it is followed by another
//! safe row (so the greedy match is known maximal); consumed rows are evicted. This handles
//! out-of-order arrival within the watermark's lateness and bounds state.
//!
//! Measures are evaluated at match time, not at arrival: a measure references specific matched rows
//! (e.g. `FIRST(a.ts)`, `LAST(b.v)`), which are only known once the match and its per-row pattern
//! variable labels are found. Each measure is an expression over a synthetic row whose columns are
//! produced by its [`MeasureSlot`]s from the matched rows.
//!
//! State: the buffered rows (the raw input row plus its satisfied pattern variables) are persisted
//! to a state table — written through on arrival, deleted on consumption — and restored on recovery.
//!
//! Matching is not incremental: each advancing watermark re-runs the matcher from the start of the
//! buffer rather than resuming partial NFA state. Eviction and empty-partition removal keep the
//! buffer bounded to the live (unfinalized) window, so the work per watermark is bounded by that
//! window rather than the partition's history; carrying incremental NFA state across watermarks is a
//! possible future optimization.

use std::collections::HashMap;
use std::ops::Bound;

use futures::{StreamExt, pin_mut};
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::row::{OwnedRow, Row, RowExt, once};
use risingwave_common::types::{DataType, Datum, DefaultOrd, ScalarImpl, ToOwnedDatum};
use risingwave_common::util::row_id::RowIdGenerator;
use risingwave_expr::aggregate::{AggCall, BoxedAggregateFunction, build_append_only};
use risingwave_expr::expr::{EvalErrorReport, NonStrictExpression, build_non_strict_from_prost};
use risingwave_pb::stream_plan::{
    MatchRecognizeDefine as PbMatchRecognizeDefine,
    MatchRecognizeMeasure as PbMatchRecognizeMeasure,
};
use risingwave_storage::StateStore;
use risingwave_storage::store::PrefetchOptions;

use super::nfa::{CandidateMatcher, Nfa, SkipMode};
use crate::common::table::state_table::StateTable;
use crate::executor::prelude::*;

/// How a [`MeasureSlot`] resolves against the rows of a match (mirrors the planner's slot kinds).
#[derive(Clone, Copy, PartialEq, Eq)]
enum MeasureSlotKind {
    /// Column value of the first row labeled `var` within the match.
    First,
    /// Column value of the last row labeled `var` (also a bare `var.col` under FINAL semantics).
    Last,
    /// The pattern variable bound to the match's last row.
    Classifier,
    /// Number of rows in the whole match (`COUNT(*)`).
    CountStar,
    /// Number of rows labeled `var` with a non-null `col` (`COUNT(var.col)`).
    Count,
    /// Minimum `col` over rows labeled `var` (`MIN(var.col)`).
    Min,
    /// Maximum `col` over rows labeled `var` (`MAX(var.col)`).
    Max,
    /// `SUM(var.col)`, evaluated by the slot's [`AggSlot`] aggregate kernel. `AVG` is lowered to a
    /// `Sum` slot plus a `Count` slot and a division expression, so it has no kind of its own.
    Sum,
}

/// A `SUM`/`AVG` aggregate kernel for a slot, plus the input column type used to feed it.
struct AggSlot {
    func: BoxedAggregateFunction,
    col_type: DataType,
}

/// One navigation input that a measure expression reads. The executor materializes one value per
/// slot from a match's rows and labels, forming the synthetic row the measure is evaluated over.
struct MeasureSlot {
    kind: MeasureSlotKind,
    /// Pattern variables this slot navigates over (several for a `SUBSET`). A row matches if its
    /// label is any of these. Empty for [`MeasureSlotKind::Classifier`].
    vars: Vec<String>,
    /// Input column index to read. Unused for [`MeasureSlotKind::Classifier`].
    col_idx: usize,
    /// The aggregate kernel for [`MeasureSlotKind::Sum`] (`AVG` is lowered to `Sum` plus `Count`).
    agg: Option<AggSlot>,
}

/// A `MEASURES` item compiled for execution.
pub struct CompiledMeasure {
    /// Expression over the synthetic per-match row: `InputRef(i)` reads `slots[i]`.
    expr: NonStrictExpression,
    slots: Vec<MeasureSlot>,
}

impl CompiledMeasure {
    /// Builds a compiled measure from its protobuf, building any aggregate kernels its slots need.
    pub fn from_protobuf(
        pb: &PbMatchRecognizeMeasure,
        error_report: impl EvalErrorReport + 'static,
    ) -> StreamExecutorResult<Self> {
        let expr = build_non_strict_from_prost(
            pb.expr
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("MATCH_RECOGNIZE measure missing expression"))?,
            error_report,
        )?;
        let slots = pb
            .slots
            .iter()
            .map(|s| {
                let kind = match s.kind {
                    0 => MeasureSlotKind::Last,
                    1 => MeasureSlotKind::First,
                    2 => MeasureSlotKind::Classifier,
                    3 => MeasureSlotKind::CountStar,
                    4 => MeasureSlotKind::Count,
                    5 => MeasureSlotKind::Min,
                    6 => MeasureSlotKind::Max,
                    7 => MeasureSlotKind::Sum,
                    // Fail fast on an unknown kind rather than silently treating it as LAST, which
                    // would change measure semantics under a corrupt plan or version skew.
                    other => {
                        return Err(anyhow::anyhow!(
                            "invalid MATCH_RECOGNIZE measure slot kind: {other}"
                        )
                        .into());
                    }
                };
                let agg = match kind {
                    MeasureSlotKind::Sum => {
                        let call =
                            AggCall::from_protobuf(s.agg_call.as_ref().ok_or_else(|| {
                                anyhow::anyhow!(
                                    "MATCH_RECOGNIZE SUM/AVG measure slot missing agg_call"
                                )
                            })?)?;
                        let col_type = call.args.arg_types()[0].clone();
                        let func = build_append_only(&call)?;
                        Some(AggSlot { func, col_type })
                    }
                    _ => None,
                };
                Ok(MeasureSlot {
                    kind,
                    vars: s.vars.clone(),
                    col_idx: s.col_idx as usize,
                    agg,
                })
            })
            .collect::<StreamExecutorResult<Vec<_>>>()?;
        Ok(CompiledMeasure { expr, slots })
    }
}

impl MeasureSlot {
    /// Resolves this slot against a match: `rows[start..]` are the matched rows and `labels[i]` is
    /// the pattern variable bound to `rows[start + i]`.
    async fn resolve(
        &self,
        rows: &[BufferedRow],
        start: usize,
        labels: &[String],
    ) -> StreamExecutorResult<Datum> {
        // The column value of the row at match-relative index `j`.
        let col_at = |j: usize| rows[start + j].row.datum_at(self.col_idx).to_owned_datum();
        // Whether a row's label is one this slot navigates over (a plain var, or any SUBSET member).
        let matches = |l: &String| self.vars.iter().any(|v| v == l);
        Ok(match self.kind {
            MeasureSlotKind::Classifier => {
                labels.last().map(|s| ScalarImpl::Utf8(s.as_str().into()))
            }
            MeasureSlotKind::First => labels.iter().position(&matches).and_then(col_at),
            MeasureSlotKind::Last => labels.iter().rposition(&matches).and_then(col_at),
            MeasureSlotKind::CountStar => Some(ScalarImpl::Int64(labels.len() as i64)),
            MeasureSlotKind::Count => {
                let n = labels
                    .iter()
                    .enumerate()
                    .filter(|(j, l)| matches(l) && col_at(*j).is_some())
                    .count();
                Some(ScalarImpl::Int64(n as i64))
            }
            MeasureSlotKind::Min => labels
                .iter()
                .enumerate()
                .filter(|(_, l)| matches(l))
                .filter_map(|(j, _)| col_at(j))
                .min_by(|a, b| a.default_cmp(b)),
            MeasureSlotKind::Max => labels
                .iter()
                .enumerate()
                .filter(|(_, l)| matches(l))
                .filter_map(|(j, _)| col_at(j))
                .max_by(|a, b| a.default_cmp(b)),
            MeasureSlotKind::Sum => {
                let agg = self.agg.as_ref().ok_or_else(|| {
                    anyhow::anyhow!("MATCH_RECOGNIZE SUM measure slot has no kernel")
                })?;
                // Feed the kernel a single-column chunk of the col values over the matching rows.
                let input: Vec<(Op, OwnedRow)> = labels
                    .iter()
                    .enumerate()
                    .filter(|(_, l)| matches(l))
                    .map(|(j, _)| (Op::Insert, OwnedRow::new(vec![col_at(j)])))
                    .collect();
                if input.is_empty() {
                    None
                } else {
                    let chunk = StreamChunk::from_rows(&input, std::slice::from_ref(&agg.col_type));
                    let mut state = agg.func.create_state()?;
                    agg.func.update(&mut state, &chunk).await?;
                    agg.func.get_result(&state).await?
                }
            }
        })
    }
}

/// How a [`DefineSlot`] resolves against the candidate row (mirrors the planner's slot kinds).
#[derive(Clone, Copy, PartialEq, Eq)]
enum DefineSlotKind {
    /// The candidate row's own column.
    SelfCol,
    /// `PREV(col, offset)`: a row `offset` positions earlier in the ordered partition.
    Prev,
    /// `NEXT(col, offset)`: `offset` positions later.
    Next,
    /// `FIRST(var.col)`: the first row labeled `vars` in the in-progress match.
    RunningFirst,
    /// `LAST(var.col)` / bare other-variable reference: the last such row (running).
    RunningLast,
}

/// One input a `DEFINE` predicate reads (mirrors the planner's [`DefineSlot`]).
struct DefineSlot {
    kind: DefineSlotKind,
    vars: Vec<String>,
    col_idx: usize,
    offset: usize,
}

/// A `DEFINE` predicate compiled for execution: a boolean condition over a synthetic slot row.
pub struct CompiledDefine {
    symbol: String,
    condition: NonStrictExpression,
    slots: Vec<DefineSlot>,
}

impl CompiledDefine {
    pub fn from_protobuf(
        pb: &PbMatchRecognizeDefine,
        error_report: impl EvalErrorReport + 'static,
    ) -> StreamExecutorResult<Self> {
        let condition = build_non_strict_from_prost(
            pb.condition
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("MATCH_RECOGNIZE define missing condition"))?,
            error_report,
        )?;
        let slots = pb
            .slots
            .iter()
            .map(|s| {
                let kind = match s.kind {
                    0 => DefineSlotKind::SelfCol,
                    1 => DefineSlotKind::Prev,
                    2 => DefineSlotKind::Next,
                    3 => DefineSlotKind::RunningFirst,
                    4 => DefineSlotKind::RunningLast,
                    // Fail fast on an unknown kind rather than silently treating it as a self-column
                    // reference, which would change the DEFINE predicate's meaning under a corrupt
                    // plan or version skew.
                    other => {
                        return Err(StreamExecutorError::from(anyhow::anyhow!(
                            "invalid MATCH_RECOGNIZE define slot kind: {other}"
                        )));
                    }
                };
                Ok(DefineSlot {
                    kind,
                    vars: s.vars.clone(),
                    col_idx: s.col_idx as usize,
                    offset: s.offset as usize,
                })
            })
            .collect::<StreamExecutorResult<Vec<_>>>()?;
        Ok(CompiledDefine {
            symbol: pb.symbol.clone(),
            condition,
            slots,
        })
    }
}

/// Evaluates `DEFINE` predicates against the in-progress match, driving the NFA. Holds the sorted
/// safe-prefix rows of one partition and the compiled `DEFINE`s; a variable with no `DEFINE` is
/// universally true.
struct DefineMatcher<'a> {
    rows: &'a [BufferedRow],
    safe_len: usize,
    defines: &'a HashMap<String, CompiledDefine>,
    /// `WITHIN` span predicate over `[last_order_key, first_order_key]`. Applied as a candidate is
    /// bound so the NFA prunes any extension that would push the match's span past the bound,
    /// yielding the longest match that fits the window rather than rejecting an overshooting greedy
    /// match after the fact.
    within: Option<&'a NonStrictExpression>,
}

impl DefineMatcher<'_> {
    /// The value a slot reads for a candidate at `pos`, where `match_start` is the match's first row
    /// and `labels[k]` is the variable bound to `rows[match_start + k]`.
    fn slot_value(
        &self,
        slot: &DefineSlot,
        pos: usize,
        match_start: usize,
        labels: &[String],
    ) -> Datum {
        let col_at = |i: usize| self.rows[i].row.datum_at(slot.col_idx).to_owned_datum();
        let in_var = |l: &String| slot.vars.iter().any(|v| v == l);
        match slot.kind {
            DefineSlotKind::SelfCol => col_at(pos),
            DefineSlotKind::Prev => pos.checked_sub(slot.offset).and_then(col_at),
            DefineSlotKind::Next => {
                let i = pos + slot.offset;
                if i < self.safe_len { col_at(i) } else { None }
            }
            DefineSlotKind::RunningFirst => labels
                .iter()
                .position(in_var)
                .and_then(|k| col_at(match_start + k)),
            DefineSlotKind::RunningLast => labels
                .iter()
                .rposition(in_var)
                .and_then(|k| col_at(match_start + k)),
        }
    }
}

impl CandidateMatcher for DefineMatcher<'_> {
    async fn matches(
        &self,
        var: &str,
        pos: usize,
        labels: &[String],
    ) -> StreamExecutorResult<bool> {
        let match_start = pos - labels.len();
        // A pattern variable with no DEFINE matches every row; one with a DEFINE must satisfy it.
        if let Some(def) = self.defines.get(var) {
            let synthetic: Vec<Datum> = def
                .slots
                .iter()
                .map(|slot| self.slot_value(slot, pos, match_start, labels))
                .collect();
            let value = def
                .condition
                .eval_row_infallible(&OwnedRow::new(synthetic))
                .await;
            if !value.is_some_and(|s| s.into_bool()) {
                return Ok(false);
            }
        }
        // WITHIN: binding `pos` extends the match to span `[match_start, pos]`. Reject the candidate
        // if that span exceeds the bound, so the NFA backtracks to a shorter match that fits.
        if let Some(within) = self.within {
            let first_key = self.rows[match_start].order_key.clone();
            let last_key = self.rows[pos].order_key.clone();
            let span_row = OwnedRow::new(vec![last_key, first_key]);
            let value = within.eval_row_infallible(&span_row).await;
            if !value.is_some_and(|s| s.into_bool()) {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

pub struct MatchRecognizeExecutorArgs<S: StateStore> {
    pub ctx: ActorContextRef,
    pub input: Executor,
    /// Output schema: the `PARTITION BY` columns followed by the `MEASURES` columns.
    pub schema: Schema,
    pub chunk_size: usize,
    pub partition_key_indices: Vec<usize>,
    pub order_key_indices: Vec<usize>,
    pub measures: Vec<CompiledMeasure>,
    pub defines: Vec<CompiledDefine>,
    /// `WITHIN` span check over `[last_order_key, first_order_key]`; rejects matches that exceed it.
    pub within: Option<NonStrictExpression>,
    /// `WITHIN` deadline `first_order_key + interval` over a synthetic `[first_order_key]` row; the
    /// watermark at which a partial starting at that row expires. Used to wake idle partitions to
    /// evict timed-out partials. `None` when there is no `WITHIN`.
    pub within_deadline: Option<NonStrictExpression>,
    pub nfa: Nfa,
    pub skip: SkipMode,
    /// Number of input columns; the buffered raw input row stored per row in the state table.
    pub input_arity: usize,
    pub state_table: StateTable<S>,
    /// Wakeup frontier: `pk (partition...) -> next_wakeup_order_key`. Point-looked-up by partition.
    pub frontier_meta_table: StateTable<S>,
    /// Wakeup frontier: `pk (next_wakeup_order_key, partition...)`, distributed by partition.
    pub frontier_index_table: StateTable<S>,
}

pub struct MatchRecognizeExecutor<S: StateStore> {
    ctx: ActorContextRef,
    input: Executor,
    schema: Schema,
    chunk_size: usize,
    partition_key_indices: Vec<usize>,
    /// Input column index of the leading ORDER BY column (the watermark column). The full ORDER BY
    /// is encoded in the state-table key, so the buffer scans back already ordered; the executor
    /// only needs the leading column here, to find the safe prefix against the watermark.
    time_col: usize,
    measures: Vec<CompiledMeasure>,
    /// Compiled `DEFINE` predicates keyed by their pattern variable.
    defines: HashMap<String, CompiledDefine>,
    within: Option<NonStrictExpression>,
    /// `WITHIN` deadline expr (see [`MatchRecognizeExecutorArgs`]); folded into `next_wakeup` so an
    /// idle partition is woken to evict a partial that has timed out.
    within_deadline: Option<NonStrictExpression>,
    nfa: Nfa,
    skip: SkipMode,
    input_arity: usize,
    state_table: StateTable<S>,
    /// Wakeup frontier (see [`MatchRecognizeExecutorArgs`]). Maintained on insert so a watermark can
    /// visit only the partitions that need attention.
    frontier_meta_table: StateTable<S>,
    frontier_index_table: StateTable<S>,
}

/// A buffered input row, materialized from the state table while processing one partition.
struct BufferedRow {
    /// Per-actor monotonic id; the state-table key tiebreaker (keeps rows with equal ORDER BY keys
    /// distinct and stably ordered).
    seq: i64,
    /// Leading ORDER BY value (a copy of `row[time_col]`), compared against the watermark to find
    /// the safe prefix. The buffer arrives pre-sorted by the full ORDER BY key (state-table PK).
    order_key: Datum,
    /// The raw input row, read by DEFINE and MEASURES navigation slots at match time.
    row: OwnedRow,
}

impl<S: StateStore> MatchRecognizeExecutor<S> {
    pub fn new(args: MatchRecognizeExecutorArgs<S>) -> Self {
        let time_col = args.order_key_indices[0];
        let defines = args
            .defines
            .into_iter()
            .map(|d| (d.symbol.clone(), d))
            .collect();
        Self {
            ctx: args.ctx,
            input: args.input,
            schema: args.schema,
            chunk_size: args.chunk_size,
            partition_key_indices: args.partition_key_indices,
            time_col,
            measures: args.measures,
            defines,
            within: args.within,
            within_deadline: args.within_deadline,
            nfa: args.nfa,
            skip: args.skip,
            input_arity: args.input_arity,
            state_table: args.state_table,
            frontier_meta_table: args.frontier_meta_table,
            frontier_index_table: args.frontier_index_table,
        }
    }

    /// Memoized WITHIN-deadline lookup for one partition visit. `deadline(rows[i]) = order_key +
    /// interval` is consulted by the boundary-emit guard, the eviction scan, and the wakeup
    /// recompute; the expression is evaluated at most once per row (`memo[i]`) instead of once per
    /// consulting site.
    async fn deadline_at(
        deadline_expr: &NonStrictExpression,
        rows: &[BufferedRow],
        memo: &mut [Option<Datum>],
        i: usize,
    ) -> Datum {
        if memo[i].is_none() {
            let synthetic = OwnedRow::new(vec![rows[i].order_key.clone()]);
            memo[i] = Some(deadline_expr.eval_row_infallible(&synthetic).await);
        }
        memo[i].as_ref().unwrap().clone()
    }

    /// Fold a wakeup key into a running minimum (`None` = nothing folded yet).
    fn fold_min(acc: &mut Datum, k: &ScalarImpl) {
        let lower = match acc {
            Some(m) => k.default_cmp(m).is_lt(),
            None => true,
        };
        if lower {
            *acc = Some(k.clone());
        }
    }

    /// Maintain the wakeup frontier for one partition on insert. If the partition has no frontier
    /// entry, or the chunk's earliest new order key precedes its recorded wakeup, (re)point the
    /// frontier at `new_min`. On insert `next_wakeup` only ever moves *earlier*; a watermark pass
    /// recomputes it precisely (including any WITHIN-driven expiry) after processing the partition.
    ///
    /// `frontier_meta`: `pk (partition...) -> next_wakeup` — updated in place (pk unchanged).
    /// `frontier_index`: `pk (next_wakeup, partition...)` — `next_wakeup` is part of the key, so a
    /// change is a delete of the old entry plus an insert of the new one.
    async fn update_frontier_on_insert(
        meta: &mut StateTable<S>,
        index: &mut StateTable<S>,
        partition_key: &OwnedRow,
        new_min: Datum,
    ) -> StreamExecutorResult<()> {
        // This point-reads the meta table per touched partition. With `forbid_preload_all_rows` a
        // cold partition round-trips to the state store, so it adds a read to the ingest path. For
        // high-cardinality workloads a bounded in-memory frontier cache (mirroring
        // `append_only_dedup`'s `ManagedLruCache`) would absorb hot partitions; deferred until the
        // watermark path consumes the frontier and profiling justifies the added complexity.
        let p = partition_key.len();
        let old = meta.get_row(partition_key).await?;
        let old_wakeup: Option<Datum> = old.as_ref().map(|r| r.datum_at(p).to_owned_datum());
        let should_update = match &old_wakeup {
            None => true,
            Some(ow) => new_min
                .as_ref()
                .unwrap()
                .default_cmp(ow.as_ref().unwrap())
                .is_lt(),
        };
        if !should_update {
            return Ok(());
        }
        // Build the written rows by chaining *references* (the row params are `impl Row`), so neither
        // `partition_key` nor the old meta row is cloned for the meta/index writes — only the small
        // single-datum wakeup rows are allocated.
        let wakeup_row = OwnedRow::new(vec![new_min.clone()]);
        // meta: pk (partition...) -> next_wakeup. pk is unchanged, so update in place.
        match &old {
            Some(old_row) => meta.update(old_row, partition_key.chain(&wakeup_row)),
            None => meta.insert(partition_key.chain(&wakeup_row)),
        }
        // index: pk (next_wakeup, partition...). next_wakeup is part of the key, so a change is a
        // delete of the old entry plus an insert of the new one.
        if let Some(ow) = &old_wakeup {
            let old_wakeup_row = OwnedRow::new(vec![ow.clone()]);
            index.delete(old_wakeup_row.chain(partition_key));
        }
        let new_index_key = OwnedRow::new(vec![new_min]);
        index.insert(new_index_key.chain(partition_key));
        Ok(())
    }

    /// Remove a partition's wakeup-frontier entry from both tables. Used when a processed partition
    /// is empty or has no future row to wake for. `old_wakeup` is the partition's current frontier
    /// value (already in hand from the index scan), so neither table needs a point read — it doubles
    /// as the meta old-value, valid because meta and index are kept in lockstep (see the
    /// candidate-processing comment in the watermark arm).
    fn remove_frontier(
        meta: &mut StateTable<S>,
        index: &mut StateTable<S>,
        partition_key: &OwnedRow,
        old_wakeup: &Datum,
    ) {
        let ow = OwnedRow::new(vec![old_wakeup.clone()]);
        index.delete((&ow).chain(partition_key));
        meta.delete(partition_key.chain(&ow));
    }

    /// Re-point a partition's wakeup frontier from `old_wakeup` to `new_wakeup`. The index PK leads
    /// with the wakeup, so its entry is delete+insert; the meta PK is the partition, so it updates
    /// in place. Rows are chained by reference to avoid cloning the partition key. `old_wakeup` (from
    /// the index scan) doubles as the meta old-value; this is valid only while meta and index stay in
    /// lockstep (see the candidate-processing comment in the watermark arm).
    fn move_frontier(
        meta: &mut StateTable<S>,
        index: &mut StateTable<S>,
        partition_key: &OwnedRow,
        old_wakeup: &Datum,
        new_wakeup: Datum,
    ) {
        let ow = OwnedRow::new(vec![old_wakeup.clone()]);
        let nw = OwnedRow::new(vec![new_wakeup]);
        index.delete((&ow).chain(partition_key));
        index.insert((&nw).chain(partition_key));
        meta.update(partition_key.chain(&ow), partition_key.chain(&nw));
    }
}

impl<S: StateStore> Execute for MatchRecognizeExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

impl<S: StateStore> MatchRecognizeExecutor<S> {
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self: Box<Self>) {
        let Self {
            ctx,
            input,
            schema,
            chunk_size,
            partition_key_indices,
            time_col,
            measures,
            defines,
            within,
            within_deadline,
            nfa,
            skip,
            input_arity,
            mut state_table,
            mut frontier_meta_table,
            mut frontier_index_table,
        } = *self;

        let mut input = input.execute();
        let barrier = expect_first_barrier(&mut input).await?;
        let first_epoch = barrier.epoch;
        yield Message::Barrier(barrier);
        state_table.init_epoch(first_epoch).await?;
        frontier_meta_table.init_epoch(first_epoch).await?;
        frontier_index_table.init_epoch(first_epoch).await?;

        // Generator for the buffer-table PK tiebreaker `seq`, assigned to every input row. A
        // snowflake-style id (timestamp + owned vnode + sequence) is unique across this actor's
        // lifetime and across rescaling, so distinct rows never collide on `(partition, seq)`.
        // The output `_match_id` is NOT minted here — it is the match's start-row `seq` (see the
        // emit path), making emitted output deterministic across recovery replay. Rebuilt on a
        // vnode-bitmap change below.
        let mut row_id_gen = RowIdGenerator::new(
            state_table.vnodes().iter_vnodes(),
            state_table.vnodes().len(),
        );

        // In-memory lower bound on the minimum `next_wakeup` across this actor's frontier entries.
        // `None` = unknown (startup, or after a vnode-bitmap change) — the next watermark must scan
        // and re-establish it; `Some(None)` = the frontier is known empty; `Some(Some(k))` = no
        // entry is earlier than `k`. Purely an optimization, never persisted: it lets a watermark
        // with nothing due skip the per-vnode index scans with a single comparison.
        let mut min_wakeup: Option<Datum> = None;

        // No in-memory partition buffer: the state table is the single source of truth. Inserts are
        // written through immediately; each watermark scans the buffered rows back in PK order
        // (partition, order key, seq), processing one partition at a time and holding only that
        // partition's rows resident — so memory is bounded by the largest single partition's live
        // buffer, not by a whole vnode or the number of distinct keys. Recovery and rescale need no
        // in-memory rebuild.

        #[for_await]
        for msg in input {
            match msg? {
                Message::Chunk(chunk) => {
                    // Append-only input: write each row through to the state table. DEFINE and
                    // MEASURES are evaluated later, at watermark time, against the buffered rows.
                    let chunk = chunk.compact_vis();
                    // Earliest new order key per partition touched by this chunk, so the wakeup
                    // frontier is updated once per distinct partition (O(#partitions in chunk)) rather
                    // than once per row.
                    let mut chunk_min: HashMap<OwnedRow, Datum> = HashMap::new();
                    for (op, row_ref) in chunk.rows() {
                        // The input is required to be append-only (enforced at planning time), so only
                        // Insert is expected. Fail loud on anything else rather than silently
                        // producing wrong matches if that contract is ever violated upstream.
                        if !matches!(op, Op::Insert) {
                            return Err(anyhow::anyhow!(
                                "MATCH_RECOGNIZE requires append-only input but received a {:?} record",
                                op
                            )
                            .into());
                        }
                        let order_key = row_ref.datum_at(time_col).to_owned_datum();
                        // A row with a NULL order key has no event time: it can never fall under the
                        // watermark, so it would never be finalized or evicted (it would linger
                        // forever) and cannot be meaningfully ordered against other rows. Drop it, as
                        // event-time processing does with NULL-rowtime rows.
                        if order_key.is_none() {
                            continue;
                        }
                        let partition_key = row_ref.project(&partition_key_indices).to_owned_row();
                        chunk_min
                            .entry(partition_key)
                            .and_modify(|m| {
                                if order_key
                                    .as_ref()
                                    .unwrap()
                                    .default_cmp(m.as_ref().unwrap())
                                    .is_lt()
                                {
                                    *m = order_key.clone();
                                }
                            })
                            .or_insert_with(|| order_key.clone());
                        // State-table row layout: `[ seq, <input cols..> ]` — the partition columns
                        // and order key are columns of the stored input row. Written through by
                        // reference: `insert` takes `impl Row`, so the seq datum is chained onto the
                        // borrowed chunk row with no intermediate owned materialization.
                        state_table.insert(
                            once(Some(ScalarImpl::Int64(row_id_gen.next()))).chain(row_ref),
                        );
                    }
                    // One frontier update per distinct partition. On insert `next_wakeup` only ever
                    // moves earlier (a new row can only make a partition need attention sooner); the
                    // watermark path (the `Message::Watermark` arm below) reads the frontier to decide
                    // which partitions to visit, then recomputes each visited partition's entry
                    // precisely. Insert and watermark both mutate `frontier_meta` and `frontier_index`
                    // in lockstep — the invariant the watermark path relies on when it reuses an index
                    // datum as the meta old-value.
                    for (partition_key, new_min) in chunk_min {
                        // Keep the in-memory lower bound a lower bound: an insert only ever moves a
                        // partition's wakeup earlier. (Unknown stays unknown until a scan.)
                        if let (Some(acc), Some(k)) = (&mut min_wakeup, &new_min) {
                            Self::fold_min(acc, k);
                        }
                        Self::update_frontier_on_insert(
                            &mut frontier_meta_table,
                            &mut frontier_index_table,
                            &partition_key,
                            new_min,
                        )
                        .await?;
                    }
                }
                Message::Watermark(watermark) => {
                    // Only the leading ORDER BY column drives matching.
                    if watermark.col_idx != time_col {
                        continue;
                    }
                    let w = watermark.val;

                    // Idle fast path: if the in-memory lower bound on the frontier's minimum
                    // `next_wakeup` is known and past `w`, no partition can be due — skip the
                    // per-vnode index scans entirely. Unknown (`None`) falls through to a scan,
                    // which re-establishes the exact value below.
                    match &min_wakeup {
                        Some(None) => continue,
                        Some(Some(k)) if k.default_cmp(&w).is_gt() => continue,
                        _ => {}
                    }

                    // Frontier-driven: visit only the partitions whose `next_wakeup <= w`, instead
                    // of sweeping every live partition. Per owned vnode, scan the index (ordered by
                    // next_wakeup) and stop at the first entry past the watermark; then, for each
                    // candidate partition, read just that partition from the buffer table, match /
                    // emit / evict, and recompute its frontier entry. Work is therefore proportional
                    // to the partitions that actually need attention, not to the number of live
                    // partitions.
                    let mut builder = StreamChunkBuilder::new(chunk_size, schema.data_types());
                    // Recomputed exactly during the scan: the first past-`w` index entry per vnode
                    // plus every re-pointed candidate wakeup — every remaining frontier entry is one
                    // or the other.
                    let mut next_min: Datum = None;
                    let vnodes: Vec<_> = state_table.vnodes().iter_vnodes().collect();
                    for vnode in vnodes {
                        // 1. Collect candidate `(old_wakeup, partition)` from the index, then drop the
                        //    iterator — a state-table delete cannot interleave with an open iterator,
                        //    and we mutate the index below. The index is PK-ordered by
                        //    `(next_wakeup, partition)`, so candidates (`next_wakeup <= w`) sort first
                        //    and we stop at the first row past `w`.
                        let mut candidates: Vec<(Datum, OwnedRow)> = Vec::new();
                        {
                            let sub_range: (Bound<OwnedRow>, Bound<OwnedRow>) =
                                (Bound::Unbounded, Bound::Unbounded);
                            let iter = frontier_index_table
                                .iter_with_vnode(vnode, &sub_range, PrefetchOptions::default())
                                .await?;
                            pin_mut!(iter);
                            while let Some(item) = iter.next().await {
                                // index row = [next_wakeup, partition...]
                                let row = item?.into_owned_row();
                                let next_wakeup = row.datum_at(0).to_owned_datum();
                                // Stop at the first entry past the watermark. Sound because the index
                                // PK leads with `next_wakeup` ascending and `iter_with_vnode` yields
                                // rows in memcomparable PK order, which agrees with `default_cmp` on
                                // the order-key type (negatives sort before positives; a tie on
                                // `next_wakeup` falls through to the partition suffix, so every
                                // equal-wakeup candidate is seen before any greater one). `next_wakeup`
                                // is never NULL: NULL order keys are dropped at ingest, and a WITHIN
                                // deadline is only scheduled when it evaluates non-null. The first
                                // past-`w` entry also seeds the recomputed in-memory lower bound
                                // (every entry before it is a candidate being reprocessed below).
                                match &next_wakeup {
                                    Some(k) if k.default_cmp(&w).is_le() => {}
                                    Some(k) => {
                                        Self::fold_min(&mut next_min, k);
                                        break;
                                    }
                                    None => break,
                                }
                                let partition_key = OwnedRow::new(
                                    (1..row.len())
                                        .map(|i| row.datum_at(i).to_owned_datum())
                                        .collect(),
                                );
                                candidates.push((next_wakeup, partition_key));
                            }
                        }

                        // 2. Process each candidate partition. `old_wakeup` is this partition's
                        //    frontier value as stored in the *index*; `move_frontier`/`remove_frontier`
                        //    below reuse it as the *meta* old-value instead of point-reading meta. That
                        //    is sound only because meta and index are always mutated together (by
                        //    `update_frontier_on_insert`, `move_frontier`, `remove_frontier`), each
                        //    writing the same wakeup to both — so they never disagree on a partition's
                        //    `next_wakeup`. The consistent-old-value check does not backstop this: a
                        //    stale old-value on an untouched, already-committed row is accepted, not
                        //    rejected, so the lockstep must hold by construction.
                        for (old_wakeup, partition_key) in candidates {
                            // Read this partition's rows from the buffer table in PK order
                            // (partition, order key, seq) — already ORDER BY ordered, no sort — then
                            // drop the iterator so the evicting deletes below can run in place.
                            let mut rows: Vec<BufferedRow> = Vec::new();
                            {
                                let sub_range: (Bound<OwnedRow>, Bound<OwnedRow>) =
                                    (Bound::Unbounded, Bound::Unbounded);
                                let iter = state_table
                                    .iter_with_prefix(
                                        &partition_key,
                                        &sub_range,
                                        PrefetchOptions::default(),
                                    )
                                    .await?;
                                pin_mut!(iter);
                                while let Some(item) = iter.next().await {
                                    let row = item?.into_owned_row();
                                    let seq = row.datum_at(0).expect("seq not null").into_int64();
                                    let input_row = OwnedRow::new(
                                        (1..1 + input_arity)
                                            .map(|i| row.datum_at(i).to_owned_datum())
                                            .collect(),
                                    );
                                    let order_key = input_row.datum_at(time_col).to_owned_datum();
                                    rows.push(BufferedRow {
                                        seq,
                                        order_key,
                                        row: input_row,
                                    });
                                }
                            }

                            if rows.is_empty() {
                                // Stale frontier entry for an empty partition: drop it.
                                Self::remove_frontier(
                                    &mut frontier_meta_table,
                                    &mut frontier_index_table,
                                    &partition_key,
                                    &old_wakeup,
                                );
                                continue;
                            }

                            // The prefix with leading order_key <= w is final.
                            let safe_len = rows
                                .iter()
                                .take_while(|r| {
                                    matches!(&r.order_key, Some(k) if k.default_cmp(&w).is_le())
                                })
                                .count();
                            // WITHIN-deadline memo for the safe prefix (see `deadline_at`); only
                            // sized when a WITHIN bound exists.
                            let mut deadline_memo: Vec<Option<Datum>> = if within_deadline.is_some()
                            {
                                vec![None; safe_len]
                            } else {
                                Vec::new()
                            };
                            // Evaluate DEFINE predicates against the in-progress match; the matcher
                            // borrows `rows`.
                            let matcher = DefineMatcher {
                                rows: &rows,
                                safe_len,
                                defines: &defines,
                                within: within.as_ref(),
                            };
                            let found = nfa.find_matches_dynamic(safe_len, &matcher, &skip).await?;
                            let mut cursor = 0usize;
                            for m in found {
                                if m.start < cursor {
                                    continue;
                                }
                                // At the safe boundary we normally wait for a trailing safe row to
                                // confirm the greedy match is maximal (it might still extend). But
                                // under WITHIN, once the watermark has passed this match's deadline
                                // (its first row's order_key + interval), no future row can legally
                                // extend it — any extension would fall inside the now-fully-safe
                                // window — so it is final and must be emitted now, before the WITHIN
                                // eviction below drops its rows. Without WITHIN, keep waiting.
                                if m.end >= safe_len {
                                    let within_final = if let Some(dl) = &within_deadline {
                                        let deadline = Self::deadline_at(
                                            dl,
                                            &rows,
                                            &mut deadline_memo,
                                            m.start,
                                        )
                                        .await;
                                        matches!(&deadline, Some(d) if d.default_cmp(&w).is_le())
                                    } else {
                                        false
                                    };
                                    // `break`, not `continue`. `found` is ordered by start; the WITHIN
                                    // bound is a constant interval and rows are PK-sorted by order key,
                                    // so a match's deadline (its first order key + interval) is monotone
                                    // non-decreasing in its start. Once one boundary match is not yet
                                    // within-final, no later one can be either, so stopping is correct.
                                    // `continue` would be actively wrong: it could emit a later, shorter
                                    // match, advance `cursor` past this held boundary match, and then
                                    // let the eviction below delete this match's start row — losing it.
                                    // With `break` the held match's rows are retained (a complete match
                                    // at the boundary is `reaches_boundary_alive`), and any shorter
                                    // overlapping match is re-found on a later watermark (at worst
                                    // delayed to this match's WITHIN deadline), never lost.
                                    if !within_final {
                                        break;
                                    }
                                }
                                // WITHIN is enforced inside the matcher. Evaluate each measure over
                                // the synthetic row its slots produce from the matched rows + labels.
                                let mut measure_datums: Vec<Datum> =
                                    Vec::with_capacity(measures.len());
                                for measure in &measures {
                                    let mut synthetic = Vec::with_capacity(measure.slots.len());
                                    for slot in &measure.slots {
                                        synthetic
                                            .push(slot.resolve(&rows, m.start, &m.labels).await?);
                                    }
                                    let synthetic = OwnedRow::new(synthetic);
                                    let value = measure.expr.eval_row_infallible(&synthetic).await;
                                    measure_datums.push(value);
                                }
                                // The match's identity is its start row's `seq`: deterministic
                                // across recovery replay (re-emission after a rollback reproduces
                                // byte-identical output, unlike a freshly minted id), and unique
                                // forever because an emitted match's start row is always evicted
                                // below (the same invariant that prevents cross-watermark double
                                // emits), so no later match can ever share the start. A stable,
                                // replay-deterministic identity is also the foundation an
                                // emit-on-update mode would retract against.
                                let match_id = rows[m.start].seq;
                                // Stream the match into the chunk builder, flushing a full chunk the
                                // moment it fills — output memory stays bounded by one chunk.
                                let measures_row = OwnedRow::new(measure_datums);
                                if let Some(c) = builder.append_row(
                                    Op::Insert,
                                    (&partition_key)
                                        .chain(&measures_row)
                                        .chain(once(Some(ScalarImpl::Int64(match_id)))),
                                ) {
                                    yield Message::Chunk(c);
                                }
                                cursor = skip.next_pos(m.start, m.end, &m.labels);
                            }

                            // Evict finalized rows that can no longer be part of any match. Retain
                            // from the earliest row that is still a live match start; drop everything
                            // before it. A start at position `p` is live only if BOTH hold:
                            //
                            //  * it is structurally alive at the safe boundary — a match from `p` can
                            //    still reach the boundary given the safe rows so far. (It is not enough
                            //    that `p` can merely *begin* the pattern: for `(a b)`, a buffer
                            //    `[a, x, x]` whose `a` is followed only by non-matching safe rows can
                            //    never complete, though the `a` can still begin it.)
                            //
                            //  * its WITHIN window is still open — `order_key + interval > w`. Once the
                            //    watermark passes that deadline, every row within the bound is final,
                            //    so if no match from `p` has completed by now none ever can, and `p` is
                            //    dead even though the NFA is structurally still expecting more input.
                            //    This is what bounds idle-partition state (see the doc's state bound):
                            //    reaches_boundary_alive alone would keep a lone `[a]` forever.
                            //
                            // The partition iterator is already dropped, so delete in place.
                            let mut retain_from = safe_len;
                            for (p, _) in rows.iter().enumerate().take(safe_len).skip(cursor) {
                                if let Some(deadline_expr) = &within_deadline {
                                    let deadline = Self::deadline_at(
                                        deadline_expr,
                                        &rows,
                                        &mut deadline_memo,
                                        p,
                                    )
                                    .await;
                                    // Window closed (deadline <= w): `p` is dead, skip it.
                                    if matches!(&deadline, Some(d) if d.default_cmp(&w).is_le()) {
                                        continue;
                                    }
                                }
                                if nfa.reaches_boundary_alive(p, safe_len, &matcher).await? {
                                    retain_from = p;
                                    break;
                                }
                            }
                            // Delete by reference: the seq datum chained onto the borrowed buffered
                            // row (`delete` takes `impl Row`), so no owned copy is built per evictee.
                            for c in &rows[0..retain_from] {
                                state_table
                                    .delete(once(Some(ScalarImpl::Int64(c.seq))).chain(&c.row));
                            }

                            // Recompute the wakeup frontier as the earlier of two events:
                            //
                            //  (a) the next row to become safe — the earliest surviving row whose
                            //      order_key is still past the watermark. Survivors `rows[retain_from..]`
                            //      are PK-ordered, so it is the first such row.
                            //
                            //  (b) the earliest WITHIN expiry of a retained live partial — so an idle
                            //      partition (no future row) is still woken to evict a partial that
                            //      times out. The earliest retained safe partial is `rows[retain_from]`
                            //      (if it is `<= w`); its deadline is `first_order_key + interval`,
                            //      evaluated from `within_deadline`. After that watermark the existing
                            //      eviction predicate (which already honours WITHIN) drops it.
                            //
                            // If neither exists, drop the frontier entry — a later insert re-schedules.
                            let row_wakeup: Option<Datum> = rows[retain_from..]
                                .iter()
                                .find(|r| {
                                    matches!(&r.order_key, Some(k) if k.default_cmp(&w).is_gt())
                                })
                                .map(|r| r.order_key.clone());
                            let within_wakeup: Option<Datum> = match &within_deadline {
                                Some(deadline_expr)
                                    if retain_from < rows.len()
                                        && matches!(
                                            &rows[retain_from].order_key,
                                            Some(k) if k.default_cmp(&w).is_le()
                                        ) =>
                                {
                                    let dl = Self::deadline_at(
                                        deadline_expr,
                                        &rows,
                                        &mut deadline_memo,
                                        retain_from,
                                    )
                                    .await;
                                    // A null deadline (only on eval error) carries no schedule.
                                    dl.is_some().then_some(dl)
                                }
                                _ => None,
                            };
                            let new_wakeup: Option<Datum> = match (row_wakeup, within_wakeup) {
                                (Some(a), Some(b)) => Some(
                                    if a.as_ref().unwrap().default_cmp(b.as_ref().unwrap()).is_le()
                                    {
                                        a
                                    } else {
                                        b
                                    },
                                ),
                                (a, b) => a.or(b),
                            };
                            match new_wakeup {
                                Some(nw) => {
                                    // The re-pointed wakeup is > w, so it belongs in the recomputed
                                    // lower bound.
                                    if let Some(k) = &nw {
                                        Self::fold_min(&mut next_min, k);
                                    }
                                    Self::move_frontier(
                                        &mut frontier_meta_table,
                                        &mut frontier_index_table,
                                        &partition_key,
                                        &old_wakeup,
                                        nw,
                                    )
                                }
                                None => Self::remove_frontier(
                                    &mut frontier_meta_table,
                                    &mut frontier_index_table,
                                    &partition_key,
                                    &old_wakeup,
                                ),
                            }
                        }
                    }
                    // The scan visited every owned vnode, so `next_min` is now the exact minimum
                    // remaining `next_wakeup` — known until the next insert lowers it.
                    min_wakeup = Some(next_min);
                    if let Some(c) = builder.take() {
                        yield Message::Chunk(c);
                    }
                }
                Message::Barrier(barrier) => {
                    // Commit all three state tables at this epoch. The frontier tables are
                    // distributed by partition like the buffer table, so they re-shard together on a
                    // vnode-bitmap change.
                    let post_commit = state_table.commit(barrier.epoch).await?;
                    let meta_post_commit = frontier_meta_table.commit(barrier.epoch).await?;
                    let index_post_commit = frontier_index_table.commit(barrier.epoch).await?;
                    let update_vnode_bitmap = barrier.as_update_vnode_bitmap(ctx.id);
                    yield Message::Barrier(barrier);
                    meta_post_commit
                        .post_yield_barrier(update_vnode_bitmap.clone())
                        .await?;
                    index_post_commit
                        .post_yield_barrier(update_vnode_bitmap.clone())
                        .await?;
                    // On a vnode-bitmap change (rescaling) the set of partitions this actor owns
                    // shifts. There is no in-memory buffer to reload — the state table is
                    // authoritative and the next watermark scans whatever vnodes are now owned. Rebuild
                    // the id generator only when the owned set may have *grown* (`cache_may_stale`), so
                    // new ids fall in the now-owned range. This is deliberately narrower than
                    // `RowIdGenExecutor`, which rebuilds on any bitmap change: there the generated id
                    // *is* the row-id distribution key, so a stale vnode would misplace rows. Here `seq`
                    // is only a within-partition PK tiebreaker (and, via a match's start row, the
                    // output `_match_id`) — not a distribution key, so a lost vnode never misplaces
                    // state, and the snowflake timestamp keeps ids unique regardless. Rebuilding on
                    // growth alone suffices.
                    if let Some((_, cache_may_stale)) =
                        post_commit.post_yield_barrier(update_vnode_bitmap).await?
                    {
                        // The owned-vnode set changed: the in-memory frontier lower bound no longer
                        // describes it. Unknown forces the next watermark to scan and re-establish.
                        min_wakeup = None;
                        if cache_may_stale {
                            row_id_gen = RowIdGenerator::new(
                                state_table.vnodes().iter_vnodes(),
                                state_table.vnodes().len(),
                            );
                        }
                    }
                }
            }
        }
    }
}

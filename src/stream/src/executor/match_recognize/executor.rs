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
//! Scope: append-only input, `ONE ROW PER MATCH`, `AFTER MATCH SKIP {PAST LAST ROW | TO NEXT ROW}`,
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
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::{DataType, Datum, DefaultOrd, ScalarImpl, ToOwnedDatum};
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
    /// The aggregate kernel for [`MeasureSlotKind::Sum`] / [`MeasureSlotKind::Avg`].
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
                    1 => MeasureSlotKind::First,
                    2 => MeasureSlotKind::Classifier,
                    3 => MeasureSlotKind::CountStar,
                    4 => MeasureSlotKind::Count,
                    5 => MeasureSlotKind::Min,
                    6 => MeasureSlotKind::Max,
                    7 => MeasureSlotKind::Sum,
                    _ => MeasureSlotKind::Last,
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
            .map(|s| DefineSlot {
                kind: match s.kind {
                    1 => DefineSlotKind::Prev,
                    2 => DefineSlotKind::Next,
                    3 => DefineSlotKind::RunningFirst,
                    4 => DefineSlotKind::RunningLast,
                    _ => DefineSlotKind::SelfCol,
                },
                vars: s.vars.clone(),
                col_idx: s.col_idx as usize,
                offset: s.offset as usize,
            })
            .collect();
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
    pub nfa: Nfa,
    pub skip: SkipMode,
    /// Number of input columns; the buffered raw input row stored per row in the state table.
    pub input_arity: usize,
    pub state_table: StateTable<S>,
}

pub struct MatchRecognizeExecutor<S: StateStore> {
    ctx: ActorContextRef,
    input: Executor,
    schema: Schema,
    chunk_size: usize,
    partition_key_indices: Vec<usize>,
    /// Input column indices of all ORDER BY columns, used for a total sort of the buffer. The first
    /// is the leading (watermark) column; the rest break ties so the row order is total.
    order_key_indices: Vec<usize>,
    /// Input column index of the leading ORDER BY column (the watermark column).
    time_col: usize,
    measures: Vec<CompiledMeasure>,
    /// Compiled `DEFINE` predicates keyed by their pattern variable.
    defines: HashMap<String, CompiledDefine>,
    within: Option<NonStrictExpression>,
    nfa: Nfa,
    skip: SkipMode,
    input_arity: usize,
    state_table: StateTable<S>,
}

/// A buffered input row within one partition.
struct BufferedRow {
    /// Per-partition monotonic id; the state-table key suffix.
    seq: i64,
    /// Leading ORDER BY value (a copy of `row[time_col]`), used to sort the buffer and compare
    /// against the watermark.
    order_key: Datum,
    /// The raw input row, read by DEFINE and MEASURES navigation slots at match time.
    row: OwnedRow,
}

/// Per-partition match state: buffered rows and the next seq to assign.
#[derive(Default)]
struct PartitionState {
    rows: Vec<BufferedRow>,
    next_seq: i64,
    /// Whether `rows` needs (re)sorting by order key before the next match. Set when rows are
    /// appended (inserts, recovery) since they land out of order at the tail; cleared after a sort.
    /// Draining the sorted front keeps the remainder sorted, so a watermark that adds no new rows
    /// can skip the sort entirely.
    needs_sort: bool,
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
            order_key_indices: args.order_key_indices,
            time_col,
            measures: args.measures,
            defines,
            within: args.within,
            nfa: args.nfa,
            skip: args.skip,
            input_arity: args.input_arity,
            state_table: args.state_table,
        }
    }

    /// Build a state-table row: `[ seq , <input cols..> ]`. The partition columns and order key are
    /// columns of the stored input row (the key is the partition columns plus `seq`), so they are
    /// not stored separately.
    fn state_row(row: &BufferedRow) -> OwnedRow {
        let mut datums: Vec<Datum> = Vec::with_capacity(1 + row.row.len());
        datums.push(Some(ScalarImpl::Int64(row.seq)));
        datums.extend(row.row.iter().map(|d| d.to_owned_datum()));
        OwnedRow::new(datums)
    }

    /// (Re)build the per-partition buffers from the state table by scanning every vnode this actor
    /// owns. Used both on recovery and after a vnode-bitmap change (rescaling), where ownership of
    /// partitions shifts: a full reload drops partitions no longer owned and picks up newly-owned
    /// ones. The state is distributed by the partition key, so an empty-prefix scan must go vnode by
    /// vnode (a vnode cannot be computed from an empty prefix).
    async fn load_partitions(
        state_table: &StateTable<S>,
        input_arity: usize,
        time_col: usize,
        partition_key_indices: &[usize],
    ) -> StreamExecutorResult<HashMap<OwnedRow, PartitionState>> {
        let mut partitions: HashMap<OwnedRow, PartitionState> = HashMap::new();
        let sub_range: (Bound<&[Datum]>, Bound<&[Datum]>) = (Bound::Unbounded, Bound::Unbounded);
        let vnodes = state_table.vnodes().clone();
        for vnode in vnodes.iter_vnodes() {
            let iter = state_table
                .iter_with_vnode(vnode, &sub_range, PrefetchOptions::default())
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
                let partition_key = (&input_row).project(partition_key_indices).to_owned_row();
                let state = partitions.entry(partition_key).or_default();
                state.rows.push(BufferedRow {
                    seq,
                    order_key,
                    row: input_row,
                });
                state.next_seq = state.next_seq.max(seq + 1);
                // Recovered rows arrive in state-table (seq) order, not order-key order.
                state.needs_sort = true;
            }
        }
        Ok(partitions)
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
            order_key_indices,
            time_col,
            measures,
            defines,
            within,
            nfa,
            skip,
            input_arity,
            mut state_table,
        } = *self;

        let mut input = input.execute();
        let barrier = expect_first_barrier(&mut input).await?;
        let first_epoch = barrier.epoch;
        yield Message::Barrier(barrier);
        state_table.init_epoch(first_epoch).await?;

        // Monotonic per-match id, the hidden unique stream key. Seeded from the (meta-guaranteed
        // monotonic) epoch and re-seeded up to each barrier's epoch, so ids strictly increase within
        // a run and across restarts (a recovered run starts from an epoch greater than any before
        // it). Combined with the partition columns this uniquely identifies every emitted match.
        let mut next_match_id: i64 = first_epoch.curr as i64;

        // Recovery: rebuild the per-partition buffers from the state table.
        let mut partitions =
            Self::load_partitions(&state_table, input_arity, time_col, &partition_key_indices)
                .await?;

        #[for_await]
        for msg in input {
            match msg? {
                Message::Chunk(chunk) => {
                    // Append-only input: buffer the raw rows; DEFINE and MEASURES are both evaluated
                    // on the watermark, against the in-progress match.
                    let chunk = chunk.compact_vis();
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
                        let partition_key = row_ref.project(&partition_key_indices).to_owned_row();
                        let order_key = row_ref.datum_at(time_col).to_owned_datum();
                        // A row with a NULL order key has no event time: it can never fall under the
                        // watermark, so it would never be finalized or evicted (it would linger in the
                        // buffer forever) and cannot be meaningfully ordered against other rows. Drop
                        // it, as event-time processing does with NULL-rowtime rows.
                        if order_key.is_none() {
                            continue;
                        }

                        let state = partitions.entry(partition_key).or_default();
                        let seq = state.next_seq;
                        state.next_seq += 1;
                        let buffered = BufferedRow {
                            seq,
                            order_key,
                            row: row_ref.to_owned_row(),
                        };
                        state_table.insert(Self::state_row(&buffered));
                        state.rows.push(buffered);
                        state.needs_sort = true;
                    }
                }
                Message::Watermark(watermark) => {
                    // Only the leading ORDER BY column drives matching.
                    if watermark.col_idx != time_col {
                        continue;
                    }
                    let w = watermark.val;

                    let mut out_rows: Vec<OwnedRow> = Vec::new();
                    for (partition_key, state) in &mut partitions {
                        // Total sort by all ORDER BY columns (lexicographically), so rows with equal
                        // leading keys are still deterministically ordered. The leading column also
                        // drives the watermark: the prefix with leading order_key <= w is final.
                        // Skip when no rows were appended since the last sort (the buffer is still
                        // ordered — draining the sorted front preserves order).
                        if state.needs_sort {
                            state.rows.sort_by(|a, b| {
                                order_key_indices
                                    .iter()
                                    .map(|&oc| {
                                        a.row
                                            .datum_at(oc)
                                            .to_owned_datum()
                                            .default_cmp(&b.row.datum_at(oc).to_owned_datum())
                                    })
                                    .find(|o| o.is_ne())
                                    .unwrap_or(std::cmp::Ordering::Equal)
                            });
                            state.needs_sort = false;
                        }
                        let safe_len = state
                            .rows
                            .iter()
                            .take_while(
                                |r| matches!(&r.order_key, Some(k) if k.default_cmp(&w).is_le()),
                            )
                            .count();

                        // Evaluate DEFINE predicates against the in-progress match while matching.
                        // The matcher borrows `state.rows`; it is scoped so the borrow ends before
                        // the drain below.
                        let retain_from = {
                            let matcher = DefineMatcher {
                                rows: &state.rows,
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
                                // Final only if another safe row follows (greedy match is maximal).
                                if m.end >= safe_len {
                                    break;
                                }
                                // WITHIN is enforced inside the matcher (it prunes overshooting
                                // candidates during traversal), so no post-hoc span check is needed.
                                // Evaluate each measure over the synthetic row its slots produce from
                                // the matched rows and their per-row labels.
                                let mut measure_datums: Vec<Datum> =
                                    Vec::with_capacity(measures.len());
                                for measure in &measures {
                                    let mut synthetic = Vec::with_capacity(measure.slots.len());
                                    for slot in &measure.slots {
                                        synthetic.push(
                                            slot.resolve(&state.rows, m.start, &m.labels).await?,
                                        );
                                    }
                                    let synthetic = OwnedRow::new(synthetic);
                                    let value = measure.expr.eval_row_infallible(&synthetic).await;
                                    measure_datums.push(value);
                                }
                                let match_id = next_match_id;
                                next_match_id += 1;
                                out_rows.push(
                                    partition_key
                                        .clone()
                                        .chain(OwnedRow::new(measure_datums))
                                        .chain(OwnedRow::new(vec![Some(ScalarImpl::Int64(
                                            match_id,
                                        ))]))
                                        .into_owned_row(),
                                );
                                cursor = skip.next_pos(m.start, m.end, &m.labels);
                            }

                            // Evict finalized rows that can no longer be part of any match: everything
                            // before the earliest safe row that could still *begin* a match. Rows the
                            // scan consumed sit before `cursor`; rows after it that cannot start the
                            // pattern are dead (a match is contiguous from its start), so they are
                            // dropped instead of lingering and being re-scanned every watermark. A live
                            // partial match (a row that begins the pattern but needs future rows) is
                            // retained.
                            let mut retain_from = safe_len;
                            for p in cursor..safe_len {
                                if nfa.can_begin_at(p, &matcher).await? {
                                    retain_from = p;
                                    break;
                                }
                            }
                            retain_from
                        };
                        for consumed in state.rows.drain(0..retain_from) {
                            state_table.delete(Self::state_row(&consumed));
                        }
                    }

                    // Drop partitions whose buffer is now empty so the map is bounded by the number
                    // of *live* partitions, not every partition key ever seen. A later row for such a
                    // key simply recreates the entry (its state-table rows were already deleted, so
                    // the fresh seq counter starts clean).
                    partitions.retain(|_, state| !state.rows.is_empty());

                    let mut builder = StreamChunkBuilder::new(chunk_size, schema.data_types());
                    for row in out_rows {
                        if let Some(c) = builder.append_row(Op::Insert, row) {
                            yield Message::Chunk(c);
                        }
                    }
                    if let Some(c) = builder.take() {
                        yield Message::Chunk(c);
                    }
                }
                Message::Barrier(barrier) => {
                    // Keep match ids ahead of the epoch clock so they stay unique across restarts.
                    next_match_id = next_match_id.max(barrier.epoch.curr as i64);
                    let post_commit = state_table.commit(barrier.epoch).await?;
                    let update_vnode_bitmap = barrier.as_update_vnode_bitmap(ctx.id);
                    yield Message::Barrier(barrier);
                    // On a vnode-bitmap change (rescaling) the set of partitions this actor owns
                    // shifts. The in-memory buffer is authoritative, not read-through, so reload it
                    // from the (now-migrated) state table: this drops partitions no longer owned and
                    // picks up newly-owned ones. Without it the buffer desyncs from the state table.
                    if let Some((_, cache_may_stale)) =
                        post_commit.post_yield_barrier(update_vnode_bitmap).await?
                        && cache_may_stale
                    {
                        partitions = Self::load_partitions(
                            &state_table,
                            input_arity,
                            time_col,
                            &partition_key_indices,
                        )
                        .await?;
                    }
                }
            }
        }
    }
}

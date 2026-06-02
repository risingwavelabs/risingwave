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

use std::collections::{BTreeSet, HashMap};
use std::ops::Bound;

use futures::{StreamExt, pin_mut};
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::{DataType, Datum, DefaultOrd, ScalarImpl, ToOwnedDatum};
use risingwave_expr::aggregate::{AggCall, BoxedAggregateFunction, build_append_only};
use risingwave_expr::expr::{EvalErrorReport, NonStrictExpression, build_non_strict_from_prost};
use risingwave_pb::stream_plan::MatchRecognizeMeasure as PbMatchRecognizeMeasure;
use risingwave_storage::StateStore;

use super::nfa::{Nfa, SkipMode};
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
    /// `SUM(var.col)` / `AVG(var.col)`, evaluated by the slot's [`AggSlot`] aggregate kernel.
    Sum,
    /// See [`MeasureSlotKind::Sum`].
    Avg,
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
    /// Pattern variable to navigate to. Unused for [`MeasureSlotKind::Classifier`].
    var: String,
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
            pb.expr.as_ref().expect("match_recognize measure expr"),
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
                    8 => MeasureSlotKind::Avg,
                    _ => MeasureSlotKind::Last,
                };
                let agg = match kind {
                    MeasureSlotKind::Sum | MeasureSlotKind::Avg => {
                        let call = AggCall::from_protobuf(
                            s.agg_call.as_ref().expect("sum/avg slot needs agg_call"),
                        )?;
                        let col_type = call.args.arg_types()[0].clone();
                        let func = build_append_only(&call)?;
                        Some(AggSlot { func, col_type })
                    }
                    _ => None,
                };
                Ok(MeasureSlot {
                    kind,
                    var: s.var.clone(),
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
        Ok(match self.kind {
            MeasureSlotKind::Classifier => {
                labels.last().map(|s| ScalarImpl::Utf8(s.as_str().into()))
            }
            MeasureSlotKind::First => labels.iter().position(|l| l == &self.var).and_then(col_at),
            MeasureSlotKind::Last => labels.iter().rposition(|l| l == &self.var).and_then(col_at),
            MeasureSlotKind::CountStar => Some(ScalarImpl::Int64(labels.len() as i64)),
            MeasureSlotKind::Count => {
                let n = labels
                    .iter()
                    .enumerate()
                    .filter(|(j, l)| *l == &self.var && col_at(*j).is_some())
                    .count();
                Some(ScalarImpl::Int64(n as i64))
            }
            MeasureSlotKind::Min => labels
                .iter()
                .enumerate()
                .filter(|(_, l)| *l == &self.var)
                .filter_map(|(j, _)| col_at(j))
                .min_by(|a, b| a.default_cmp(b)),
            MeasureSlotKind::Max => labels
                .iter()
                .enumerate()
                .filter(|(_, l)| *l == &self.var)
                .filter_map(|(j, _)| col_at(j))
                .max_by(|a, b| a.default_cmp(b)),
            MeasureSlotKind::Sum | MeasureSlotKind::Avg => {
                let agg = self.agg.as_ref().expect("sum/avg slot has an aggregate kernel");
                // Feed the kernel a single-column chunk of the col values over rows labeled `var`.
                let input: Vec<(Op, OwnedRow)> = labels
                    .iter()
                    .enumerate()
                    .filter(|(_, l)| *l == &self.var)
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

pub struct MatchRecognizeExecutorArgs<S: StateStore> {
    pub ctx: ActorContextRef,
    pub input: Executor,
    /// Output schema: the `PARTITION BY` columns followed by the `MEASURES` columns.
    pub schema: Schema,
    pub chunk_size: usize,
    pub partition_key_indices: Vec<usize>,
    pub order_key_indices: Vec<usize>,
    pub measures: Vec<CompiledMeasure>,
    pub define_symbols: Vec<String>,
    pub define_exprs: Vec<NonStrictExpression>,
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
    /// Input column index of the leading ORDER BY column (the watermark column).
    time_col: usize,
    measures: Vec<CompiledMeasure>,
    define_symbols: Vec<String>,
    define_exprs: Vec<NonStrictExpression>,
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
    /// The pattern variables whose `DEFINE` predicate this row satisfies.
    satisfied: BTreeSet<String>,
    /// The raw input row, read by measure navigation slots at match time.
    row: OwnedRow,
}

/// Per-partition match state: buffered rows and the next seq to assign.
#[derive(Default)]
struct PartitionState {
    rows: Vec<BufferedRow>,
    next_seq: i64,
}

fn join_satisfied(satisfied: &BTreeSet<String>) -> String {
    satisfied.iter().cloned().collect::<Vec<_>>().join(",")
}

fn split_satisfied(s: &str) -> BTreeSet<String> {
    s.split(',').filter(|p| !p.is_empty()).map(|p| p.to_owned()).collect()
}

impl<S: StateStore> MatchRecognizeExecutor<S> {
    pub fn new(args: MatchRecognizeExecutorArgs<S>) -> Self {
        let time_col = args.order_key_indices[0];
        Self {
            ctx: args.ctx,
            input: args.input,
            schema: args.schema,
            chunk_size: args.chunk_size,
            partition_key_indices: args.partition_key_indices,
            time_col,
            measures: args.measures,
            define_symbols: args.define_symbols,
            define_exprs: args.define_exprs,
            nfa: args.nfa,
            skip: args.skip,
            input_arity: args.input_arity,
            state_table: args.state_table,
        }
    }

    /// Build a state-table row: `[ seq , satisfied , <input cols..> ]`. The partition columns and
    /// order key are columns of the stored input row (the state-table key is the partition columns
    /// plus `seq`), so they are not stored separately.
    fn state_row(row: &BufferedRow) -> OwnedRow {
        let mut datums: Vec<Datum> = Vec::with_capacity(2 + row.row.len());
        datums.push(Some(ScalarImpl::Int64(row.seq)));
        datums.push(Some(ScalarImpl::Utf8(join_satisfied(&row.satisfied).into())));
        datums.extend(row.row.iter().map(|d| d.to_owned_datum()));
        OwnedRow::new(datums)
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
            define_symbols,
            define_exprs,
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

        // Recovery: rebuild the per-partition buffers from the state table.
        let mut partitions: HashMap<OwnedRow, PartitionState> = HashMap::new();
        {
            let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) = &(Bound::Unbounded, Bound::Unbounded);
            let iter = state_table
                .iter_with_prefix(None::<OwnedRow>, sub_range, Default::default())
                .await?;
            pin_mut!(iter);
            while let Some(item) = iter.next().await {
                let row = item?.into_owned_row();
                let seq = row.datum_at(0).expect("seq not null").into_int64();
                let satisfied = row
                    .datum_at(1)
                    .map(|d| split_satisfied(d.into_utf8()))
                    .unwrap_or_default();
                let input_row = OwnedRow::new(
                    (2..2 + input_arity)
                        .map(|i| row.datum_at(i).to_owned_datum())
                        .collect(),
                );
                let order_key = input_row.datum_at(time_col).to_owned_datum();
                let partition_key = (&input_row).project(&partition_key_indices).to_owned_row();
                let state = partitions.entry(partition_key).or_default();
                state.rows.push(BufferedRow {
                    seq,
                    order_key,
                    satisfied,
                    row: input_row,
                });
                state.next_seq = state.next_seq.max(seq + 1);
            }
        }

        #[for_await]
        for msg in input {
            match msg? {
                Message::Chunk(chunk) => {
                    // Append-only input: buffer rows; matching happens on watermark. DEFINE
                    // predicates are current-row, so they are evaluated here; MEASURES reference
                    // specific matched rows and are deferred to match time.
                    let chunk = chunk.compact_vis();
                    let data_chunk = chunk.data_chunk();

                    let mut define_cols = Vec::with_capacity(define_exprs.len());
                    for e in &define_exprs {
                        define_cols.push(e.eval_infallible(data_chunk).await);
                    }

                    for (idx, (op, row_ref)) in chunk.rows().enumerate() {
                        if !matches!(op, Op::Insert) {
                            continue;
                        }
                        let partition_key = row_ref.project(&partition_key_indices).to_owned_row();
                        let order_key = row_ref.datum_at(time_col).to_owned_datum();

                        let mut satisfied = BTreeSet::new();
                        for (d, sym) in define_symbols.iter().enumerate() {
                            if matches!(define_cols[d].value_at(idx), Some(s) if s.into_bool()) {
                                satisfied.insert(sym.clone());
                            }
                        }

                        let state = partitions.entry(partition_key).or_default();
                        let seq = state.next_seq;
                        state.next_seq += 1;
                        let buffered = BufferedRow {
                            seq,
                            order_key,
                            satisfied,
                            row: row_ref.to_owned_row(),
                        };
                        state_table.insert(Self::state_row(&buffered));
                        state.rows.push(buffered);
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
                        // Sort by order key; the prefix with order_key <= w is final.
                        state.rows.sort_by(|a, b| a.order_key.default_cmp(&b.order_key));
                        let safe_len = state
                            .rows
                            .iter()
                            .take_while(|r| matches!(&r.order_key, Some(k) if k.default_cmp(&w).is_le()))
                            .count();
                        let satisfied: Vec<BTreeSet<String>> = state.rows[..safe_len]
                            .iter()
                            .map(|r| r.satisfied.clone())
                            .collect();

                        let mut cursor = 0usize;
                        for m in nfa.find_matches_labeled(&satisfied, skip) {
                            if m.start < cursor {
                                continue;
                            }
                            // Final only if another safe row follows (greedy match is maximal).
                            if m.end >= safe_len {
                                break;
                            }
                            // Evaluate each measure over the synthetic row its slots produce from
                            // the matched rows and their per-row labels.
                            let mut measure_datums: Vec<Datum> = Vec::with_capacity(measures.len());
                            for measure in &measures {
                                let mut synthetic = Vec::with_capacity(measure.slots.len());
                                for slot in &measure.slots {
                                    synthetic
                                        .push(slot.resolve(&state.rows, m.start, &m.labels).await?);
                                }
                                let synthetic = OwnedRow::new(synthetic);
                                let value = measure.expr.eval_row_infallible(&synthetic).await;
                                measure_datums.push(value);
                            }
                            out_rows.push(
                                partition_key
                                    .clone()
                                    .chain(OwnedRow::new(measure_datums))
                                    .into_owned_row(),
                            );
                            cursor = match skip {
                                SkipMode::PastLastRow => m.end,
                                SkipMode::ToNextRow => m.start + 1,
                            };
                        }
                        for consumed in state.rows.drain(0..cursor) {
                            state_table.delete(Self::state_row(&consumed));
                        }
                    }

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
                    let post_commit = state_table.commit(barrier.epoch).await?;
                    let update_vnode_bitmap = barrier.as_update_vnode_bitmap(ctx.id);
                    yield Message::Barrier(barrier);
                    post_commit.post_yield_barrier(update_vnode_bitmap).await?;
                }
            }
        }
    }
}

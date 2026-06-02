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
//! scalar `MEASURES` evaluated on the match's last row.
//!
//! Event-time model: rows are buffered per partition (in any arrival order). Matching is driven by
//! the watermark on the leading `ORDER BY` column: when the watermark advances to `w`, every row
//! with `order_key <= w` is final (no earlier row can still arrive), so the buffer is sorted by
//! order key and the `<= w` prefix is matched. A match is emitted once it is followed by another
//! safe row (so the greedy match is known maximal); consumed rows are evicted. This handles
//! out-of-order arrival within the watermark's lateness and bounds state.
//!
//! State: the buffered rows (order key, satisfied pattern variables, pre-evaluated MEASURES) are
//! persisted to a state table — written through on arrival, deleted on consumption — and restored
//! on recovery.

use std::collections::{BTreeSet, HashMap};
use std::ops::Bound;

use futures::{StreamExt, pin_mut};
use risingwave_common::array::Op;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::{Datum, DefaultOrd, ScalarImpl, ToOwnedDatum};
use risingwave_expr::expr::NonStrictExpression;
use risingwave_storage::StateStore;

use super::nfa::{Nfa, SkipMode};
use crate::common::table::state_table::StateTable;
use crate::executor::prelude::*;

pub struct MatchRecognizeExecutorArgs<S: StateStore> {
    pub ctx: ActorContextRef,
    pub input: Executor,
    /// Output schema: the `PARTITION BY` columns followed by the `MEASURES` columns.
    pub schema: Schema,
    pub chunk_size: usize,
    pub partition_key_indices: Vec<usize>,
    pub order_key_indices: Vec<usize>,
    pub measures: Vec<NonStrictExpression>,
    pub define_symbols: Vec<String>,
    pub define_exprs: Vec<NonStrictExpression>,
    pub nfa: Nfa,
    pub skip: SkipMode,
    /// Indices into `measures` that are `CLASSIFIER()`: filled at emit with the pattern variable
    /// bound to the match's last row rather than the (placeholder) pre-evaluated measure value.
    pub classifier_measure_indices: Vec<usize>,
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
    measures: Vec<NonStrictExpression>,
    define_symbols: Vec<String>,
    define_exprs: Vec<NonStrictExpression>,
    nfa: Nfa,
    skip: SkipMode,
    classifier_measure_indices: Vec<usize>,
    state_table: StateTable<S>,
}

/// A buffered input row within one partition.
struct BufferedRow {
    /// Per-partition monotonic id; the state-table key suffix.
    seq: i64,
    /// Leading ORDER BY value, used to sort the buffer and compare against the watermark.
    order_key: Datum,
    /// The pattern variables whose `DEFINE` predicate this row satisfies.
    satisfied: BTreeSet<String>,
    /// Pre-evaluated `MEASURES` values for this row (used when it is a match's last row).
    measures: OwnedRow,
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
            classifier_measure_indices: args.classifier_measure_indices,
            state_table: args.state_table,
        }
    }

    /// Build a state-table row: `[ partition cols.. , seq , order_key , satisfied , measure cols.. ]`.
    fn state_row(partition_key: &OwnedRow, row: &BufferedRow) -> OwnedRow {
        let mut datums: Vec<Datum> = partition_key.iter().map(|d| d.to_owned_datum()).collect();
        datums.push(Some(ScalarImpl::Int64(row.seq)));
        datums.push(row.order_key.clone());
        datums.push(Some(ScalarImpl::Utf8(join_satisfied(&row.satisfied).into())));
        datums.extend(row.measures.iter().map(|d| d.to_owned_datum()));
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
            classifier_measure_indices,
            mut state_table,
        } = *self;

        let n_part = partition_key_indices.len();
        let n_measure = measures.len();

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
                let partition_key =
                    OwnedRow::new((0..n_part).map(|i| row.datum_at(i).to_owned_datum()).collect());
                let seq = row.datum_at(n_part).expect("seq not null").into_int64();
                let order_key = row.datum_at(n_part + 1).to_owned_datum();
                let satisfied = row
                    .datum_at(n_part + 2)
                    .map(|d| split_satisfied(d.into_utf8()))
                    .unwrap_or_default();
                let measures_row = OwnedRow::new(
                    (n_part + 3..n_part + 3 + n_measure)
                        .map(|i| row.datum_at(i).to_owned_datum())
                        .collect(),
                );
                let state = partitions.entry(partition_key).or_default();
                state.rows.push(BufferedRow {
                    seq,
                    order_key,
                    satisfied,
                    measures: measures_row,
                });
                state.next_seq = state.next_seq.max(seq + 1);
            }
        }

        #[for_await]
        for msg in input {
            match msg? {
                Message::Chunk(chunk) => {
                    // Append-only input: buffer rows; matching happens on watermark.
                    let chunk = chunk.compact_vis();
                    let data_chunk = chunk.data_chunk();

                    let mut define_cols = Vec::with_capacity(define_exprs.len());
                    for e in &define_exprs {
                        define_cols.push(e.eval_infallible(data_chunk).await);
                    }
                    let mut measure_cols = Vec::with_capacity(measures.len());
                    for e in &measures {
                        measure_cols.push(e.eval_infallible(data_chunk).await);
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
                        let measure_datums: Vec<Datum> = measure_cols
                            .iter()
                            .map(|c| c.value_at(idx).to_owned_datum())
                            .collect();

                        let state = partitions.entry(partition_key.clone()).or_default();
                        let seq = state.next_seq;
                        state.next_seq += 1;
                        let buffered = BufferedRow {
                            seq,
                            order_key,
                            satisfied,
                            measures: OwnedRow::new(measure_datums),
                        };
                        state_table.insert(Self::state_row(&partition_key, &buffered));
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
                            let last_row = &state.rows[m.end - 1];
                            let measures_row: Vec<Datum> = if classifier_measure_indices.is_empty() {
                                last_row.measures.iter().map(|d| d.to_owned_datum()).collect()
                            } else {
                                // CLASSIFIER() = the pattern variable bound to the match's last row.
                                let last_label = m.labels.last().cloned();
                                last_row
                                    .measures
                                    .iter()
                                    .enumerate()
                                    .map(|(i, d)| {
                                        if classifier_measure_indices.contains(&i) {
                                            last_label.clone().map(|s| ScalarImpl::Utf8(s.into()))
                                        } else {
                                            d.to_owned_datum()
                                        }
                                    })
                                    .collect()
                            };
                            out_rows.push(
                                partition_key
                                    .clone()
                                    .chain(OwnedRow::new(measures_row))
                                    .into_owned_row(),
                            );
                            cursor = match skip {
                                SkipMode::PastLastRow => m.end,
                                SkipMode::ToNextRow => m.start + 1,
                            };
                        }
                        for consumed in state.rows.drain(0..cursor) {
                            state_table.delete(Self::state_row(partition_key, &consumed));
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

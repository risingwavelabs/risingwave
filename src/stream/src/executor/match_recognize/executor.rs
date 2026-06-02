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

//! Streaming `MATCH_RECOGNIZE` executor (v1).
//!
//! Scope: append-only input, `ONE ROW PER MATCH`, `AFTER MATCH SKIP {PAST LAST ROW | TO NEXT ROW}`,
//! scalar `MEASURES` evaluated on the match's last row. Assumes input arrives in `ORDER BY` order
//! per partition (no re-sort yet). A match is emitted once a following row exists, so the greedy
//! match is known maximal.
//!
//! State: the per-partition live row window (each row's satisfied pattern variables + pre-evaluated
//! MEASURES) is persisted to a state table — written through on arrival, deleted on consumption,
//! and restored on recovery — so match state survives failover. The cursor is not persisted:
//! consumed rows are compacted away after each drain, so the persisted window is exactly the live
//! set and scanning resumes from its start.

use std::collections::{BTreeSet, HashMap};
use std::ops::Bound;

use futures::{StreamExt, pin_mut};
use risingwave_common::array::Op;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::{Datum, ScalarImpl, ToOwnedDatum};
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
    pub state_table: StateTable<S>,
}

pub struct MatchRecognizeExecutor<S: StateStore> {
    ctx: ActorContextRef,
    input: Executor,
    schema: Schema,
    chunk_size: usize,
    partition_key_indices: Vec<usize>,
    measures: Vec<NonStrictExpression>,
    define_symbols: Vec<String>,
    define_exprs: Vec<NonStrictExpression>,
    nfa: Nfa,
    skip: SkipMode,
    state_table: StateTable<S>,
}

/// A buffered input row within one partition.
struct BufferedRow {
    /// Per-partition monotonic id; the state-table key suffix.
    seq: i64,
    /// The pattern variables whose `DEFINE` predicate this row satisfies.
    satisfied: BTreeSet<String>,
    /// Pre-evaluated `MEASURES` values for this row (used when it is a match's last row).
    measures: OwnedRow,
}

/// Per-partition match state: the live (un-consumed) buffered rows and the next seq to assign.
#[derive(Default)]
struct PartitionState {
    rows: Vec<BufferedRow>,
    next_seq: i64,
}

/// Join a satisfied-variable set into the stored varchar form.
fn join_satisfied(satisfied: &BTreeSet<String>) -> String {
    satisfied.iter().cloned().collect::<Vec<_>>().join(",")
}

/// Split the stored varchar form back into a satisfied-variable set.
fn split_satisfied(s: &str) -> BTreeSet<String> {
    s.split(',').filter(|p| !p.is_empty()).map(|p| p.to_owned()).collect()
}

impl<S: StateStore> MatchRecognizeExecutor<S> {
    pub fn new(args: MatchRecognizeExecutorArgs<S>) -> Self {
        Self {
            ctx: args.ctx,
            input: args.input,
            schema: args.schema,
            chunk_size: args.chunk_size,
            partition_key_indices: args.partition_key_indices,
            measures: args.measures,
            define_symbols: args.define_symbols,
            define_exprs: args.define_exprs,
            nfa: args.nfa,
            skip: args.skip,
            state_table: args.state_table,
        }
    }

    /// Build a state-table row: `[ partition cols.. , seq , satisfied , measure cols.. ]`.
    fn state_row(partition_key: &OwnedRow, row: &BufferedRow) -> OwnedRow {
        let mut datums: Vec<Datum> = partition_key.iter().map(|d| d.to_owned_datum()).collect();
        datums.push(Some(ScalarImpl::Int64(row.seq)));
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
            measures,
            define_symbols,
            define_exprs,
            nfa,
            skip,
            mut state_table,
        } = *self;

        let n_part = partition_key_indices.len();
        let n_measure = measures.len();

        let mut input = input.execute();
        let barrier = expect_first_barrier(&mut input).await?;
        let first_epoch = barrier.epoch;
        yield Message::Barrier(barrier);
        state_table.init_epoch(first_epoch).await?;

        // Recovery: rebuild the per-partition live windows from the state table.
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
                let satisfied = row
                    .datum_at(n_part + 1)
                    .map(|d| split_satisfied(d.into_utf8()))
                    .unwrap_or_default();
                let measures_row = OwnedRow::new(
                    (n_part + 2..n_part + 2 + n_measure)
                        .map(|i| row.datum_at(i).to_owned_datum())
                        .collect(),
                );
                let state = partitions.entry(partition_key).or_default();
                state.rows.push(BufferedRow {
                    seq,
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
                    let chunk = chunk.compact_vis();
                    let data_chunk = chunk.data_chunk();

                    // Vectorized evaluation of DEFINE predicates and MEASURES over the chunk.
                    let mut define_cols = Vec::with_capacity(define_exprs.len());
                    for e in &define_exprs {
                        define_cols.push(e.eval_infallible(data_chunk).await);
                    }
                    let mut measure_cols = Vec::with_capacity(measures.len());
                    for e in &measures {
                        measure_cols.push(e.eval_infallible(data_chunk).await);
                    }

                    let mut touched: Vec<OwnedRow> = Vec::new();

                    for (idx, (op, row_ref)) in chunk.rows().enumerate() {
                        // Append-only input: only inserts contribute to matches.
                        if !matches!(op, Op::Insert) {
                            continue;
                        }

                        let partition_key = row_ref.project(&partition_key_indices).to_owned_row();

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
                            satisfied,
                            measures: OwnedRow::new(measure_datums),
                        };
                        state_table.insert(Self::state_row(&partition_key, &buffered));
                        state.rows.push(buffered);
                        if !touched.contains(&partition_key) {
                            touched.push(partition_key);
                        }
                    }

                    // Drain newly-complete matches from each touched partition, then compact away
                    // the consumed prefix (both in memory and in the state table).
                    let mut builder = StreamChunkBuilder::new(chunk_size, schema.data_types());
                    for partition_key in touched {
                        let state = partitions.get_mut(&partition_key).unwrap();
                        let satisfied: Vec<BTreeSet<String>> =
                            state.rows.iter().map(|r| r.satisfied.clone()).collect();

                        let mut cursor = 0usize;
                        for m in nfa.find_matches(&satisfied, skip) {
                            if m.start < cursor {
                                continue;
                            }
                            // Only emit once the greedy match is known maximal: a following row exists.
                            if m.end >= state.rows.len() {
                                break;
                            }
                            let last_row = &state.rows[m.end - 1];
                            let out = partition_key
                                .clone()
                                .chain(last_row.measures.clone())
                                .into_owned_row();
                            if let Some(c) = builder.append_row(Op::Insert, out) {
                                yield Message::Chunk(c);
                            }
                            cursor = match skip {
                                SkipMode::PastLastRow => m.end,
                                SkipMode::ToNextRow => m.start + 1,
                            };
                        }

                        // Compact: rows before `cursor` are consumed and can no longer start a match.
                        for consumed in state.rows.drain(0..cursor) {
                            state_table.delete(Self::state_row(&partition_key, &consumed));
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
                Message::Watermark(_) => {
                    // Watermark-driven finalization / state eviction is a later piece.
                }
            }
        }
    }
}

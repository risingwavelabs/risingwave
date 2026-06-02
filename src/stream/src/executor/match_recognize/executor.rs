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

//! Streaming `MATCH_RECOGNIZE` executor (v1, in-memory).
//!
//! Scope of this first cut (deliberately bounded; see the contribution notes):
//! - append-only input, `ONE ROW PER MATCH`, `AFTER MATCH SKIP PAST LAST ROW`;
//! - **assumes input arrives in `ORDER BY` order** per partition (no re-sort yet);
//! - **scalar** `MEASURES`, evaluated on the match's last row (no aggregates / navigation yet);
//! - **in-memory** per-partition buffers (no checkpoint/recovery yet — that is the next piece);
//! - emits a match only once a following row exists, so the greedy match is known to be maximal.

use std::collections::BTreeSet;
use std::collections::HashMap;

use risingwave_common::array::Op;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::{Datum, ToOwnedDatum};
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
    /// State table for partial-match / buffer persistence. Not yet used (in-memory v1).
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
    /// The pattern variables whose `DEFINE` predicate this row satisfies.
    satisfied: BTreeSet<String>,
    /// Pre-evaluated `MEASURES` values for this row (used when it is a match's last row).
    measures: OwnedRow,
}

/// Per-partition match state: buffered rows and the next row not yet consumed by a match.
#[derive(Default)]
struct PartitionState {
    rows: Vec<BufferedRow>,
    cursor: usize,
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

        let mut input = input.execute();
        let barrier = expect_first_barrier(&mut input).await?;
        let first_epoch = barrier.epoch;
        yield Message::Barrier(barrier);
        state_table.init_epoch(first_epoch).await?;

        let mut partitions: HashMap<OwnedRow, PartitionState> = HashMap::new();

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

                        let partition_key =
                            row_ref.project(&partition_key_indices).to_owned_row();

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
                        state.rows.push(BufferedRow {
                            satisfied,
                            measures: OwnedRow::new(measure_datums),
                        });
                        if !touched.contains(&partition_key) {
                            touched.push(partition_key);
                        }
                    }

                    // Drain newly-complete matches from each touched partition.
                    let mut builder = StreamChunkBuilder::new(chunk_size, schema.data_types());
                    for partition_key in touched {
                        let state = partitions.get_mut(&partition_key).unwrap();
                        let satisfied: Vec<BTreeSet<String>> =
                            state.rows.iter().map(|r| r.satisfied.clone()).collect();

                        for m in nfa.find_matches(&satisfied, skip) {
                            if m.start < state.cursor {
                                continue; // already emitted
                            }
                            // Only emit once the greedy match is known maximal: a row exists after it.
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
                            state.cursor = m.end;
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

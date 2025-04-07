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

use risingwave_common::array::{Op, RowRef};
use risingwave_common::bitmap::BitmapBuilder;
use risingwave_common::row::RowExt;
use risingwave_common::util::iter_util::{ZipEqDebug, ZipEqFast};
use risingwave_expr::expr::NonStrictExpression;

use crate::cache::ManagedLruCache;
use crate::common::metrics::MetricsInfo;
use crate::consistency::consistency_panic;
use crate::executor::prelude::*;

/// An executor that materializes the result of a set of expressions.
/// The expressions are evaluated on `Insert`/`UpdateInsert` rows and the results are stored in a state table.
/// When a `Delete`/`UpdateDelete` row is received, the corresponding result row is popped from the state table
/// without the need to re-evaluate the expressions.
///
/// - Executor output: `input | expression results`, PK is inherited from the input.
/// - State table: `input | expression results`.
/// - State table PK: `state clean column | rest of input pk`.
pub struct MaterializedExprsExecutor<S: StateStore> {
    input: Executor,
    inner: Inner<S>,
}

pub struct MaterializedExprsArgs<S: StateStore> {
    pub actor_ctx: ActorContextRef,
    pub input: Executor,
    pub exprs: Vec<NonStrictExpression>,
    pub state_table: StateTable<S>,
    pub state_clean_col_idx: Option<usize>,
    pub watermark_epoch: AtomicU64Ref,
}

impl<S: StateStore> MaterializedExprsExecutor<S> {
    pub fn new(args: MaterializedExprsArgs<S>) -> Self {
        let state_table_pk_indices = args.state_table.pk_indices().to_vec();
        Self {
            input: args.input,
            inner: Inner {
                actor_ctx: args.actor_ctx.clone(),
                exprs: args.exprs,
                state_table: StateTableWrapper::new(
                    args.state_table,
                    args.actor_ctx.clone(),
                    args.watermark_epoch,
                ),
                state_table_pk_indices,
                state_clean_col_idx: args.state_clean_col_idx,
            },
        }
    }
}

impl<S: StateStore> Execute for MaterializedExprsExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.inner.execute(self.input).boxed()
    }
}

struct StateTableWrapper<S: StateStore> {
    inner: StateTable<S>,
    cache: ManagedLruCache<OwnedRow, OwnedRow>,
}

impl<S: StateStore> StateTableWrapper<S> {
    fn new(
        table: StateTable<S>,
        actor_ctx: ActorContextRef,
        watermark_epoch: AtomicU64Ref,
    ) -> Self {
        let metrics_info = MetricsInfo::new(
            actor_ctx.streaming_metrics.clone(),
            table.table_id(),
            actor_ctx.id,
            "MaterializedExprs",
        );

        Self {
            inner: table,
            cache: ManagedLruCache::unbounded(watermark_epoch, metrics_info),
        }
    }

    fn insert(&mut self, row: impl Row) {
        let owned_row = row.into_owned_row();
        let pk = (&owned_row)
            .project(self.inner.pk_indices())
            .into_owned_row();

        // Store the record and update the cache
        self.inner.insert(&owned_row);
        self.cache.put(pk, owned_row);
    }

    async fn remove_by_pk(&mut self, pk: impl Row) -> StreamExecutorResult<Option<OwnedRow>> {
        // Try to get from cache first
        let pk_owned = pk.into_owned_row();
        if let Some(row) = self.cache.get(&pk_owned) {
            let cloned_row = row.clone();
            self.inner.delete(row);
            return Ok(Some(cloned_row));
        }

        // Cache miss, get from state table
        let row = self.inner.get_row(pk_owned).await?;
        if let Some(ref row) = row {
            self.inner.delete(row);
        }
        Ok(row)
    }
}

struct Inner<S: StateStore> {
    actor_ctx: ActorContextRef,
    /// Expressions to evaluate.
    exprs: Vec<NonStrictExpression>,
    /// State table to store the results.
    state_table: StateTableWrapper<S>,
    /// State table PK indices.
    state_table_pk_indices: Vec<usize>,
    /// Index of the column used for state cleaning.
    state_clean_col_idx: Option<usize>,
}

impl<S: StateStore> Inner<S> {
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute(mut self, input: Executor) {
        let mut input = input.execute();
        let first_barrier = expect_first_barrier(&mut input).await?;
        let first_epoch = first_barrier.epoch;
        yield Message::Barrier(first_barrier);
        self.state_table.inner.init_epoch(first_epoch).await?;

        #[for_await]
        for msg in input {
            let msg = msg?;
            match msg {
                Message::Chunk(input_chunk) => {
                    let mut eval_visibility = BitmapBuilder::from(input_chunk.visibility().clone());
                    for (i, op) in input_chunk.ops().iter().enumerate() {
                        // hide deletions from expression evaluation
                        match op {
                            Op::Delete | Op::UpdateDelete => eval_visibility.set(i, false),
                            _ => {}
                        }
                    }
                    let eval_chunk = input_chunk
                        .data_chunk()
                        .with_visibility(eval_visibility.finish());

                    let mut eval_result_arrs = Vec::with_capacity(self.exprs.len());
                    for expr in &self.exprs {
                        // for deletions, the evaluation result is NULL
                        eval_result_arrs.push(expr.eval_infallible(&eval_chunk).await);
                    }

                    let mut eval_result_builders = eval_result_arrs
                        .iter()
                        .map(|arr| arr.create_builder(input_chunk.capacity()))
                        .collect::<Vec<_>>();
                    // now we need to replace the NULLs with the old evaluation results
                    for (row_idx, row_op) in input_chunk.rows_with_holes().enumerate() {
                        let Some((op, row)) = row_op else {
                            // it's invisible in the input
                            for builder in &mut eval_result_builders {
                                builder.append_null();
                            }
                            continue;
                        };

                        match op {
                            Op::Insert | Op::UpdateInsert => {
                                // for insertions, append the evaluation results
                                for (arr, builder) in eval_result_arrs
                                    .iter()
                                    .zip_eq_fast(&mut eval_result_builders)
                                {
                                    let datum_ref = unsafe { arr.value_at_unchecked(row_idx) };
                                    builder.append(datum_ref);
                                }

                                self.state_table.insert(
                                    row.chain(RowRef::with_columns(&eval_result_arrs, row_idx)),
                                );
                            }
                            Op::Delete | Op::UpdateDelete => {
                                // for deletions, append the old evaluation results
                                let pk = row.project(&self.state_table_pk_indices);
                                let old_row = self.state_table.remove_by_pk(pk).await?;
                                if let Some(old_row) = old_row {
                                    for (datum_ref, builder) in old_row
                                        .iter()
                                        .skip(row.len())
                                        .zip_eq_debug(&mut eval_result_builders)
                                    {
                                        builder.append(datum_ref);
                                    }
                                } else {
                                    consistency_panic!("delete non-existing row");
                                    for builder in &mut eval_result_builders {
                                        builder.append_null();
                                    }
                                }
                            }
                        }
                    }

                    let (ops, mut columns, vis) = input_chunk.into_inner();
                    columns.extend(
                        eval_result_builders
                            .into_iter()
                            .map(|builder| builder.finish().into()),
                    );
                    yield Message::Chunk(StreamChunk::with_visibility(ops, columns, vis));
                }
                Message::Barrier(barrier) => {
                    let post_commit = self.state_table.inner.commit(barrier.epoch).await?;
                    let update_vnode_bitmap = barrier.as_update_vnode_bitmap(self.actor_ctx.id);
                    yield Message::Barrier(barrier);

                    // evict LRU cache
                    self.state_table.cache.evict();

                    if let Some((_, cache_may_stale)) =
                        post_commit.post_yield_barrier(update_vnode_bitmap).await?
                    {
                        if cache_may_stale {
                            self.state_table.cache.clear();
                        }
                    }
                }
                Message::Watermark(watermark) => {
                    if let Some(state_clean_col_idx) = self.state_clean_col_idx
                        && state_clean_col_idx == watermark.col_idx
                    {
                        self.state_table
                            .inner
                            .update_watermark(watermark.val.clone());
                    }
                    yield Message::Watermark(watermark);
                }
            }
        }
    }
}

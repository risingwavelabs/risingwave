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
    cache: ManagedLruCache<OwnedRow, Option<OwnedRow>>,
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
        self.cache.put(pk, Some(owned_row));
    }

    async fn remove_by_pk(&mut self, pk: impl Row) -> StreamExecutorResult<Option<OwnedRow>> {
        // Try to get from cache first
        let pk_owned = pk.into_owned_row();
        if let Some(row) = self.cache.get(&pk_owned) {
            // Cache hit, delete from cache and state table
            if let Some(row) = row {
                let cloned_row = row.clone();
                self.inner.delete(row);
                // Mark as deleted in cache
                self.cache.put(pk_owned, None);
                Ok(Some(cloned_row))
            } else {
                Ok(None)
            }
        } else {
            // Cache miss, get from state table
            let row = self.inner.get_row(pk_owned).await?;
            if let Some(ref row) = row {
                self.inner.delete(row);
            }
            Ok(row)
        }
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
                        && cache_may_stale
                    {
                        self.state_table.cache.clear();
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    use multimap::MultiMap;
    use risingwave_common::array::{ArrayRef, DataChunk};
    use risingwave_common::catalog::Field;
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::{
        Datum, F64, JsonbVal, ListValue, MapType, MapValue, ScalarImpl, StructType, StructValue,
    };
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_expr::Result;
    use risingwave_expr::expr::{
        AsyncExpression, AsyncExpressionBoxExt, ExpressionInfo, LogReport,
    };
    use risingwave_storage::memory::MemoryStateStore;
    use serde_json::json;
    use tokio::sync::Semaphore;

    use super::*;
    use crate::executor::project::ProjectExecutor;
    use crate::executor::test_utils::top_n_executor::create_in_memory_state_table_from_state_store;
    use crate::executor::test_utils::{MessageSender, MockSource, StreamExecutorTestExt};

    #[derive(Debug, Clone, Default)]
    struct RemoteExpression {
        calls: Arc<AtomicU64>,
        fail: Arc<AtomicBool>,
        gate: Option<Arc<Semaphore>>,
        typed: bool,
    }

    impl RemoteExpression {
        fn result(&self, version: u64) -> ScalarImpl {
            if !self.typed {
                return JsonbVal::from(json!({"version": version})).into();
            }
            StructValue::new(vec![
                Some((version as i64).into()),
                Some(
                    MapValue::try_from_kv(
                        ListValue::from_iter(["billing", "technical"]),
                        ListValue::from_iter([F64::from(0.8), F64::from(0.2)]),
                    )
                    .unwrap()
                    .into(),
                ),
                Some(ListValue::from_iter([F64::from(0.25), F64::from(0.75)]).into()),
            ])
            .into()
        }
    }

    impl ExpressionInfo for RemoteExpression {
        fn return_type(&self) -> DataType {
            if self.typed {
                StructType::new([
                    ("version", DataType::Int64),
                    (
                        "choices",
                        MapType::from_kv(DataType::Varchar, DataType::Float64).into(),
                    ),
                    ("levels", DataType::list(DataType::Float64)),
                ])
                .into()
            } else {
                DataType::Jsonb
            }
        }
    }

    impl AsyncExpression for RemoteExpression {
        async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
            let mut builder = self.return_type().create_array_builder(input.capacity());
            for visible in input.visibility().iter() {
                if !visible {
                    builder.append(None::<ScalarImpl>);
                    continue;
                }
                let version = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
                if let Some(gate) = &self.gate {
                    let _permit = gate.acquire().await.unwrap();
                }
                while self.fail.load(Ordering::SeqCst) {
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
                builder.append(Some(self.result(version)));
            }
            Ok(builder.finish().into_ref())
        }

        async fn eval_row(&self, _: &OwnedRow) -> Result<Datum> {
            unreachable!("these executors evaluate chunks")
        }
    }

    async fn executor(
        store: MemoryStateStore,
        expression: RemoteExpression,
        materialized: bool,
    ) -> (MessageSender, BoxedMessageStream) {
        let (tx, source) = MockSource::channel();
        let input = source.into_executor(
            Schema::new(vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
            ]),
            vec![0],
        );
        // Exercise the non-strict wrapper used in production, not just the mock's error return.
        let output_type = expression.return_type();
        let expr = NonStrictExpression::new_topmost(expression.boxed(), LogReport);
        let actor_ctx = ActorContext::for_test(123);
        if !materialized {
            return (
                tx,
                ProjectExecutor::new(actor_ctx, input, vec![expr], MultiMap::new(), vec![], false)
                    .boxed()
                    .execute(),
            );
        }
        let state_table = create_in_memory_state_table_from_state_store(
            &[DataType::Int64, DataType::Int64, output_type],
            &[OrderType::ascending()],
            &[0],
            store,
        )
        .await;
        let stream = MaterializedExprsExecutor::new(MaterializedExprsArgs {
            actor_ctx,
            input,
            exprs: vec![expr],
            state_table,
            state_clean_col_idx: None,
            watermark_epoch: Arc::new(AtomicU64::new(0)),
        })
        .boxed()
        .execute();
        (tx, stream)
    }

    #[tokio::test]
    async fn restores_results_for_update_and_delete_after_recovery() {
        for typed in [false, true] {
            let store = MemoryStateStore::new();
            let expr = RemoteExpression {
                typed,
                ..Default::default()
            };
            let (mut tx, mut stream) = executor(store.clone(), expr.clone(), true).await;
            tx.push_barrier(test_epoch(1), false);
            stream.expect_barrier().await;
            tx.push_chunk(StreamChunk::from_pretty("I I\n+ 1 10"));
            let chunk = stream.expect_chunk().await;
            assert_eq!(chunk.columns()[2].datum_at(0), Some(expr.result(1)));
            tx.push_barrier(test_epoch(2), false);
            stream.expect_barrier().await;
            drop(stream);

            // Cold UPDATE reads the saved value, then evaluates only the new row.
            let (mut tx, mut recovered) = executor(store.clone(), expr.clone(), true).await;
            tx.push_barrier(test_epoch(2), false);
            recovered.expect_barrier().await;
            tx.push_chunk(StreamChunk::from_pretty("I I\nU- 1 10\nU+ 1 20"));
            let chunk = recovered.expect_chunk().await;
            assert_eq!(chunk.ops(), &[Op::UpdateDelete, Op::UpdateInsert]);
            assert_eq!(chunk.columns()[2].datum_at(0), Some(expr.result(1)));
            assert_eq!(chunk.columns()[2].datum_at(1), Some(expr.result(2)));
            assert_eq!(expr.calls.load(Ordering::SeqCst), 2);
            tx.push_barrier(test_epoch(3), false);
            recovered.expect_barrier().await;
            drop(recovered);

            // A second cold recovery must delete the updated value without the service.
            expr.fail.store(true, Ordering::SeqCst);
            let (mut tx, mut recovered) = executor(store, expr.clone(), true).await;
            tx.push_barrier(test_epoch(3), false);
            recovered.expect_barrier().await;
            tx.push_chunk(StreamChunk::from_pretty("I I\n- 1 20"));
            let chunk = recovered.expect_chunk().await;
            assert_eq!(chunk.ops(), &[Op::Delete]);
            assert_eq!(chunk.columns()[2].datum_at(0), Some(expr.result(2)));
            assert_eq!(expr.calls.load(Ordering::SeqCst), 2);
        }
    }

    #[tokio::test]
    async fn control_messages_wait_for_pending_results() {
        for materialized in [false, true] {
            let gate = Arc::new(Semaphore::new(0));
            let expr = RemoteExpression {
                gate: Some(gate.clone()),
                ..Default::default()
            };
            let (mut tx, mut stream) =
                executor(MemoryStateStore::new(), expr.clone(), materialized).await;
            tx.push_barrier(test_epoch(1), false);
            stream.expect_barrier().await;
            tx.push_chunk(StreamChunk::from_pretty("I I\n+ 1 10"));
            tx.push_int64_watermark(0, 1);
            tx.push_barrier(test_epoch(2), false);
            stream.next_unwrap_pending();
            assert_eq!(expr.calls.load(Ordering::SeqCst), 1);
            gate.add_permits(1);
            stream.expect_chunk().await;
            // A plain Project has no watermark derivation in this fixture; MaterializedExprs
            // passes its input watermark through after producing the result.
            if materialized {
                assert!(
                    stream
                        .next()
                        .await
                        .unwrap()
                        .unwrap()
                        .as_watermark()
                        .is_some()
                );
            }
            stream.expect_barrier().await;
        }
    }

    #[tokio::test]
    async fn retrying_expression_waits_before_emitting_results_or_checkpoints() {
        for materialized in [false, true] {
            let expr = RemoteExpression::default();
            expr.fail.store(true, Ordering::SeqCst);
            let (mut tx, mut stream) =
                executor(MemoryStateStore::new(), expr.clone(), materialized).await;
            tx.push_barrier(test_epoch(1), false);
            stream.expect_barrier().await;
            tx.push_chunk(StreamChunk::from_pretty("I I\n+ 1 10"));
            tx.push_barrier(test_epoch(2), false);
            stream.next_unwrap_pending();
            expr.fail.store(false, Ordering::SeqCst);
            let chunk = stream.expect_chunk().await;
            let column = if materialized { 2 } else { 0 };
            assert_eq!(chunk.columns()[column].datum_at(0), Some(expr.result(1)));
            stream.expect_barrier().await;
        }
    }
}

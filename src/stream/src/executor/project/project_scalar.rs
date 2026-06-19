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

use std::collections::VecDeque;
use std::future::Future;
use std::time::Instant;

use futures::future::{Either, select};
use futures::pin_mut;
use futures::stream::FuturesOrdered;
use multimap::MultiMap;
use risingwave_common::row::RowExt;
use risingwave_common::types::ToOwnedDatum;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::expr::NonStrictExpression;

use crate::executor::prelude::*;
use crate::task::ActorId;

type ProjectChunkFuture = impl Future<Output = StreamExecutorResult<ProjectedChunk>> + Send;
type PendingProjectChunks = FuturesOrdered<ProjectChunkFuture>;

struct ProjectedChunk {
    chunk_seq: u64,
    finished_at: Instant,
    chunk: StreamChunk,
}

fn project_exprs_contain_openai_embedding(exprs: &[NonStrictExpression]) -> bool {
    exprs.iter().any(|expr| {
        let expr_debug = format!("{expr:?}");
        expr_debug.contains("OpenAiEmbedding")
    })
}

/// `ProjectExecutor` project data with the `expr`. The `expr` takes a chunk of data,
/// and returns a new data chunk. And then, `ProjectExecutor` will insert, delete
/// or update element into next operator according to the result of the expression.
pub struct ProjectExecutor {
    input: Executor,
    inner: Inner,
}

struct Inner {
    actor_id: ActorId,

    /// Expressions of the current projection.
    exprs: Arc<Vec<NonStrictExpression>>,
    /// All the watermark derivations, (`input_column_index`, `output_column_index`). And the
    /// derivation expression is the project's expression itself.
    watermark_derivations: MultiMap<usize, usize>,
    /// Indices of nondecreasing expressions in the expression list.
    nondecreasing_expr_indices: Vec<usize>,
    /// Last seen values of nondecreasing expressions, buffered to periodically produce watermarks.
    last_nondec_expr_values: Vec<Option<ScalarImpl>>,

    /// Whether there are likely no-op updates in the output chunks, so that eliminating them with
    /// `StreamChunk::eliminate_adjacent_noop_update` could be beneficial.
    eliminate_noop_updates: bool,

    /// Maximum number of chunks with in-flight projection evaluation.
    project_expr_concurrency: usize,

    /// Whether to log detailed chunk-level projection execution for `OpenAI` embedding
    /// projections.
    enable_project_openai_embedding_detail_log: bool,
}

impl ProjectExecutor {
    pub fn new(
        ctx: ActorContextRef,
        input: Executor,
        exprs: Vec<NonStrictExpression>,
        watermark_derivations: MultiMap<usize, usize>,
        nondecreasing_expr_indices: Vec<usize>,
        noop_update_hint: bool,
    ) -> Self {
        let n_nondecreasing_exprs = nondecreasing_expr_indices.len();
        let eliminate_noop_updates =
            noop_update_hint || ctx.config.developer.aggressive_noop_update_elimination;
        let project_expr_concurrency = match ctx.config.developer.project_expr_concurrency {
            0 => usize::MAX,
            concurrency => concurrency,
        };
        let enable_project_openai_embedding_detail_log = ctx
            .config
            .developer
            .enable_project_openai_embedding_detail_log
            && project_exprs_contain_openai_embedding(&exprs);
        if enable_project_openai_embedding_detail_log {
            tracing::info!(
                actor_id = ?ctx.id,
                project_expr_concurrency,
                expr_count = exprs.len(),
                "openai embedding project detail logging enabled"
            );
        }
        let actor_id = ctx.id;
        Self {
            input,
            inner: Inner {
                actor_id,
                exprs: Arc::new(exprs),
                watermark_derivations,
                nondecreasing_expr_indices,
                last_nondec_expr_values: vec![None; n_nondecreasing_exprs],
                eliminate_noop_updates,
                project_expr_concurrency,
                enable_project_openai_embedding_detail_log,
            },
        }
    }
}

impl Debug for ProjectExecutor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProjectExecutor")
            .field("exprs", &self.inner.exprs)
            .finish()
    }
}

impl Execute for ProjectExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.inner.execute(self.input).boxed()
    }
}

pub async fn apply_project_exprs(
    exprs: &[NonStrictExpression],
    chunk: StreamChunk,
) -> StreamExecutorResult<StreamChunk> {
    let (data_chunk, ops) = chunk.into_parts();
    let mut projected_columns = Vec::new();

    for expr in exprs {
        let evaluated_expr = expr.eval_infallible(&data_chunk).await;
        projected_columns.push(evaluated_expr);
    }
    let (_, vis) = data_chunk.into_parts();

    let new_chunk = StreamChunk::with_visibility(ops, projected_columns, vis);

    Ok(new_chunk)
}

impl Inner {
    #[define_opaque(ProjectChunkFuture)]
    fn map_filter_chunk(
        exprs: Arc<Vec<NonStrictExpression>>,
        eliminate_noop_updates: bool,
        chunk: StreamChunk,
        actor_id: ActorId,
        enable_project_openai_embedding_detail_log: bool,
        chunk_seq: u64,
    ) -> ProjectChunkFuture {
        let input_row_count = chunk.cardinality();
        async move {
            if enable_project_openai_embedding_detail_log {
                tracing::info!(
                    actor_id = ?actor_id,
                    chunk_seq,
                    input_row_count,
                    "openai embedding project chunk mapping started"
                );
            }
            let start = Instant::now();
            let mut new_chunk = apply_project_exprs(&exprs, chunk).await?;
            if eliminate_noop_updates {
                new_chunk = new_chunk.eliminate_adjacent_noop_update();
            }
            let output_row_count = new_chunk.cardinality();
            if enable_project_openai_embedding_detail_log {
                tracing::info!(
                    actor_id = ?actor_id,
                    chunk_seq,
                    elapsed_ms = start.elapsed().as_millis(),
                    output_row_count,
                    "openai embedding project chunk mapping finished"
                );
            }
            Ok(ProjectedChunk {
                chunk_seq,
                finished_at: Instant::now(),
                chunk: new_chunk,
            })
        }
    }

    async fn handle_watermark(
        exprs: &[NonStrictExpression],
        watermark_derivations: &MultiMap<usize, usize>,
        watermark: Watermark,
    ) -> StreamExecutorResult<Vec<Watermark>> {
        let out_col_indices = match watermark_derivations.get_vec(&watermark.col_idx) {
            Some(v) => v,
            None => return Ok(vec![]),
        };
        let mut ret = vec![];
        for out_col_idx in out_col_indices {
            let out_col_idx = *out_col_idx;
            let derived_watermark = watermark
                .clone()
                .transform_with_expr(&exprs[out_col_idx], out_col_idx)
                .await;
            if let Some(derived_watermark) = derived_watermark {
                ret.push(derived_watermark);
            } else {
                warn!(
                    "a NULL watermark is derived with the expression {}!",
                    out_col_idx
                );
            }
        }
        Ok(ret)
    }

    fn update_last_nondec_expr_values(
        nondecreasing_expr_indices: &[usize],
        last_nondec_expr_values: &mut [Option<ScalarImpl>],
        new_chunk: &StreamChunk,
    ) {
        if !nondecreasing_expr_indices.is_empty()
            && let Some((_, first_visible_row)) = new_chunk.rows().next()
        {
            // it's ok to use the first row here, just one chunk delay
            first_visible_row
                .project(nondecreasing_expr_indices)
                .iter()
                .enumerate()
                .for_each(|(idx, value)| {
                    last_nondec_expr_values[idx] = Some(
                        value
                            .to_owned_datum()
                            .expect("non-decreasing expression should never be NULL"),
                    );
                });
        }
    }

    async fn next_projected_chunk(
        pending_project_chunks: &mut PendingProjectChunks,
        pending_chunk_seqs: &mut VecDeque<u64>,
        nondecreasing_expr_indices: &[usize],
        last_nondec_expr_values: &mut [Option<ScalarImpl>],
        actor_id: ActorId,
        enable_project_openai_embedding_detail_log: bool,
    ) -> StreamExecutorResult<StreamChunk> {
        let ProjectedChunk {
            chunk_seq,
            finished_at,
            chunk,
        } = pending_project_chunks
            .next()
            .await
            .expect("pending project chunks should not be empty")?;
        let expected_chunk_seq = pending_chunk_seqs
            .pop_front()
            .expect("pending chunk seqs should not be empty");
        debug_assert_eq!(chunk_seq, expected_chunk_seq);
        let output_row_count = chunk.cardinality();
        if enable_project_openai_embedding_detail_log {
            tracing::info!(
                actor_id = ?actor_id,
                chunk_seq,
                pending_project_chunks = pending_project_chunks.len(),
                pending_chunk_seqs = ?pending_chunk_seqs,
                finished_to_dequeue_ms = finished_at.elapsed().as_millis(),
                output_row_count,
                "openai embedding project chunk mapping dequeued"
            );
        }
        Self::update_last_nondec_expr_values(
            nondecreasing_expr_indices,
            last_nondec_expr_values,
            &chunk,
        );
        Ok(chunk)
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute(self, input: Executor) {
        let Inner {
            actor_id,
            exprs,
            watermark_derivations,
            nondecreasing_expr_indices,
            mut last_nondec_expr_values,
            eliminate_noop_updates,
            project_expr_concurrency,
            enable_project_openai_embedding_detail_log,
        } = self;

        let mut input = input.execute();
        let first_barrier = expect_first_barrier(&mut input).await?;
        let mut is_paused = first_barrier.is_pause_on_startup();
        yield Message::Barrier(first_barrier);

        let mut pending_project_chunks = PendingProjectChunks::new();
        let mut pending_chunk_seqs = VecDeque::new();
        let mut next_chunk_seq = 0;

        loop {
            if pending_project_chunks.len() >= project_expr_concurrency {
                if enable_project_openai_embedding_detail_log {
                    tracing::info!(
                        actor_id = ?actor_id,
                        pending_project_chunks = pending_project_chunks.len(),
                        pending_chunk_seqs = ?pending_chunk_seqs,
                        "openai embedding project concurrency limit reached"
                    );
                }
                let new_chunk = Self::next_projected_chunk(
                    &mut pending_project_chunks,
                    &mut pending_chunk_seqs,
                    &nondecreasing_expr_indices,
                    &mut last_nondec_expr_values,
                    actor_id,
                    enable_project_openai_embedding_detail_log,
                )
                .await?;
                yield Message::Chunk(new_chunk);
                continue;
            }

            let msg = if pending_project_chunks.is_empty() {
                input.next().await.ok_or_else(|| {
                    StreamExecutorError::channel_closed("upstream executor closed unexpectedly")
                })??
            } else {
                let next_projected_chunk = Self::next_projected_chunk(
                    &mut pending_project_chunks,
                    &mut pending_chunk_seqs,
                    &nondecreasing_expr_indices,
                    &mut last_nondec_expr_values,
                    actor_id,
                    enable_project_openai_embedding_detail_log,
                );
                let next_input_msg = async {
                    input.next().await.ok_or_else(|| {
                        StreamExecutorError::channel_closed("upstream executor closed unexpectedly")
                    })?
                };

                pin_mut!(next_projected_chunk);
                pin_mut!(next_input_msg);

                match select(next_projected_chunk, next_input_msg).await {
                    Either::Left((new_chunk, _)) => {
                        yield Message::Chunk(new_chunk?);
                        continue;
                    }
                    Either::Right((msg, _)) => msg?,
                }
            };

            match msg {
                Message::Watermark(w) => {
                    if enable_project_openai_embedding_detail_log {
                        tracing::info!(
                            actor_id = ?actor_id,
                            pending_project_chunks = pending_project_chunks.len(),
                            pending_chunk_seqs = ?pending_chunk_seqs,
                            watermark_col_idx = w.col_idx,
                            "openai embedding project draining chunks before watermark"
                        );
                    }
                    while !pending_project_chunks.is_empty() {
                        let new_chunk = Self::next_projected_chunk(
                            &mut pending_project_chunks,
                            &mut pending_chunk_seqs,
                            &nondecreasing_expr_indices,
                            &mut last_nondec_expr_values,
                            actor_id,
                            enable_project_openai_embedding_detail_log,
                        )
                        .await?;
                        yield Message::Chunk(new_chunk);
                    }

                    let watermarks =
                        Self::handle_watermark(&exprs, &watermark_derivations, w).await?;
                    for watermark in watermarks {
                        yield Message::Watermark(watermark)
                    }
                }
                Message::Chunk(chunk) => {
                    let chunk_seq = next_chunk_seq;
                    next_chunk_seq += 1;
                    if enable_project_openai_embedding_detail_log {
                        let input_row_count = chunk.cardinality();
                        tracing::info!(
                            actor_id = ?actor_id,
                            chunk_seq,
                            pending_project_chunks_before_enqueue = pending_project_chunks.len(),
                            pending_chunk_seqs = ?pending_chunk_seqs,
                            input_row_count,
                            "openai embedding project chunk mapping enqueued"
                        );
                    }
                    pending_project_chunks.push_back(Self::map_filter_chunk(
                        exprs.clone(),
                        eliminate_noop_updates,
                        chunk,
                        actor_id,
                        enable_project_openai_embedding_detail_log,
                        chunk_seq,
                    ));
                    pending_chunk_seqs.push_back(chunk_seq);
                    if enable_project_openai_embedding_detail_log {
                        tracing::info!(
                            actor_id = ?actor_id,
                            chunk_seq,
                            pending_project_chunks_after_enqueue = pending_project_chunks.len(),
                            pending_chunk_seqs = ?pending_chunk_seqs,
                            "openai embedding project pending chunks updated"
                        );
                    }
                }
                Message::Barrier(barrier) => {
                    if enable_project_openai_embedding_detail_log {
                        tracing::info!(
                            actor_id = ?actor_id,
                            pending_project_chunks = pending_project_chunks.len(),
                            pending_chunk_seqs = ?pending_chunk_seqs,
                            barrier_epoch = barrier.epoch.curr,
                            is_paused,
                            "openai embedding project draining chunks before barrier"
                        );
                    }
                    while !pending_project_chunks.is_empty() {
                        let new_chunk = Self::next_projected_chunk(
                            &mut pending_project_chunks,
                            &mut pending_chunk_seqs,
                            &nondecreasing_expr_indices,
                            &mut last_nondec_expr_values,
                            actor_id,
                            enable_project_openai_embedding_detail_log,
                        )
                        .await?;
                        yield Message::Chunk(new_chunk);
                    }

                    if !is_paused {
                        for (&expr_idx, value) in nondecreasing_expr_indices
                            .iter()
                            .zip_eq_fast(&mut last_nondec_expr_values)
                        {
                            if let Some(value) = std::mem::take(value) {
                                yield Message::Watermark(Watermark::new(
                                    expr_idx,
                                    exprs[expr_idx].return_type(),
                                    value,
                                ))
                            }
                        }
                    }

                    if let Some(mutation) = barrier.mutation.as_deref() {
                        match mutation {
                            Mutation::Pause => {
                                is_paused = true;
                            }
                            Mutation::Resume => {
                                is_paused = false;
                            }
                            _ => (),
                        }
                    }

                    yield Message::Barrier(barrier);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{self, AtomicBool, AtomicI64, AtomicUsize};
    use std::time::Duration;

    use risingwave_common::array::DataChunk;
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::catalog::Field;
    use risingwave_common::config::StreamingConfig;
    use risingwave_common::types::DefaultOrd;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_expr::expr::{self, Expression, ValueImpl};
    use tokio::sync::Notify;
    use tokio::time::timeout;

    use super::*;
    use crate::executor::test_utils::expr::build_from_pretty;
    use crate::executor::test_utils::{MockSource, StreamExecutorTestExt};

    fn actor_context_with_project_expr_concurrency(concurrency: usize) -> ActorContextRef {
        let mut config = StreamingConfig::default();
        config.developer.project_expr_concurrency = concurrency;
        let mut ctx = ActorContext::for_test(123);
        Arc::get_mut(&mut ctx)
            .expect("test actor context should not be shared")
            .config = Arc::new(config);
        ctx
    }

    #[tokio::test]
    async fn test_projection() {
        let chunk1 = StreamChunk::from_pretty(
            " I I
            + 1 4
            + 2 5
            + 3 6",
        );
        let chunk2 = StreamChunk::from_pretty(
            " I I
            + 7 8
            - 3 6",
        );
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
            ],
        };
        let stream_key = vec![0];
        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(schema, stream_key);

        let test_expr = build_from_pretty("(add:int8 $0:int8 $1:int8)");

        let proj = ProjectExecutor::new(
            ActorContext::for_test(123),
            source,
            vec![test_expr],
            MultiMap::new(),
            vec![],
            false,
        );
        let mut proj = proj.boxed().execute();

        tx.push_barrier(test_epoch(1), false);
        let barrier = proj.next().await.unwrap().unwrap();
        barrier.as_barrier().unwrap();

        tx.push_chunk(chunk1);
        tx.push_chunk(chunk2);

        let msg = proj.next().await.unwrap().unwrap();
        assert_eq!(
            *msg.as_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I
                + 5
                + 7
                + 9"
            )
        );

        let msg = proj.next().await.unwrap().unwrap();
        assert_eq!(
            *msg.as_chunk().unwrap(),
            StreamChunk::from_pretty(
                "  I
                + 15
                -  9"
            )
        );

        tx.push_barrier(test_epoch(2), true);
        assert!(proj.next().await.unwrap().unwrap().is_stop());
    }

    #[derive(Debug)]
    struct BlockingProjectExpr {
        started_count: Arc<AtomicUsize>,
        second_started: Arc<AtomicBool>,
        second_started_notify: Arc<Notify>,
        release_first: Arc<AtomicBool>,
        release_first_notify: Arc<Notify>,
    }

    #[async_trait::async_trait]
    impl Expression for BlockingProjectExpr {
        fn return_type(&self) -> DataType {
            DataType::Int64
        }

        async fn eval_v2(&self, input: &DataChunk) -> expr::Result<ValueImpl> {
            let call_idx = self.started_count.fetch_add(1, atomic::Ordering::SeqCst);
            if call_idx == 0 {
                loop {
                    let notified = self.second_started_notify.notified();
                    if self.second_started.load(atomic::Ordering::SeqCst) {
                        break;
                    }
                    notified.await;
                }
                loop {
                    let notified = self.release_first_notify.notified();
                    if self.release_first.load(atomic::Ordering::SeqCst) {
                        break;
                    }
                    notified.await;
                }
            } else if call_idx == 1 {
                self.second_started.store(true, atomic::Ordering::SeqCst);
                self.second_started_notify.notify_waiters();
            }

            Ok(ValueImpl::Scalar {
                value: Some((call_idx as i64).into()),
                capacity: input.capacity(),
            })
        }

        async fn eval_row(&self, _input: &OwnedRow) -> expr::Result<Datum> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn test_projection_evaluates_chunks_concurrently_before_barrier() {
        let schema = Schema {
            fields: vec![Field::unnamed(DataType::Int64)],
        };
        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(schema, StreamKey::new());

        let started_count = Arc::new(AtomicUsize::new(0));
        let second_started = Arc::new(AtomicBool::new(false));
        let second_started_notify = Arc::new(Notify::new());
        let release_first = Arc::new(AtomicBool::new(false));
        let release_first_notify = Arc::new(Notify::new());

        let test_expr = NonStrictExpression::for_test(BlockingProjectExpr {
            started_count,
            second_started: second_started.clone(),
            second_started_notify: second_started_notify.clone(),
            release_first: release_first.clone(),
            release_first_notify: release_first_notify.clone(),
        });

        let proj = ProjectExecutor::new(
            actor_context_with_project_expr_concurrency(2),
            source,
            vec![test_expr],
            MultiMap::new(),
            vec![],
            false,
        );
        let mut proj = proj.boxed().execute();

        tx.push_barrier(test_epoch(1), false);
        proj.expect_barrier().await;

        tx.push_chunk(StreamChunk::from_pretty(
            " I
            + 1",
        ));
        tx.push_chunk(StreamChunk::from_pretty(
            " I
            + 2",
        ));
        tx.push_barrier(test_epoch(2), true);

        let next_msg = proj.next();
        pin_mut!(next_msg);
        timeout(Duration::from_secs(5), async {
            loop {
                let notified = second_started_notify.notified();
                if second_started.load(atomic::Ordering::SeqCst) {
                    break;
                }
                tokio::select! {
                    _ = notified => {}
                    msg = &mut next_msg => {
                        panic!("project executor emitted before the second chunk started: {msg:?}");
                    }
                }
            }
        })
        .await
        .expect("second chunk expression did not start");

        release_first.store(true, atomic::Ordering::SeqCst);
        release_first_notify.notify_waiters();

        let msg = next_msg.await.unwrap().unwrap();
        assert_eq!(
            *msg.as_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I
                + 0"
            )
        );

        let msg = proj.next().await.unwrap().unwrap();
        assert_eq!(
            *msg.as_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I
                + 1"
            )
        );

        assert!(proj.next().await.unwrap().unwrap().is_stop());
    }

    static DUMMY_COUNTER: AtomicI64 = AtomicI64::new(0);

    #[derive(Debug)]
    struct DummyNondecreasingExpr;

    #[async_trait::async_trait]
    impl Expression for DummyNondecreasingExpr {
        fn return_type(&self) -> DataType {
            DataType::Int64
        }

        async fn eval_v2(&self, input: &DataChunk) -> expr::Result<ValueImpl> {
            let value = DUMMY_COUNTER.fetch_add(1, atomic::Ordering::SeqCst);
            Ok(ValueImpl::Scalar {
                value: Some(value.into()),
                capacity: input.capacity(),
            })
        }

        async fn eval_row(&self, _input: &OwnedRow) -> expr::Result<Datum> {
            let value = DUMMY_COUNTER.fetch_add(1, atomic::Ordering::SeqCst);
            Ok(Some(value.into()))
        }
    }

    #[tokio::test]
    async fn test_watermark_projection() {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
            ],
        };
        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(schema, StreamKey::new());

        let a_expr = build_from_pretty("(add:int8 $0:int8 1:int8)");
        let b_expr = build_from_pretty("(subtract:int8 $0:int8 1:int8)");
        let c_expr = NonStrictExpression::for_test(DummyNondecreasingExpr);

        let proj = ProjectExecutor::new(
            ActorContext::for_test(123),
            source,
            vec![a_expr, b_expr, c_expr],
            MultiMap::from_iter(vec![(0, 0), (0, 1)].into_iter()),
            vec![2],
            false,
        );
        let mut proj = proj.boxed().execute();

        tx.push_barrier(test_epoch(1), false);
        tx.push_int64_watermark(0, 100);

        proj.expect_barrier().await;
        let w1 = proj.expect_watermark().await;
        let w2 = proj.expect_watermark().await;
        let (w1, w2) = if w1.col_idx < w2.col_idx {
            (w1, w2)
        } else {
            (w2, w1)
        };

        assert_eq!(
            w1,
            Watermark {
                col_idx: 0,
                data_type: DataType::Int64,
                val: ScalarImpl::Int64(101)
            }
        );
        assert_eq!(
            w2,
            Watermark {
                col_idx: 1,
                data_type: DataType::Int64,
                val: ScalarImpl::Int64(99)
            }
        );

        // just push some random chunks
        tx.push_chunk(StreamChunk::from_pretty(
            "   I I
            + 120 4
            + 146 5
            + 133 6",
        ));
        proj.expect_chunk().await;
        tx.push_chunk(StreamChunk::from_pretty(
            "   I I
            + 213 8
            - 133 6",
        ));
        proj.expect_chunk().await;

        tx.push_barrier(test_epoch(2), false);
        let w3 = proj.expect_watermark().await;
        proj.expect_barrier().await;

        tx.push_chunk(StreamChunk::from_pretty(
            "   I I
            + 100 3
            + 104 5
            + 187 3",
        ));
        proj.expect_chunk().await;

        tx.push_barrier(test_epoch(3), false);
        let w4 = proj.expect_watermark().await;
        proj.expect_barrier().await;

        assert_eq!(w3.col_idx, w4.col_idx);
        assert!(w3.val.default_cmp(&w4.val).is_le());

        tx.push_int64_watermark(1, 100);
        tx.push_barrier(test_epoch(4), true);

        assert!(proj.next().await.unwrap().unwrap().is_stop());
    }
}

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

use std::future::Future;

use futures::future::{Either, pending, select};
use futures::pin_mut;
use futures::stream::FuturesOrdered;
use multimap::MultiMap;
use risingwave_common::row::RowExt;
use risingwave_common::types::ToOwnedDatum;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::expr::NonStrictExpression;

use crate::executor::prelude::*;

type ProjectMessageFuture = impl Future<Output = StreamExecutorResult<ProjectedMessage>> + Send;
type PendingProjectMessages = FuturesOrdered<ProjectMessageFuture>;

enum ProjectMessageInput {
    Chunk(StreamChunk),
    Watermark {
        watermark: Watermark,
        out_col_indices: Vec<usize>,
    },
    Barrier(Barrier),
}

enum ProjectedMessage {
    Chunk(StreamChunk),
    Watermark(Vec<Watermark>),
    Barrier(Barrier),
}

/// `ProjectExecutor` project data with the `expr`. The `expr` takes a chunk of data,
/// and returns a new data chunk. And then, `ProjectExecutor` will insert, delete
/// or update element into next operator according to the result of the expression.
pub struct ProjectExecutor {
    input: Executor,
    inner: Inner,
}

struct Inner {
    _ctx: ActorContextRef,

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
        Self {
            input,
            inner: Inner {
                _ctx: ctx,
                exprs: Arc::new(exprs),
                watermark_derivations,
                nondecreasing_expr_indices,
                last_nondec_expr_values: vec![None; n_nondecreasing_exprs],
                eliminate_noop_updates,
                project_expr_concurrency,
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
    #[define_opaque(ProjectMessageFuture)]
    fn project_message(
        exprs: Arc<Vec<NonStrictExpression>>,
        eliminate_noop_updates: bool,
        input: ProjectMessageInput,
    ) -> ProjectMessageFuture {
        async move {
            match input {
                ProjectMessageInput::Chunk(chunk) => {
                    let mut new_chunk = apply_project_exprs(&exprs, chunk).await?;
                    if eliminate_noop_updates {
                        new_chunk = new_chunk.eliminate_adjacent_noop_update();
                    }
                    Ok(ProjectedMessage::Chunk(new_chunk))
                }
                ProjectMessageInput::Watermark {
                    watermark,
                    out_col_indices,
                } => {
                    let mut ret = vec![];
                    for out_col_idx in out_col_indices {
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
                    Ok(ProjectedMessage::Watermark(ret))
                }
                ProjectMessageInput::Barrier(barrier) => Ok(ProjectedMessage::Barrier(barrier)),
            }
        }
    }

    fn update_last_nondec_expr_values(
        nondecreasing_expr_indices: &[usize],
        last_nondec_expr_values: &mut [Option<ScalarImpl>],
        new_chunk: &StreamChunk,
    ) {
        {
            {
                {
                    {
                        if !nondecreasing_expr_indices.is_empty()
                            && let Some((_, first_visible_row)) = new_chunk.rows().next()
                        {
                            // it's ok to use the first row here, just one chunk delay
                            first_visible_row
                                .project(nondecreasing_expr_indices)
                                .iter()
                                .enumerate()
                                .for_each(|(idx, value)| {
                                    last_nondec_expr_values[idx] =
                                        Some(value.to_owned_datum().expect(
                                            "non-decreasing expression should never be NULL",
                                        ));
                                });
                        }
                    }
                }
            }
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute(self, input: Executor) {
        let Inner {
            _ctx: _,
            exprs,
            watermark_derivations,
            nondecreasing_expr_indices,
            mut last_nondec_expr_values,
            eliminate_noop_updates,
            project_expr_concurrency,
        } = self;

        let mut input = input.execute();
        let first_barrier = expect_first_barrier(&mut input).await?;
        let mut is_paused = first_barrier.is_pause_on_startup();
        yield Message::Barrier(first_barrier);

        let mut pending_project_messages = PendingProjectMessages::new();

        loop {
            let has_pending_project_message = !pending_project_messages.is_empty();
            let can_read_input = pending_project_messages.len() < project_expr_concurrency;
            let next_projected_message = async {
                if has_pending_project_message {
                    pending_project_messages
                        .next()
                        .await
                        .expect("pending project messages should not be empty")
                } else {
                    pending().await
                }
            };
            let next_input_msg = async {
                if can_read_input {
                    input.next().await.ok_or_else(|| {
                        StreamExecutorError::channel_closed("upstream executor closed unexpectedly")
                    })?
                } else {
                    pending().await
                }
            };

            pin_mut!(next_projected_message);
            pin_mut!(next_input_msg);

            match select(next_projected_message, next_input_msg).await {
                Either::Left((projected_message, _)) => match projected_message? {
                    ProjectedMessage::Chunk(new_chunk) => {
                        Self::update_last_nondec_expr_values(
                            &nondecreasing_expr_indices,
                            &mut last_nondec_expr_values,
                            &new_chunk,
                        );
                        yield Message::Chunk(new_chunk);
                    }
                    ProjectedMessage::Watermark(watermarks) => {
                        for watermark in watermarks {
                            yield Message::Watermark(watermark);
                        }
                    }
                    ProjectedMessage::Barrier(barrier) => {
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
                },
                Either::Right((msg, _)) => match msg? {
                    Message::Watermark(w) => {
                        let out_col_indices = match watermark_derivations.get_vec(&w.col_idx) {
                            Some(v) => v,
                            None => continue,
                        };
                        pending_project_messages.push_back(Self::project_message(
                            exprs.clone(),
                            eliminate_noop_updates,
                            ProjectMessageInput::Watermark {
                                watermark: w,
                                out_col_indices: out_col_indices.clone(),
                            },
                        ));
                    }
                    Message::Chunk(chunk) => {
                        pending_project_messages.push_back(Self::project_message(
                            exprs.clone(),
                            eliminate_noop_updates,
                            ProjectMessageInput::Chunk(chunk),
                        ));
                    }
                    Message::Barrier(barrier) => {
                        pending_project_messages.push_back(Self::project_message(
                            exprs.clone(),
                            eliminate_noop_updates,
                            ProjectMessageInput::Barrier(barrier),
                        ));
                    }
                },
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

    #[derive(Debug)]
    struct FirstProjectExprWaitsForStartedCount {
        started_count: Arc<AtomicUsize>,
        started_notify: Arc<Notify>,
        unblock_first_at_started_count: usize,
    }

    #[async_trait::async_trait]
    impl Expression for FirstProjectExprWaitsForStartedCount {
        fn return_type(&self) -> DataType {
            DataType::Int64
        }

        async fn eval_v2(&self, input: &DataChunk) -> expr::Result<ValueImpl> {
            let call_idx = self.started_count.fetch_add(1, atomic::Ordering::SeqCst);
            self.started_notify.notify_waiters();
            if call_idx == 0 {
                loop {
                    let notified = self.started_notify.notified();
                    if self.started_count.load(atomic::Ordering::SeqCst)
                        >= self.unblock_first_at_started_count
                    {
                        break;
                    }
                    notified.await;
                }
            }

            Ok(ValueImpl::Scalar {
                value: Some((call_idx as i64).into()),
                capacity: input.capacity(),
            })
        }

        async fn eval_row(&self, _input: &OwnedRow) -> expr::Result<Datum> {
            Ok(Some(0_i64.into()))
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

    #[tokio::test]
    async fn test_projection_keeps_concurrency_across_barrier() {
        let schema = Schema {
            fields: vec![Field::unnamed(DataType::Int64)],
        };
        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(schema, StreamKey::new());

        let started_count = Arc::new(AtomicUsize::new(0));
        let started_notify = Arc::new(Notify::new());
        let test_expr = NonStrictExpression::for_test(FirstProjectExprWaitsForStartedCount {
            started_count: started_count.clone(),
            started_notify: started_notify.clone(),
            unblock_first_at_started_count: 3,
        });

        let proj = ProjectExecutor::new(
            actor_context_with_project_expr_concurrency(4),
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
        tx.push_barrier(test_epoch(2), false);
        tx.push_chunk(StreamChunk::from_pretty(
            " I
            + 3",
        ));
        tx.push_barrier(test_epoch(3), true);

        let first_msg = timeout(Duration::from_secs(5), async {
            let next_msg = proj.next();
            pin_mut!(next_msg);
            let mut first_msg = None;
            loop {
                let notified = started_notify.notified();
                if started_count.load(atomic::Ordering::SeqCst) >= 3 {
                    break;
                }
                tokio::select! {
                    _ = notified => {}
                    msg = &mut next_msg => {
                        if started_count.load(atomic::Ordering::SeqCst) < 3 {
                            panic!("project executor emitted before the post-barrier chunk started: {msg:?}");
                        }
                        first_msg = Some(msg.unwrap().unwrap());
                        break;
                    }
                }
            }
            match first_msg {
                Some(msg) => msg,
                None => next_msg.await.unwrap().unwrap(),
            }
        })
        .await
        .expect("post-barrier chunk expression did not start");

        assert_eq!(
            *first_msg.as_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I
                + 0"
            )
        );
        assert_eq!(
            *proj.next().await.unwrap().unwrap().as_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I
                + 1"
            )
        );
        proj.expect_barrier().await;
        assert_eq!(
            *proj.next().await.unwrap().unwrap().as_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I
                + 2"
            )
        );
        assert!(proj.next().await.unwrap().unwrap().is_stop());
    }

    #[tokio::test]
    async fn test_projection_keeps_concurrency_across_watermark() {
        let schema = Schema {
            fields: vec![Field::unnamed(DataType::Int64)],
        };
        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(schema, StreamKey::new());

        let started_count = Arc::new(AtomicUsize::new(0));
        let started_notify = Arc::new(Notify::new());
        let test_expr = NonStrictExpression::for_test(FirstProjectExprWaitsForStartedCount {
            started_count: started_count.clone(),
            started_notify: started_notify.clone(),
            unblock_first_at_started_count: 3,
        });

        let proj = ProjectExecutor::new(
            actor_context_with_project_expr_concurrency(4),
            source,
            vec![test_expr],
            MultiMap::from_iter(vec![(0, 0)].into_iter()),
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
        tx.push_int64_watermark(0, 100);
        tx.push_chunk(StreamChunk::from_pretty(
            " I
            + 3",
        ));
        tx.push_barrier(test_epoch(2), true);

        let first_msg = timeout(Duration::from_secs(5), async {
            let next_msg = proj.next();
            pin_mut!(next_msg);
            let mut first_msg = None;
            loop {
                let notified = started_notify.notified();
                if started_count.load(atomic::Ordering::SeqCst) >= 3 {
                    break;
                }
                tokio::select! {
                    _ = notified => {}
                    msg = &mut next_msg => {
                        if started_count.load(atomic::Ordering::SeqCst) < 3 {
                            panic!("project executor emitted before the post-watermark chunk started: {msg:?}");
                        }
                        first_msg = Some(msg.unwrap().unwrap());
                        break;
                    }
                }
            }
            match first_msg {
                Some(msg) => msg,
                None => next_msg.await.unwrap().unwrap(),
            }
        })
        .await
        .expect("post-watermark chunk expression did not start");

        assert_eq!(
            *first_msg.as_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I
                + 0"
            )
        );
        assert_eq!(
            *proj.next().await.unwrap().unwrap().as_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I
                + 1"
            )
        );
        assert_eq!(
            proj.expect_watermark().await,
            Watermark {
                col_idx: 0,
                data_type: DataType::Int64,
                val: ScalarImpl::Int64(0)
            }
        );
        assert_eq!(
            *proj.next().await.unwrap().unwrap().as_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I
                + 2"
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

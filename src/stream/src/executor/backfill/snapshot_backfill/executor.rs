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

use std::cmp::min;
use std::collections::VecDeque;
use std::future::{Future, pending, ready};
use std::mem::take;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use futures::future::{Either, try_join_all};
use futures::{FutureExt, Stream, TryFutureExt, TryStreamExt, pin_mut};
use risingwave_common::array::StreamChunk;
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::metrics::LabelGuardedIntCounter;
use risingwave_common::row::OwnedRow;
use risingwave_common::util::epoch::{Epoch, EpochPair};
use risingwave_common_rate_limit::RateLimit;
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_storage::StateStore;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::table::ChangeLogRow;
use risingwave_storage::table::batch_table::BatchTable;
use tokio::select;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::executor::backfill::snapshot_backfill::receive_next_barrier;
use crate::executor::backfill::snapshot_backfill::state::{
    BackfillState, EpochBackfillProgress, VnodeBackfillProgress,
};
use crate::executor::backfill::snapshot_backfill::vnode_stream::VnodeStream;
use crate::executor::backfill::utils::{create_builder, mapping_message};
use crate::executor::monitor::StreamingMetrics;
use crate::executor::prelude::{StateTable, StreamExt, try_stream};
use crate::executor::{
    ActorContextRef, Barrier, BoxedMessageStream, DispatcherBarrier, DispatcherMessage, Execute,
    MergeExecutorInput, Message, StreamExecutorError, StreamExecutorResult, expect_first_barrier,
};
use crate::task::CreateMviewProgressReporter;

pub struct SnapshotBackfillExecutor<S: StateStore> {
    /// Upstream table
    upstream_table: BatchTable<S>,

    /// Backfill progress table
    progress_state_table: StateTable<S>,

    /// Upstream with the same schema with the upstream table.
    upstream: MergeExecutorInput,

    /// The column indices need to be forwarded to the downstream from the upstream and table scan.
    output_indices: Vec<usize>,

    progress: CreateMviewProgressReporter,

    chunk_size: usize,
    rate_limit: RateLimit,

    barrier_rx: UnboundedReceiver<Barrier>,

    actor_ctx: ActorContextRef,
    metrics: Arc<StreamingMetrics>,

    snapshot_epoch: Option<u64>,
}

impl<S: StateStore> SnapshotBackfillExecutor<S> {
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new(
        upstream_table: BatchTable<S>,
        progress_state_table: StateTable<S>,
        upstream: MergeExecutorInput,
        output_indices: Vec<usize>,
        actor_ctx: ActorContextRef,
        progress: CreateMviewProgressReporter,
        chunk_size: usize,
        rate_limit: RateLimit,
        barrier_rx: UnboundedReceiver<Barrier>,
        metrics: Arc<StreamingMetrics>,
        snapshot_epoch: Option<u64>,
    ) -> Self {
        assert_eq!(&upstream.info.schema, upstream_table.schema());
        if upstream_table.pk_in_output_indices().is_none() {
            panic!(
                "storage table should include all pk columns in output: pk_indices: {:?}, output_indices: {:?}, schema: {:?}",
                upstream_table.pk_indices(),
                upstream_table.output_indices(),
                upstream_table.schema()
            )
        };
        if !matches!(rate_limit, RateLimit::Disabled) {
            debug!(
                ?rate_limit,
                "create snapshot backfill executor with rate limit"
            );
        }
        Self {
            upstream_table,
            progress_state_table,
            upstream,
            output_indices,
            progress,
            chunk_size,
            rate_limit,
            barrier_rx,
            actor_ctx,
            metrics,
            snapshot_epoch,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        debug!("snapshot backfill executor start");
        let first_upstream_barrier = expect_first_barrier(&mut self.upstream).await?;
        debug!(epoch = ?first_upstream_barrier.epoch, "get first upstream barrier");
        let first_recv_barrier = receive_next_barrier(&mut self.barrier_rx).await?;
        debug!(epoch = ?first_recv_barrier.epoch, "get first inject barrier");
        let should_snapshot_backfill: Option<u64> = if let Some(snapshot_epoch) =
            self.snapshot_epoch
        {
            if first_upstream_barrier.epoch != first_recv_barrier.epoch {
                assert!(snapshot_epoch <= first_upstream_barrier.epoch.prev);
                Some(snapshot_epoch)
            } else {
                None
            }
        } else {
            // when snapshot epoch is not set, the StreamNode must be created previously and has finished the backfill
            if cfg!(debug_assertions) {
                panic!(
                    "snapshot epoch not set. first_upstream_epoch: {:?}, first_recv_epoch: {:?}",
                    first_upstream_barrier.epoch, first_recv_barrier.epoch
                );
            } else {
                warn!(first_upstream_epoch = ?first_upstream_barrier.epoch, first_recv_epoch=?first_recv_barrier.epoch, "snapshot epoch not set");
                assert_eq!(first_upstream_barrier.epoch, first_recv_barrier.epoch);
                None
            }
        };
        let first_recv_barrier_epoch = first_recv_barrier.epoch;
        yield Message::Barrier(first_recv_barrier);
        let mut backfill_state = BackfillState::new(
            self.progress_state_table,
            first_recv_barrier_epoch,
            self.upstream_table.pk_serializer().clone(),
        )
        .await?;

        let (mut barrier_epoch, mut need_report_finish) = {
            if let Some(snapshot_epoch) = should_snapshot_backfill {
                let table_id_str = format!("{}", self.upstream_table.table_id().table_id);
                let actor_id_str = format!("{}", self.actor_ctx.id);

                let consume_upstream_row_count = self
                    .metrics
                    .snapshot_backfill_consume_row_count
                    .with_guarded_label_values(&[&table_id_str, &actor_id_str, "consume_upstream"]);

                let mut upstream_buffer = UpstreamBuffer::new(
                    &mut self.upstream,
                    first_upstream_barrier,
                    consume_upstream_row_count,
                );

                // Phase 1: consume upstream snapshot
                let (mut barrier_epoch, upstream_buffer) = if first_recv_barrier_epoch.prev
                    < snapshot_epoch
                {
                    info!(
                        table_id = %self.upstream_table.table_id(),
                        snapshot_epoch,
                        barrier_epoch = ?first_recv_barrier_epoch,
                        "start consuming snapshot"
                    );
                    {
                        let consuming_snapshot_row_count = self
                            .metrics
                            .snapshot_backfill_consume_row_count
                            .with_guarded_label_values(&[
                                &table_id_str,
                                &actor_id_str,
                                "consuming_snapshot",
                            ]);
                        let snapshot_stream = make_consume_snapshot_stream(
                            &self.upstream_table,
                            snapshot_epoch,
                            self.chunk_size,
                            self.rate_limit,
                            &mut self.barrier_rx,
                            &mut self.progress,
                            &mut backfill_state,
                            first_recv_barrier_epoch,
                        );

                        pin_mut!(snapshot_stream);

                        while let Some(message) = upstream_buffer
                            .run_future(snapshot_stream.try_next())
                            .await?
                        {
                            if let Message::Chunk(chunk) = &message {
                                consuming_snapshot_row_count.inc_by(chunk.cardinality() as _);
                            }
                            yield message;
                        }
                    }

                    let recv_barrier = self.barrier_rx.recv().await.expect("should exist");
                    let recv_barrier_epoch = recv_barrier.epoch;
                    assert_eq!(snapshot_epoch, recv_barrier_epoch.prev);
                    let post_commit = backfill_state.commit(recv_barrier.epoch).await?;
                    yield Message::Barrier(recv_barrier);
                    post_commit.post_yield_barrier(None).await?;
                    (
                        recv_barrier_epoch,
                        upstream_buffer.start_consuming_log_store(snapshot_epoch),
                    )
                } else {
                    info!(
                        table_id = %self.upstream_table.table_id(),
                        snapshot_epoch,
                        barrier_epoch = ?first_recv_barrier_epoch,
                        "skip consuming snapshot"
                    );
                    (
                        first_recv_barrier_epoch,
                        upstream_buffer.start_consuming_log_store(first_recv_barrier_epoch.prev),
                    )
                };

                // Phase 2: consume upstream log store
                if let Some(mut upstream_buffer) = upstream_buffer {
                    let initial_pending_lag = Duration::from_millis(
                        Epoch(upstream_buffer.pending_epoch_lag()).physical_time(),
                    );
                    info!(
                        ?barrier_epoch,
                        table_id = self.upstream_table.table_id().table_id,
                        ?initial_pending_lag,
                        "start consuming log store"
                    );

                    let consuming_log_store_row_count = self
                        .metrics
                        .snapshot_backfill_consume_row_count
                        .with_guarded_label_values(&[
                            &table_id_str,
                            &actor_id_str,
                            "consuming_log_store",
                        ]);
                    loop {
                        let barrier = receive_next_barrier(&mut self.barrier_rx).await?;
                        assert_eq!(barrier_epoch.curr, barrier.epoch.prev);
                        let is_finished = upstream_buffer.consumed_epoch(barrier.epoch).await?;
                        {
                            // we must call `next_epoch` after `consumed_epoch`, and otherwise in `next_epoch`
                            // we may block the upstream, and the upstream never get a chance to finish the `next_epoch`
                            let next_prev_epoch =
                                self.upstream_table.next_epoch(barrier_epoch.prev).await?;
                            assert_eq!(next_prev_epoch, barrier.epoch.prev);
                        }
                        barrier_epoch = barrier.epoch;
                        debug!(?barrier_epoch, kind = ?barrier.kind, "start consume epoch change log");
                        // use `upstream_buffer.run_future` to poll upstream concurrently so that we won't have back-pressure
                        // on the upstream. Otherwise, in `batch_iter_log_with_pk_bounds`, we may wait upstream epoch to be committed,
                        // and the back-pressure may cause the upstream unable to consume the barrier and then cause deadlock.
                        let mut stream = upstream_buffer
                            .run_future(make_log_stream(
                                &self.upstream_table,
                                barrier_epoch.prev,
                                None,
                                self.chunk_size,
                            ))
                            .await?;
                        while let Some(chunk) =
                            upstream_buffer.run_future(stream.try_next()).await?
                        {
                            trace!(
                                ?barrier_epoch,
                                size = chunk.cardinality(),
                                "consume change log yield chunk",
                            );
                            consuming_log_store_row_count.inc_by(chunk.cardinality() as _);
                            yield Message::Chunk(chunk);
                        }

                        trace!(?barrier_epoch, "after consume change log");

                        if is_finished {
                            assert_eq!(upstream_buffer.pending_epoch_lag(), 0);
                            self.progress.finish_consuming_log_store(barrier.epoch);
                        } else {
                            self.progress.update_create_mview_log_store_progress(
                                barrier.epoch,
                                upstream_buffer.pending_epoch_lag(),
                            );
                        }

                        stream
                            .for_vnode_pk_progress(|vnode, row_count, progress| {
                                assert_eq!(progress, None);
                                backfill_state.finish_epoch(vnode, barrier.epoch.prev, row_count);
                            })
                            .await?;
                        let post_commit = backfill_state.commit(barrier.epoch).await?;
                        let update_vnode_bitmap = barrier.as_update_vnode_bitmap(self.actor_ctx.id);
                        yield Message::Barrier(barrier);
                        post_commit.post_yield_barrier(None).await?;
                        if update_vnode_bitmap.is_some() {
                            return Err(anyhow!(
                                "should not update vnode bitmap during consuming log store"
                            )
                            .into());
                        }

                        if is_finished {
                            break;
                        }
                    }
                    info!(
                        ?barrier_epoch,
                        table_id = self.upstream_table.table_id().table_id,
                        "finish consuming log store"
                    );

                    (barrier_epoch, false)
                } else {
                    info!(
                        ?barrier_epoch,
                        table_id = self.upstream_table.table_id().table_id,
                        "skip consuming log store and start consuming upstream directly"
                    );

                    (barrier_epoch, true)
                }
            } else {
                backfill_state
                    .latest_progress()
                    .for_each(|(vnode, progress)| {
                        let progress = progress.expect("should not be empty");
                        assert_eq!(
                            progress.epoch, first_upstream_barrier.epoch.prev,
                            "vnode: {:?}",
                            vnode
                        );
                        assert_eq!(
                            progress.progress,
                            EpochBackfillProgress::Consumed,
                            "vnode: {:?}",
                            vnode
                        );
                    });
                info!(
                    table_id = self.upstream_table.table_id().table_id,
                    "skip backfill"
                );
                assert_eq!(first_upstream_barrier.epoch, first_recv_barrier_epoch);
                (first_upstream_barrier.epoch, true)
            }
        };
        let mut upstream = self.upstream.into_executor(self.barrier_rx).execute();
        let mut epoch_row_count = 0;
        // Phase 3: consume upstream
        while let Some(msg) = upstream.try_next().await? {
            match msg {
                Message::Barrier(barrier) => {
                    assert_eq!(barrier.epoch.prev, barrier_epoch.curr);
                    self.upstream_table
                        .vnodes()
                        .iter_vnodes()
                        .for_each(|vnode| {
                            // Note: the `epoch_row_count` is the accumulated row count of all vnodes of the current
                            // executor.
                            backfill_state.finish_epoch(vnode, barrier.epoch.prev, epoch_row_count);
                        });
                    epoch_row_count = 0;
                    let update_vnode_bitmap = barrier.as_update_vnode_bitmap(self.actor_ctx.id);
                    barrier_epoch = barrier.epoch;
                    if need_report_finish {
                        need_report_finish = false;
                        self.progress.finish_consuming_log_store(barrier_epoch);
                    }
                    let post_commit = backfill_state.commit(barrier.epoch).await?;
                    yield Message::Barrier(barrier);
                    if let Some(new_vnode_bitmap) =
                        post_commit.post_yield_barrier(update_vnode_bitmap).await?
                    {
                        let _prev_vnode_bitmap = self
                            .upstream_table
                            .update_vnode_bitmap(new_vnode_bitmap.clone());
                        backfill_state
                            .latest_progress()
                            .for_each(|(vnode, progress)| {
                                let progress = progress.expect("should not be empty");
                                assert_eq!(
                                    progress.epoch, barrier_epoch.prev,
                                    "vnode {:?} has unexpected progress epoch",
                                    vnode
                                );
                                assert_eq!(
                                    progress.progress,
                                    EpochBackfillProgress::Consumed,
                                    "vnode {:?} has unexpected progress",
                                    vnode
                                );
                            });
                    }
                }
                msg => {
                    if let Message::Chunk(chunk) = &msg {
                        epoch_row_count += chunk.cardinality();
                    }
                    yield msg;
                }
            }
        }
    }
}

impl<S: StateStore> Execute for SnapshotBackfillExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        let output_indices = self.output_indices.clone();
        self.execute_inner()
            .filter_map(move |result| {
                ready({
                    match result {
                        Ok(message) => mapping_message(message, &output_indices).map(Ok),
                        Err(e) => Some(Err(e)),
                    }
                })
            })
            .boxed()
    }
}

struct ConsumingSnapshot;
struct ConsumingLogStore;

struct UpstreamBuffer<'a, S> {
    upstream: &'a mut MergeExecutorInput,
    max_pending_epoch_lag: u64,
    consumed_epoch: u64,
    // newer barrier at the front
    pending_non_checkpoint_barriers: VecDeque<DispatcherBarrier>,
    /// Barriers received from upstream but not yet received the barrier from local barrier worker.
    ///
    /// In the outer `VecDeque`, newer barriers at the front.
    /// In the inner `VecDeque`, newer barrier at the front, with the last barrier as checkpoint barrier,
    /// and others as non-checkpoint barrier
    upstream_pending_barriers: VecDeque<VecDeque<DispatcherBarrier>>,
    /// Whether we have started polling any upstream data before the next barrier.
    /// When `true`, we should continue polling until the next barrier, because
    /// some data in this epoch have been discarded and data in this epoch
    /// must be read from log store
    is_polling_epoch_data: bool,
    consume_upstream_row_count: LabelGuardedIntCounter<3>,
    _phase: S,
}

impl<'a> UpstreamBuffer<'a, ConsumingSnapshot> {
    fn new(
        upstream: &'a mut MergeExecutorInput,
        first_upstream_barrier: DispatcherBarrier,
        consume_upstream_row_count: LabelGuardedIntCounter<3>,
    ) -> Self {
        Self {
            upstream,
            is_polling_epoch_data: false,
            consume_upstream_row_count,
            pending_non_checkpoint_barriers: Default::default(),
            upstream_pending_barriers: VecDeque::from_iter([VecDeque::from_iter([
                first_upstream_barrier,
            ])]),
            // no limit on the number of pending barrier in the beginning
            max_pending_epoch_lag: u64::MAX,
            consumed_epoch: 0,
            _phase: ConsumingSnapshot {},
        }
    }

    fn start_consuming_log_store(
        mut self,
        consumed_epoch: u64,
    ) -> Option<UpstreamBuffer<'a, ConsumingLogStore>> {
        // it's safe to access upstream_pending_barriers[0][0] because it's first upstream barrier
        let first_upstream_barrier_epoch = self
            .upstream_pending_barriers
            .back()
            .expect("first upstream barrier")
            .back()
            .expect("first upstream barrier")
            .epoch;
        if first_upstream_barrier_epoch.prev == consumed_epoch {
            assert_eq!(
                1,
                self.upstream_pending_barriers
                    .pop_back()
                    .expect("non-empty")
                    .len()
            );
        }
        let max_pending_epoch_lag = self.pending_epoch_lag();
        let buffer = UpstreamBuffer {
            upstream: self.upstream,
            pending_non_checkpoint_barriers: self.pending_non_checkpoint_barriers,
            upstream_pending_barriers: self.upstream_pending_barriers,
            max_pending_epoch_lag,
            is_polling_epoch_data: self.is_polling_epoch_data,
            consume_upstream_row_count: self.consume_upstream_row_count,
            consumed_epoch,
            _phase: ConsumingLogStore {},
        };
        if buffer.is_finished() {
            None
        } else {
            Some(buffer)
        }
    }
}

impl<S> UpstreamBuffer<'_, S> {
    fn can_consume_upstream(&self) -> bool {
        self.is_polling_epoch_data || self.pending_epoch_lag() < self.max_pending_epoch_lag
    }

    async fn concurrently_consume_upstream(&mut self) -> StreamExecutorError {
        {
            loop {
                if let Err(e) = try {
                    if !self.can_consume_upstream() {
                        // pause the future to block consuming upstream
                        return pending().await;
                    }
                    self.consume_until_next_checkpoint_barrier().await?;
                } {
                    break e;
                }
            }
        }
    }

    /// Consume the upstream until seeing the next barrier.
    async fn consume_until_next_checkpoint_barrier(&mut self) -> StreamExecutorResult<()> {
        let barriers = loop {
            let msg: DispatcherMessage = self
                .upstream
                .try_next()
                .await?
                .ok_or_else(|| anyhow!("end of upstream"))?;
            match msg {
                DispatcherMessage::Chunk(chunk) => {
                    self.is_polling_epoch_data = true;
                    self.consume_upstream_row_count
                        .inc_by(chunk.cardinality() as _);
                }
                DispatcherMessage::Barrier(barrier) => {
                    let is_checkpoint = barrier.kind.is_checkpoint();
                    self.pending_non_checkpoint_barriers.push_front(barrier);
                    if is_checkpoint {
                        self.is_polling_epoch_data = false;
                        break take(&mut self.pending_non_checkpoint_barriers);
                    } else {
                        self.is_polling_epoch_data = true;
                    }
                }
                DispatcherMessage::Watermark(_) => {
                    self.is_polling_epoch_data = true;
                }
            }
        };
        self.upstream_pending_barriers.push_front(barriers);
        Ok(())
    }
}

impl UpstreamBuffer<'_, ConsumingLogStore> {
    async fn consumed_epoch(&mut self, epoch: EpochPair) -> StreamExecutorResult<bool> {
        assert!(!self.is_finished());
        if self.upstream_pending_barriers.is_empty() {
            // when upstream_pending_barriers is empty and not polling any intermediate epoch data,
            // we must have returned true to indicate finish, and should not be called again.
            assert!(self.is_polling_epoch_data);
            self.consume_until_next_checkpoint_barrier().await?;
            assert_eq!(self.upstream_pending_barriers.len(), 1);
        }
        let barriers = self
            .upstream_pending_barriers
            .back_mut()
            .expect("non-empty");
        let oldest_upstream_barrier = barriers.back().expect("non-empty");
        assert!(
            oldest_upstream_barrier.epoch.prev >= epoch.prev,
            "oldest upstream barrier has epoch {:?} earlier than epoch to consume {:?}",
            oldest_upstream_barrier.epoch,
            epoch
        );
        if oldest_upstream_barrier.epoch.prev == epoch.prev {
            assert_eq!(oldest_upstream_barrier.epoch, epoch);
            barriers.pop_back();
            if barriers.is_empty() {
                self.upstream_pending_barriers.pop_back();
            }
        }
        {
            {
                let prev_epoch = epoch.prev;
                assert!(self.consumed_epoch < prev_epoch);
                let elapsed_epoch = prev_epoch - self.consumed_epoch;
                self.consumed_epoch = prev_epoch;
                if !self.upstream_pending_barriers.is_empty() {
                    // try consuming ready upstreams when we haven't yielded all pending barriers yet.
                    while self.can_consume_upstream()
                        && let Some(result) =
                            self.consume_until_next_checkpoint_barrier().now_or_never()
                    {
                        result?;
                    }
                }
                // sub to ensure that the lag is monotonically decreasing.
                // here we subtract half the elapsed epoch, so that approximately when downstream progresses two epochs,
                // the upstream can at least progress for one epoch.
                self.max_pending_epoch_lag = min(
                    self.pending_epoch_lag(),
                    self.max_pending_epoch_lag.saturating_sub(elapsed_epoch / 2),
                );
            }
        }
        Ok(self.is_finished())
    }

    fn is_finished(&self) -> bool {
        self.upstream_pending_barriers.is_empty() && !self.is_polling_epoch_data
    }
}

impl<S> UpstreamBuffer<'_, S> {
    /// Run a future while concurrently polling the upstream so that the upstream
    /// won't be back-pressured.
    async fn run_future<T, E: Into<StreamExecutorError>>(
        &mut self,
        future: impl Future<Output = Result<T, E>>,
    ) -> StreamExecutorResult<T> {
        select! {
            biased;
            e = self.concurrently_consume_upstream() => {
                Err(e)
            }
            // this arm won't be starved, because the first arm is always pending unless returning with error
            result = future => {
                result.map_err(Into::into)
            }
        }
    }

    fn pending_epoch_lag(&self) -> u64 {
        self.upstream_pending_barriers
            .front()
            .map(|barriers| {
                barriers
                    .front()
                    .expect("non-empty")
                    .epoch
                    .prev
                    .checked_sub(self.consumed_epoch)
                    .expect("pending epoch must be later than consumed_epoch")
            })
            .unwrap_or(0)
    }
}

async fn make_log_stream(
    upstream_table: &BatchTable<impl StateStore>,
    prev_epoch: u64,
    start_pk: Option<OwnedRow>,
    chunk_size: usize,
) -> StreamExecutorResult<VnodeStream<impl super::vnode_stream::ChangeLogRowStream>> {
    let data_types = upstream_table.schema().data_types();
    let start_pk = start_pk.as_ref();
    // TODO: may avoid polling all vnodes concurrently at the same time but instead with a limit on concurrency.
    let vnode_streams = try_join_all(upstream_table.vnodes().iter_vnodes().map(move |vnode| {
        upstream_table
            .batch_iter_vnode_log(
                prev_epoch,
                HummockReadEpoch::Committed(prev_epoch),
                start_pk,
                vnode,
            )
            .map_ok(move |stream| {
                let stream = stream.map_err(Into::into);
                (vnode, stream, 0)
            })
    }))
    .await?;
    let builder = create_builder(RateLimit::Disabled, chunk_size, data_types.clone());
    Ok(VnodeStream::new(
        vnode_streams,
        upstream_table.pk_in_output_indices().expect("should exist"),
        builder,
    ))
}

async fn make_snapshot_stream(
    upstream_table: &BatchTable<impl StateStore>,
    snapshot_epoch: u64,
    backfill_state: &BackfillState<impl StateStore>,
    rate_limit: RateLimit,
    chunk_size: usize,
) -> StreamExecutorResult<VnodeStream<impl super::vnode_stream::ChangeLogRowStream>> {
    let data_types = upstream_table.schema().data_types();
    let vnode_streams = try_join_all(backfill_state.latest_progress().filter_map(
        move |(vnode, progress)| {
            let start_pk = match progress {
                None => Some((None, 0)),
                Some(VnodeBackfillProgress {
                    row_count,
                    progress: EpochBackfillProgress::Consuming { latest_pk },
                    ..
                }) => Some((Some(latest_pk), *row_count)),
                Some(VnodeBackfillProgress {
                    progress: EpochBackfillProgress::Consumed,
                    ..
                }) => None,
            };
            start_pk.map(|(start_pk, row_count)| {
                upstream_table
                    .batch_iter_vnode(
                        HummockReadEpoch::Committed(snapshot_epoch),
                        start_pk,
                        vnode,
                        PrefetchOptions::prefetch_for_large_range_scan(),
                    )
                    .map_ok(move |stream| {
                        let stream = stream.map_ok(ChangeLogRow::Insert).map_err(Into::into);
                        (vnode, stream, row_count)
                    })
            })
        },
    ))
    .await?;
    let builder = create_builder(rate_limit, chunk_size, data_types.clone());
    Ok(VnodeStream::new(
        vnode_streams,
        upstream_table.pk_in_output_indices().expect("should exist"),
        builder,
    ))
}

#[expect(clippy::too_many_arguments)]
#[try_stream(ok = Message, error = StreamExecutorError)]
async fn make_consume_snapshot_stream<'a, S: StateStore>(
    upstream_table: &'a BatchTable<S>,
    snapshot_epoch: u64,
    chunk_size: usize,
    rate_limit: RateLimit,
    barrier_rx: &'a mut UnboundedReceiver<Barrier>,
    progress: &'a mut CreateMviewProgressReporter,
    backfill_state: &'a mut BackfillState<S>,
    first_recv_barrier_epoch: EpochPair,
) {
    let mut barrier_epoch = first_recv_barrier_epoch;

    // start consume upstream snapshot
    let mut snapshot_stream = make_snapshot_stream(
        upstream_table,
        snapshot_epoch,
        &*backfill_state,
        rate_limit,
        chunk_size,
    )
    .await?;

    async fn select_barrier_and_snapshot_stream(
        barrier_rx: &mut UnboundedReceiver<Barrier>,
        snapshot_stream: &mut (impl Stream<Item = StreamExecutorResult<StreamChunk>> + Unpin),
        throttle_snapshot_stream: bool,
    ) -> StreamExecutorResult<Either<Barrier, Option<StreamChunk>>> {
        select!(
            result = receive_next_barrier(barrier_rx) => {
                Ok(Either::Left(result?))
            },
            result = snapshot_stream.try_next(), if !throttle_snapshot_stream => {
                Ok(Either::Right(result?))
            }
        )
    }

    let mut count = 0;
    let mut epoch_row_count = 0;
    loop {
        let throttle_snapshot_stream = epoch_row_count as u64 > rate_limit.to_u64();
        match select_barrier_and_snapshot_stream(
            barrier_rx,
            &mut snapshot_stream,
            throttle_snapshot_stream,
        )
        .await?
        {
            Either::Left(barrier) => {
                assert_eq!(barrier.epoch.prev, barrier_epoch.curr);
                barrier_epoch = barrier.epoch;
                if barrier_epoch.curr >= snapshot_epoch {
                    return Err(anyhow!("should not receive barrier with epoch {barrier_epoch:?} later than snapshot epoch {snapshot_epoch}").into());
                }
                if let Some(chunk) = snapshot_stream.consume_builder() {
                    count += chunk.cardinality();
                    epoch_row_count += chunk.cardinality();
                    yield Message::Chunk(chunk);
                }
                snapshot_stream
                    .for_vnode_pk_progress(|vnode, row_count, pk_progress| {
                        if let Some(pk) = pk_progress {
                            backfill_state.update_epoch_progress(
                                vnode,
                                snapshot_epoch,
                                row_count,
                                pk,
                            );
                        } else {
                            backfill_state.finish_epoch(vnode, snapshot_epoch, row_count);
                        }
                    })
                    .await?;
                let post_commit = backfill_state.commit(barrier.epoch).await?;
                debug!(?barrier_epoch, count, epoch_row_count, "update progress");
                progress.update(barrier_epoch, barrier_epoch.prev, count as _);
                epoch_row_count = 0;

                yield Message::Barrier(barrier);
                post_commit.post_yield_barrier(None).await?;
            }
            Either::Right(Some(chunk)) => {
                count += chunk.cardinality();
                epoch_row_count += chunk.cardinality();
                yield Message::Chunk(chunk);
            }
            Either::Right(None) => {
                break;
            }
        }
    }

    // finish consuming upstream snapshot, report finish
    let barrier_to_report_finish = receive_next_barrier(barrier_rx).await?;
    assert_eq!(barrier_to_report_finish.epoch.prev, barrier_epoch.curr);
    barrier_epoch = barrier_to_report_finish.epoch;
    info!(?barrier_epoch, count, "report finish");
    snapshot_stream
        .for_vnode_pk_progress(|vnode, row_count, pk_progress| {
            assert_eq!(pk_progress, None);
            backfill_state.finish_epoch(vnode, snapshot_epoch, row_count);
        })
        .await?;
    let post_commit = backfill_state.commit(barrier_epoch).await?;
    progress.finish(barrier_epoch, count as _);
    yield Message::Barrier(barrier_to_report_finish);
    post_commit.post_yield_barrier(None).await?;

    // keep receiving remaining barriers until receiving a barrier with epoch as snapshot_epoch
    loop {
        let barrier = receive_next_barrier(barrier_rx).await?;
        assert_eq!(barrier.epoch.prev, barrier_epoch.curr);
        barrier_epoch = barrier.epoch;
        let post_commit = backfill_state.commit(barrier.epoch).await?;
        yield Message::Barrier(barrier);
        post_commit.post_yield_barrier(None).await?;
        if barrier_epoch.curr == snapshot_epoch {
            break;
        }
    }
    info!(?barrier_epoch, "finish consuming snapshot");
}

// Copyright 2026 RisingWave Labs
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

//! List enumerates bounded pages of Iceberg update tasks and dispatches them to Fetch.

use std::time::Duration;

use either::Either;
use futures::{TryStreamExt, stream};
use risingwave_common::array::Op;
use risingwave_common::bail;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{ColumnCatalog, ColumnId};
use risingwave_common::config::StreamingConfig;
use risingwave_common::hash::VnodeCountCompat;
use risingwave_connector::source::iceberg::IcebergProperties;
use risingwave_connector::source::iceberg::update_planner::{
    IcebergUpdateBinding, IcebergUpdatePlanner,
};
use risingwave_connector::source::iceberg::update_state::{
    IcebergUpdateAssignment, IcebergUpdateListState,
};
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_pb::plan_common::StorageTableDesc;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::table::batch_table::BatchTable;
use thiserror_ext::AsReport;
use tokio::sync::mpsc::UnboundedReceiver;

use super::{decode_value, encode, properties, scan_metrics};
use crate::executor::prelude::*;
use crate::executor::source::{StreamSourceCore, barrier_to_message_stream};
use crate::executor::stream_reader::StreamReaderWithPause;

pub struct IcebergUpdateListExecutor<S: StateStore> {
    core: StreamSourceCore<S>,
    downstream_columns: Vec<ColumnCatalog>,
    fetch_state_table: StorageTableDesc,
    barriers: UnboundedReceiver<Barrier>,
    streaming_config: Arc<StreamingConfig>,
}

impl<S: StateStore> IcebergUpdateListExecutor<S> {
    pub fn new(
        core: StreamSourceCore<S>,
        downstream_columns: Vec<ColumnCatalog>,
        fetch_state_table: StorageTableDesc,
        barriers: UnboundedReceiver<Barrier>,
        streaming_config: Arc<StreamingConfig>,
    ) -> Self {
        Self {
            core,
            downstream_columns,
            fetch_state_table,
            barriers,
            streaming_config,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let developer = &self.streaming_config.developer;
        let list_interval = Duration::from_secs(developer.iceberg_list_interval_sec);
        let batch_size = developer.iceberg_fetch_batch_size as usize;
        let properties = properties(&mut self.core)?;
        let scan_metrics = scan_metrics(&self.core, &properties);
        let fetch_reader = fetch_state_reader(
            self.core
                .split_state_store
                .state_table()
                .state_store()
                .clone(),
            &self.fetch_state_table,
        );
        let ctx = Arc::new(ListContext {
            properties,
            downstream_columns: self.downstream_columns,
            batch_size,
            job_id: self.fetch_state_table.table_id.as_raw_id(),
            fetch_reader,
        });

        let state_table = self.core.split_state_store.state_table_mut();
        let mut barriers = barrier_to_message_stream(self.barriers).boxed();
        let first = expect_first_barrier(&mut barriers).await?;
        let first_epoch = first.epoch;
        let paused = first.is_pause_on_startup();
        yield Message::Barrier(first);
        state_table.init_epoch(first_epoch).await?;

        let mut persisted_state = state_table.get_from_one_value_table().await?;
        let mut state = persisted_state
            .as_ref()
            .map(|value| decode_value::<IcebergUpdateListState>(value.as_jsonb().clone()))
            .transpose()?;
        if let Some(state) = &state {
            state.validate_columns(ctx.job_id, &ctx.downstream_columns)?;
        }
        let mut dirty = false;
        let mut planner = None;
        let mut epoch = first_epoch.curr;
        // On recovery the previous epoch is already the recovery checkpoint.
        let mut checkpoint_epoch = first_epoch.prev;

        let mut stream = ListStream::new(
            barriers,
            list_work(
                ctx.clone(),
                state.clone(),
                None,
                checkpoint_epoch,
                Duration::ZERO,
            )
            .map(Ok),
        );
        if paused {
            stream.pause_stream();
        }
        let mut next_poll = NextPoll::Running;
        while let Some(message) = stream.next().await {
            match message? {
                Either::Left(Message::Barrier(barrier)) => {
                    match barrier.mutation.as_deref() {
                        Some(Mutation::Pause) => stream.pause_stream(),
                        Some(Mutation::Resume) => stream.resume_stream(),
                        _ => {}
                    }
                    if dirty {
                        write_list_state(
                            state_table,
                            &mut persisted_state,
                            state.as_ref().expect("dirty List state"),
                        )?;
                        dirty = false;
                    }
                    state_table
                        .commit_assert_no_update_vnode_bitmap(barrier.epoch)
                        .await?;
                    epoch = barrier.epoch.curr;
                    if barrier.is_checkpoint() {
                        checkpoint_epoch = barrier.epoch.prev;
                    }
                    // A committed read must run only after yielding its barrier. Waiting on
                    // the control arm would prevent the very checkpoint it needs to observe.
                    yield Message::Barrier(barrier);
                    if let Some(idle_delay) = next_poll.idle_delay(list_interval) {
                        stream.replace_data_stream(
                            list_work(
                                ctx.clone(),
                                state.clone(),
                                planner.take(),
                                checkpoint_epoch,
                                idle_delay,
                            )
                            .map(Ok),
                        );
                        next_poll = NextPoll::Running;
                    }
                }
                Either::Left(_) => bail!("unexpected input to Iceberg update List"),
                Either::Right(Err(e)) => {
                    tracing::warn!(
                        error = %e.as_report(),
                        "Iceberg update List errored, retrying"
                    );
                    scan_metrics.record_scan_error("list_error");
                    // The failed poll consumed the planner. The next one rebuilds it from the
                    // cursor in `state`, which this poll did not change.
                    stream.replace_data_stream(stream::pending());
                    next_poll = NextPoll::Throttled;
                }
                Either::Right(Ok(ListResult {
                    state: mut new_state,
                    planner: next_planner,
                    assignments,
                    phase_finished,
                })) => {
                    planner = next_planner;
                    if phase_finished {
                        new_state.record_phase_end(epoch)?;
                    }
                    let changed = state.as_ref() != Some(&new_state);
                    dirty |= changed;
                    next_poll = if !assignments.is_empty() && !phase_finished {
                        // More pages remain in this phase, so keep listing without a barrier.
                        NextPoll::Running
                    } else if assignments.is_empty()
                        && !changed
                        && new_state.phase_end_epoch().is_none()
                    {
                        NextPoll::Throttled
                    } else {
                        NextPoll::Now
                    };
                    state = Some(new_state);
                    if !assignments.is_empty() {
                        yield Message::Chunk(assignments_chunk(&assignments)?);
                    }
                    if next_poll == NextPoll::Running {
                        stream.replace_data_stream(
                            list_work(
                                ctx.clone(),
                                state.clone(),
                                planner.take(),
                                checkpoint_epoch,
                                Duration::ZERO,
                            )
                            .map(Ok),
                        );
                    } else {
                        stream.replace_data_stream(stream::pending());
                    }
                }
            }
        }
    }
}

impl<S: StateStore> Execute for IcebergUpdateListExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }
}

impl<S: StateStore> Debug for IcebergUpdateListExecutor<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergUpdateListExecutor")
            .finish_non_exhaustive()
    }
}

type ListStream = StreamReaderWithPause<true, StreamExecutorResult<ListResult>>;

/// When List starts its next poll.
#[derive(Clone, Copy, PartialEq, Eq)]
enum NextPoll {
    /// A poll is installed on the data arm.
    Running,
    /// Poll right after the next barrier.
    Now,
    /// The last poll found nothing new or failed. Wait `list_interval` after the next barrier.
    Throttled,
}

impl NextPoll {
    /// The delay before the next poll, or `None` if one is already running.
    fn idle_delay(self, list_interval: Duration) -> Option<Duration> {
        match self {
            NextPoll::Running => None,
            NextPoll::Now => Some(Duration::ZERO),
            NextPoll::Throttled => Some(list_interval),
        }
    }
}

/// Inputs that stay fixed for the lifetime of a List executor.
pub(super) struct ListContext<S: StateStore> {
    pub(super) properties: IcebergProperties,
    pub(super) downstream_columns: Vec<ColumnCatalog>,
    pub(super) batch_size: usize,
    pub(super) job_id: u32,
    pub(super) fetch_reader: BatchTable<S>,
}

pub(super) struct ListResult {
    pub(super) state: IcebergUpdateListState,
    pub(super) planner: Option<IcebergUpdatePlanner>,
    pub(super) assignments: Vec<IcebergUpdateAssignment>,
    pub(super) phase_finished: bool,
}

/// Metadata I/O runs on the data arm so it cannot prevent List from forwarding barriers.
/// `idle_delay` throttles only catalog polls; phase completion checks never wait for it.
#[try_stream(boxed, ok = ListResult, error = StreamExecutorError)]
pub(super) async fn list_work<S: StateStore>(
    ctx: Arc<ListContext<S>>,
    state: Option<IcebergUpdateListState>,
    mut planner: Option<IcebergUpdatePlanner>,
    checkpoint_epoch: u64,
    idle_delay: Duration,
) {
    let polls_catalog = state
        .as_ref()
        .is_none_or(|state| state.phase_end_epoch().is_none());
    if polls_catalog && !idle_delay.is_zero() {
        tokio::time::sleep(idle_delay).await;
    }
    let Some(mut state) = state else {
        // Bind only once, on the pausable data arm. Persist it before enumerating tasks, so
        // their checkpoint always includes the binding even if latest advances on recovery.
        let table = ctx.properties.load_table().await?;
        let binding = IcebergUpdateBinding::bind_columns(&table, &ctx.downstream_columns)?;
        yield ListResult {
            state: IcebergUpdateListState::new(ctx.job_id, binding),
            planner: None,
            assignments: vec![],
            phase_finished: false,
        };
        return Ok(());
    };
    let mut assignments = vec![];
    let mut phase_finished = false;
    if let Some(dispatched_epoch) = state.phase_end_epoch() {
        if checkpoint_epoch >= dispatched_epoch {
            let rows = ctx
                .fetch_reader
                .batch_iter(
                    HummockReadEpoch::Committed(checkpoint_epoch),
                    false,
                    PrefetchOptions::prefetch_for_small_range_scan(),
                )
                .await?;
            pin_mut!(rows);
            if rows.try_next().await?.is_none() {
                state.complete_phase(checkpoint_epoch)?;
            }
        }
    } else {
        let table = ctx.properties.load_table().await?;
        (assignments, phase_finished) = state
            .enumerate(&table, ctx.batch_size, &mut planner)
            .await?;
    }
    yield ListResult {
        state,
        planner,
        assignments,
        phase_finished,
    };
}

/// Reads the task keys of the Fetch state table across all vnodes, so that phase
/// completion observes the tasks of every Fetch actor.
fn fetch_state_reader<S: StateStore>(store: S, desc: &StorageTableDesc) -> BatchTable<S> {
    BatchTable::new_partial(
        store,
        vec![ColumnId::new(desc.columns[0].column_id)],
        Some(Arc::new(Bitmap::ones(desc.vnode_count()))),
        desc,
    )
}

/// Writes the single List state row, inserting it on the first write.
fn write_list_state<S: StateStore>(
    table: &mut StateTable<S>,
    persisted: &mut Option<ScalarImpl>,
    state: &IcebergUpdateListState,
) -> StreamExecutorResult<()> {
    let value = ScalarImpl::Jsonb(encode(state)?);
    if let Some(previous) = persisted.replace(value.clone()) {
        table.update([Some(previous)], [Some(value)]);
    } else {
        table.insert([Some(value)]);
    }
    Ok(())
}

fn assignments_chunk(assignments: &[IcebergUpdateAssignment]) -> StreamExecutorResult<StreamChunk> {
    let rows = assignments
        .iter()
        .map(|assignment| {
            Ok((
                Op::Insert,
                OwnedRow::new(vec![
                    Some(ScalarImpl::Utf8(assignment.key()?.into())),
                    Some(ScalarImpl::Jsonb(encode(assignment)?)),
                ]),
            ))
        })
        .collect::<StreamExecutorResult<Vec<_>>>()?;
    Ok(StreamChunk::from_rows(
        &rows,
        &[DataType::Varchar, DataType::Jsonb],
    ))
}

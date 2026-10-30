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

use std::time::Duration;

use anyhow::anyhow;
use either::Either;
use iceberg::scan::FileScanTask;
use risingwave_common::array::Op;
use risingwave_common::catalog::ColumnCatalog;
use risingwave_common::metrics::GLOBAL_ERROR_METRICS;
use risingwave_connector::source::ConnectorProperties;
use risingwave_connector::source::iceberg::{
    IcebergLoadTarget, IcebergScanMetricsLabels, IcebergScanPlanner, IcebergScanProjection,
    PersistedFileScanTask,
};
use risingwave_connector::source::reader::desc::SourceDescBuilder;
use thiserror_ext::AsReport;
use tokio::sync::mpsc::UnboundedReceiver;

use super::{Backoff, LoadRetryPolicy, StartKind};
use crate::executor::LoadAction;
use crate::executor::prelude::*;
use crate::executor::source::{StreamSourceCore, barrier_to_message_stream};
use crate::executor::stream_reader::StreamReaderWithPause;
use crate::task::{LoadProgress, LocalBarrierManager};

enum ListState {
    Idle,
    /// The listing stream may still yield file tasks. `backoff` delays the re-lists of this load.
    Listing {
        backoff: Backoff,
    },
    /// Every file task has gone downstream.
    Exhausted {
        snapshot_id: Option<i64>,
    },
    /// The load gave up after repeated errors.
    Failed,
}

impl ListState {
    fn start_load(&mut self, retry_policy: LoadRetryPolicy, persisted_snapshot: Option<i64>) {
        *self = match persisted_snapshot {
            Some(snapshot_id) => Self::Exhausted {
                snapshot_id: Some(snapshot_id),
            },
            None => Self::Listing {
                backoff: retry_policy.backoff(),
            },
        };
    }

    fn is_listing(&self) -> bool {
        matches!(self, Self::Listing { .. })
    }

    fn is_exhausted(&self) -> bool {
        matches!(self, Self::Exhausted { .. })
    }

    fn is_failed(&self) -> bool {
        matches!(self, Self::Failed)
    }

    /// Returns the delay before re-listing, or `None` once the backoff of this load runs out.
    fn next_retry_delay(&mut self) -> Option<Duration> {
        match self {
            Self::Listing { backoff } => backoff.next(),
            _ => None,
        }
    }

    fn mark_failed(&mut self) {
        *self = Self::Failed;
    }

    fn mark_exhausted(&mut self, snapshot_id: Option<i64>) {
        if self.is_listing() {
            *self = Self::Exhausted { snapshot_id };
        }
    }

    fn mark_idle(&mut self) {
        *self = Self::Idle;
    }
}

/// Output of the listing stream.
enum ListingEvent {
    File(Box<FileScanTask>),
    Finished(Option<i64>),
}

/// The single row of the list state table, recording that listing `snapshot_id` has finished.
fn marker_row(snapshot_id: i64) -> OwnedRow {
    OwnedRow::new(vec![Some(ScalarImpl::Int64(snapshot_id))])
}

pub struct BatchIcebergListExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,
    /// Streaming source for external
    stream_source_core: StreamSourceCore<S>,
    /// Columns of fetch executor, used to plan files.
    /// For backward compatibility, this can be None, meaning all columns are needed.
    downstream_columns: Option<Vec<ColumnCatalog>>,
    /// Receiver of barrier channel.
    barrier_receiver: Option<UnboundedReceiver<Barrier>>,
    /// Local barrier manager for reporting list finished
    barrier_manager: LocalBarrierManager,
    load_target: IcebergLoadTarget,
    retry_policy: LoadRetryPolicy,
    start_kind: StartKind,
    /// Metrics for monitor.
    _metrics: Arc<StreamingMetrics>,
}

impl<S: StateStore> BatchIcebergListExecutor<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        actor_ctx: ActorContextRef,
        stream_source_core: StreamSourceCore<S>,
        downstream_columns: Option<Vec<ColumnCatalog>>,
        metrics: Arc<StreamingMetrics>,
        barrier_receiver: UnboundedReceiver<Barrier>,
        barrier_manager: LocalBarrierManager,
        load_target: IcebergLoadTarget,
        retry_policy: LoadRetryPolicy,
        start_kind: StartKind,
    ) -> Self {
        Self {
            actor_ctx,
            stream_source_core,
            downstream_columns,
            barrier_receiver: Some(barrier_receiver),
            barrier_manager,
            load_target,
            retry_policy,
            start_kind,
            _metrics: metrics,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let mut barrier_receiver = self.barrier_receiver.take().unwrap();
        let first_barrier = barrier_receiver
            .recv()
            .instrument_await("source_recv_first_barrier")
            .await
            .ok_or_else(|| {
                anyhow!(
                    "failed to receive the first barrier, actor_id: {:?}, source_id: {:?}",
                    self.actor_ctx.id,
                    self.stream_source_core.source_id
                )
            })?;

        // Build source description from the builder.
        let source_desc_builder: SourceDescBuilder =
            self.stream_source_core.source_desc_builder.take().unwrap();

        let properties = source_desc_builder.with_properties();
        let config = ConnectorProperties::extract(properties, false)?;
        let ConnectorProperties::Iceberg(iceberg_properties) = config else {
            unreachable!()
        };

        let iceberg_table_name = iceberg_properties.table.table_name().to_owned();
        let source_id_str = self.stream_source_core.source_id.to_string();
        let source_name_str = self.stream_source_core.source_name.clone();
        let scan_metrics =
            IcebergScanMetricsLabels::new(source_id_str, source_name_str, iceberg_table_name);
        let scan_planner = IcebergScanPlanner::new(
            (*iceberg_properties).clone(),
            IcebergScanProjection::from_downstream_columns(self.downstream_columns.as_deref()),
            Some(scan_metrics.clone()),
        );

        let pause_on_startup = first_barrier.is_pause_on_startup();
        let first_epoch = first_barrier.epoch;

        yield Message::Barrier(first_barrier);

        let state_table = self.stream_source_core.split_state_store.state_table_mut();
        state_table.init_epoch(first_epoch).await?;
        let mut persisted_snapshot = state_table
            .get_from_one_value_table()
            .await?
            .map(|value| *value.as_int64());
        let barrier_stream = barrier_to_message_stream(barrier_receiver).boxed();

        let mut stream = StreamReaderWithPause::<true, _>::new(
            barrier_stream,
            futures::stream::pending().boxed(),
        );
        let mut state = ListState::Idle;

        if pause_on_startup {
            stream.pause_stream();
        }

        while let Some(msg) = stream.next().await {
            match msg {
                Err(e) => {
                    tracing::error!(error = %e.as_report(), "encountered an error in batch iceberg list");

                    GLOBAL_ERROR_METRICS.user_source_error.report([
                        e.variant_name().to_owned(),
                        self.stream_source_core.source_id.to_string(),
                        self.stream_source_core.source_name.clone(),
                        self.actor_ctx.fragment_id.to_string(),
                    ]);

                    scan_metrics.record_scan_error("list_error");

                    if !state.is_listing() {
                        return Err(e);
                    }

                    let Some(delay) = state.next_retry_delay() else {
                        tracing::error!(
                            source_id = %self.stream_source_core.source_id,
                            fragment_id = %self.actor_ctx.fragment_id,
                            error = %e.as_report(),
                            "load failed after repeated list errors"
                        );
                        state.mark_failed();
                        stream.replace_data_stream(futures::stream::pending().boxed());
                        continue;
                    };

                    tracing::info!(
                        source_id = %self.stream_source_core.source_id,
                        fragment_id = %self.actor_ctx.fragment_id,
                        ?delay,
                        "re-listing iceberg scan tasks due to error"
                    );

                    stream.replace_data_stream(
                        Self::list_iceberg_scan_task(
                            scan_planner.clone(),
                            self.load_target,
                            Some(delay),
                        )
                        .boxed(),
                    );
                }
                Ok(msg) => match msg {
                    Either::Left(msg) => match msg {
                        Message::Barrier(barrier) => {
                            if let Some(mutation) = barrier.mutation.as_deref() {
                                match mutation {
                                    Mutation::Pause => stream.pause_stream(),
                                    Mutation::Resume => stream.resume_stream(),
                                    Mutation::Load {
                                        target,
                                        action: LoadAction::Start,
                                    } if target.addresses(
                                        self.stream_source_core.source_id,
                                        self.actor_ctx.fragment_id,
                                    ) =>
                                    {
                                        tracing::info!(
                                            ?barrier.epoch,
                                            actor_id = %self.actor_ctx.id,
                                            source_id = %self.stream_source_core.source_id,
                                            fragment_id = %self.actor_ctx.fragment_id,
                                            "load started, listing iceberg scan tasks"
                                        );
                                        if self.start_kind == StartKind::Reset
                                            && let Some(snapshot_id) = persisted_snapshot.take()
                                        {
                                            state_table.delete(marker_row(snapshot_id));
                                        }

                                        state.start_load(self.retry_policy, persisted_snapshot);

                                        if state.is_exhausted() {
                                            stream.replace_data_stream(
                                                futures::stream::pending().boxed(),
                                            );
                                        } else {
                                            stream.replace_data_stream(
                                                Self::list_iceberg_scan_task(
                                                    scan_planner.clone(),
                                                    self.load_target,
                                                    None,
                                                )
                                                .boxed(),
                                            );
                                        }
                                    }
                                    _ => (),
                                }
                            }

                            if barrier.is_checkpoint() && state.is_failed() {
                                self.barrier_manager.report_load_progress(
                                    barrier.epoch,
                                    self.actor_ctx.id,
                                    self.stream_source_core.source_id,
                                    LoadProgress::Failed,
                                );
                                state.mark_idle();
                            }

                            if barrier.is_checkpoint()
                                && persisted_snapshot.is_none()
                                && let ListState::Exhausted {
                                    snapshot_id: Some(snapshot_id),
                                } = &state
                            {
                                state_table.insert(marker_row(*snapshot_id));
                                persisted_snapshot = Some(*snapshot_id);
                            }
                            state_table
                                .commit_assert_no_update_vnode_bitmap(barrier.epoch)
                                .await?;

                            if barrier.is_checkpoint() && state.is_exhausted() {
                                tracing::info!(
                                    ?barrier.epoch,
                                    source_id = %self.stream_source_core.source_id,
                                    fragment_id = %self.actor_ctx.fragment_id,
                                    "reporting batch iceberg list finished"
                                );
                                self.barrier_manager.report_load_progress(
                                    barrier.epoch,
                                    self.actor_ctx.id,
                                    self.stream_source_core.source_id,
                                    LoadProgress::ListFinished,
                                );
                                state.mark_idle();
                            }

                            yield Message::Barrier(barrier);
                        }
                        _ => unreachable!(),
                    },
                    Either::Right(ListingEvent::Finished(snapshot_id)) => {
                        state.mark_exhausted(snapshot_id)
                    }
                    Either::Right(ListingEvent::File(file_scan_task)) => {
                        let data_file_path = file_scan_task.data_file_path.clone();
                        let persisted_task = PersistedFileScanTask::encode(*file_scan_task)?;
                        let chunk = StreamChunk::from_rows(
                            &[(
                                Op::Insert,
                                OwnedRow::new(vec![
                                    Some(ScalarImpl::Utf8(data_file_path.into())),
                                    Some(ScalarImpl::Jsonb(persisted_task)),
                                ]),
                            )],
                            &[DataType::Varchar, DataType::Jsonb],
                        );

                        yield Message::Chunk(chunk);
                    }
                },
            }
        }
    }

    /// Waits for `delay` if set, then yields a `File` for each file task, then `Finished` with
    /// the listed snapshot id, or `None` for an empty table.
    #[try_stream(ok = ListingEvent, error = StreamExecutorError)]
    async fn list_iceberg_scan_task(
        scan_planner: IcebergScanPlanner,
        load_target: IcebergLoadTarget,
        delay: Option<Duration>,
    ) {
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }

        let snapshot_plan = scan_planner.plan_load(load_target).await?;
        let snapshot_id = snapshot_plan.as_ref().map(|plan| plan.snapshot_id);

        if let Some(snapshot_plan) = snapshot_plan {
            #[for_await]
            for scan_task in snapshot_plan.tasks {
                let scan_task = scan_task?;
                tracing::debug!(
                    "list_iceberg_scan_task: data_file_path={}, scan_task={:?}",
                    scan_task.data_file_path,
                    scan_task
                );
                yield ListingEvent::File(Box::new(scan_task));
            }
        }

        yield ListingEvent::Finished(snapshot_id);
    }
}

impl<S: StateStore> Execute for BatchIcebergListExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }
}

impl<S: StateStore> Debug for BatchIcebergListExecutor<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchIcebergListExecutor")
            .field("source_id", &self.stream_source_core.source_id)
            .field("column_ids", &self.stream_source_core.column_ids)
            .field("downstream_columns", &self.downstream_columns)
            .finish()
    }
}
